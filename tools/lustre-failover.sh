#!/bin/bash

# Copyright 2026 Hewlett Packard Enterprise Development LP
# Other additional copyright holders may be indicated within.
#
# The entirety of this work is licensed under the Apache License,
# Version 2.0 (the "License"); you may not use this file except
# in compliance with the License.
#
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e
set -u
set -o pipefail
shopt -s inherit_errexit

# Switch Lustre CSI PVs between their primary and failover volume handles, and
# restart the pods that use them. See "Failover" in the README. Requires
# kubectl and jq.

DRIVER=lustre-csi.hpe.com
ACTIVE_ANNOTATION=lustre-csi.hpe.com/failover-active
EPOCH_ANNOTATION=lustre-csi.hpe.com/failover-epoch
HEALTH_ANNOTATION=lustre-csi.hpe.com/lnet-health
HEALTH_SELECTOR=app.kubernetes.io/component=lnet-health,app.kubernetes.io/part-of=lustre-csi-driver
NAMESPACE=lustre-csi-system
TIMEOUT=600
HOOK=${LUSTRE_FAILOVER_HOOK:-}
ASSUME_YES=false
SELECT_ALL=false
SELECTOR=
PVS=()
SELECTION=()
RESTART_TIMED_OUT=false
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

usage() {
    cat <<EOF
Usage: $0 <selection> [options] <command> [primary|failover]

Selection, of PVs with a failoverVolumeHandle:
  -A              All of them.
  -l <selector>   Those matching a label selector.
  -p <pv>         A PV. May be repeated.

Commands:
  status                   Show the selected PVs.
  health                   Show how many nodes reach the MGS of each side over
                           LNet, from the health Leases of the node plugins.
  pods                     List the pods using them, as "<namespace> <pod>".
  drain                    Refuse new mounts. Existing mounts are kept.
  undrain <side>           Make <side> of draining PVs active again, without
                           bumping the epoch, e.g. to roll back a switchover.
  activate <side>          Make <side> active and bump the epoch. Nodes
                           force-unmount mounts of the other side or an older epoch.
  restart-pods             Delete the pods using them and wait until they are gone.
  switchover <side>        Planned switchover, with both filesystems up:
                             drain, restart-pods, hook fence-old, hook wait-sync,
                             hook promote-new, activate.
  failover <side>          Unplanned failover, with the active filesystem down:
                             hook promote-new, activate, restart-pods,
                             hook fence-old (allowed to fail).

Options:
  -n <namespace>  Namespace of the driver's health Leases (default: $NAMESPACE).
  -t <seconds>    How long to wait for pods to be deleted (default: $TIMEOUT).
  -k <hook>       Executable run for each storage-side step, once per pair of
                  old and new MGS NID lists among the selected PVs, as
                    <hook> <step> <old-mgs-nids> <new-mgs-nids>
                  where <step> is one of:
                    fence-old    make the old filesystem read-only to these clients (nodemap)
                    wait-sync    wait for replication from old to new to catch up
                    promote-new  make the new filesystem writable by these clients (nodemap)
                  Default: \$LUSTRE_FAILOVER_HOOK. Without a hook, each step is
                  printed and must be confirmed once done by hand.
  -y              Don't ask for confirmation before a switchover or failover.
EOF
}

die() {
    echo "error: $*" >&2
    exit 1
}

log() {
    echo "==> $*" >&2
}

confirm() {
    local reply
    read -r -p "$1 [y/N] " reply </dev/tty || die "no terminal to confirm on"
    [[ $reply == [yY]* ]] || die "aborted"
}

# jq definitions shared by the queries below. The $ are jq's.
# shellcheck disable=SC2016
JQ_DEFS='
def active: .metadata.annotations["'$ACTIVE_ANNOTATION'"] // "primary";
def epoch: .metadata.annotations["'$EPOCH_ANNOTATION'"] // "0";
def handle($side): if $side == "failover" then .spec.csi.volumeAttributes.failoverVolumeHandle else .spec.csi.volumeHandle end;
def mgs: split(":/")[0];
def fs: split(":/") | "\(.[0]):/\(.[1] | split("/")[0])";
def failover_pv: .spec.csi.driver == "'$DRIVER'" and (.spec.csi.volumeAttributes.failoverVolumeHandle // "") != "";
'

# Write all PVs of the driver with a failoverVolumeHandle to $TMP/all.json,
# as a JSON array.
all_pvs() {
    kubectl get pv -o json | jq "$JQ_DEFS"' [.items[] | select(failover_pv)]' >"$TMP/all.json"
}

# Write the selected PVs to $TMP/selected.json, as a JSON array.
selected_pvs() {
    local missing
    if $SELECT_ALL; then
        all_pvs
        cp "$TMP/all.json" "$TMP/selected.json"
    elif [[ -n $SELECTOR ]]; then
        kubectl get pv -l "$SELECTOR" -o json | jq '.items' >"$TMP/selected.json"
    else
        kubectl get pv "${PVS[@]}" -o json | jq 'if has("items") then .items else [.] end' >"$TMP/selected.json"
    fi
    jq -e 'length > 0' "$TMP/selected.json" >/dev/null || die "no PVs selected"
    missing=$(jq -r "$JQ_DEFS"' .[] | select(failover_pv | not) | .metadata.name' "$TMP/selected.json")
    [[ -z $missing ]] || die "PVs without a failoverVolumeHandle: $missing"
}

other_side() {
    case $1 in
    primary) echo failover ;;
    failover) echo primary ;;
    *) die "side must be primary or failover, not '$1'" ;;
    esac
}

set_active() {
    local active=$1 bump=$2 patches name patch
    selected_pvs
    patches=$(jq -c "$JQ_DEFS"' .[]
        | (epoch | if test("^[0-9]+$") then tonumber else error("PV \(.metadata.name) has a bad epoch") end) as $epoch
        | {metadata: {name: .metadata.name, resourceVersion: .metadata.resourceVersion,
            annotations: {
                "'$ACTIVE_ANNOTATION'": $active,
                "'$EPOCH_ANNOTATION'": (if $bump then $epoch + 1 else $epoch end | tostring)}}}' \
        --arg active "$active" --argjson bump "$bump" "$TMP/selected.json")
    while read -r patch; do
        name=$(jq -r .metadata.name <<<"$patch")
        # The resourceVersion makes the patch fail rather than overwrite a
        # concurrent change to the PV.
        kubectl patch pv "$name" --type=merge -p "$patch" >/dev/null
        log "PV $name: $(jq -r --arg a "$ACTIVE_ANNOTATION" --arg e "$EPOCH_ANNOTATION" \
            '"active=\(.metadata.annotations[$a]) epoch=\(.metadata.annotations[$e])"' <<<"$patch")"
    done <<<"$patches"
}

# Check that every selected PV is active on the other side of $1, or draining.
check_active() {
    local new=$1 old bad
    old=$(other_side "$new")
    selected_pvs
    bad=$(jq -r "$JQ_DEFS"' .[] | select(active != $old and active != "draining")
        | "    \(.metadata.name) is active=\(active)"' --arg old "$old" "$TMP/selected.json")
    [[ -z $bad ]] || {
        echo "$bad" >&2
        die "the PVs above are not active=$old or draining"
    }
}

# Warn about PVs that weren't selected but share a filesystem with selected
# ones. They stay on the old side, and if both sides share an fsname, a node
# won't mount them from one side while it has the selected PVs mounted from the
# other side.
warn_unselected() {
    local others
    selected_pvs
    all_pvs
    others=$(jq -r "$JQ_DEFS"'
        ([$selected[0][] | handle("primary"), handle("failover") | fs]) as $fs
        | ([$selected[0][] | .metadata.name]) as $names
        | .[] | select((.metadata.name | IN($names[])) | not)
        | select(any(handle("primary"), handle("failover") | fs; IN($fs[])))
        | "    \(.metadata.name)"' --slurpfile selected "$TMP/selected.json" "$TMP/all.json")
    if [[ -n $others ]]; then
        echo "$others" >&2
        log "WARNING: the PVs above share a filesystem with the selected PVs but are not selected"
    fi
}

# Pods with a PVC bound to a selected PV, as "<namespace> <pod> <uid>".
pod_list() {
    selected_pvs
    kubectl get pods -A -o json >"$TMP/pods.json"
    jq -r --slurpfile pvs "$TMP/selected.json" '
        [$pvs[0][] | select(.spec.claimRef != null)
            | "\(.spec.claimRef.namespace)/\(.spec.claimRef.name)"] as $claims
        | .items[]
        | .metadata.namespace as $ns
        | select(any(.spec.volumes[]?;
            .persistentVolumeClaim != null
            and ("\($ns)/\(.persistentVolumeClaim.claimName)" | IN($claims[]))))
        | "\($ns) \(.metadata.name) \(.metadata.uid)"' "$TMP/pods.json"
}

# Delete the pods using the selected PVs, and wait until they are gone. Set
# RESTART_TIMED_OUT if some are still there after $TIMEOUT.
restart_pods() {
    local pods ns name uids remaining deadline
    RESTART_TIMED_OUT=false
    pods=$(pod_list)
    if [[ -z $pods ]]; then
        log "No pods use the selected PVs"
        return
    fi

    log "Deleting the pods using the selected PVs:"
    awk '{print "    " $1 "/" $2}' <<<"$pods" >&2
    while read -r ns name _; do
        kubectl delete pod -n "$ns" "$name" --wait=false >/dev/null
    done <<<"$pods"

    # A pod is only removed once kubelet has unpublished its volumes. Wait by
    # UID, since controllers may recreate pods with the same name.
    uids=$(awk '{print $3}' <<<"$pods")
    deadline=$((SECONDS + TIMEOUT))
    while true; do
        remaining=$(kubectl get pods -A -o json | jq -r --arg uids "$uids" \
            '($uids | split("\n")) as $u | .items[]
            | select(.metadata.uid | IN($u[])) | "    \(.metadata.namespace)/\(.metadata.name) on \(.spec.nodeName // "-")"')
        [[ -n $remaining ]] || break
        if ((SECONDS >= deadline)); then
            echo "$remaining" >&2
            log "WARNING: the pods above are still terminating after ${TIMEOUT}s. Check their nodes:" \
                "kubelet lets go of a volume only once the pod's containers are gone, and force-deleting" \
                "the pod doesn't release its mounts. For a node that is down, see the" \
                "node.kubernetes.io/out-of-service taint."
            RESTART_TIMED_OUT=true
            return
        fi
        sleep 5
    done
    log "All pods that used the selected PVs are gone"
}

# Run a storage-side step for each pair of old and new MGS NID lists.
storage_step() {
    local step=$1 old=$2 new=$3 description=$4 pairs old_mgs new_mgs
    selected_pvs
    pairs=$(jq -r "$JQ_DEFS"' [.[] | "\(handle($old) | mgs) \(handle($new) | mgs)"] | unique[]' \
        --arg old "$old" --arg new "$new" "$TMP/selected.json")
    while read -r old_mgs new_mgs; do
        if [[ -n $HOOK ]]; then
            log "Running $HOOK $step $old_mgs $new_mgs"
            "$HOOK" "$step" "$old_mgs" "$new_mgs" </dev/null
        else
            log "Manual step $step: $description (old MGS $old_mgs, new MGS $new_mgs)"
            confirm "Done?"
        fi
    done <<<"$pairs"
}

# jq definitions to read health Leases, given $now. The $ are jq's.
# shellcheck disable=SC2016
JQ_HEALTH_DEFS='
def seconds: sub("\\.[0-9]+"; "") | fromdateiso8601;
def fresh: ((.spec.renewTime // "1970-01-01T00:00:00Z") | seconds) + (.spec.leaseDurationSeconds // 0) >= $now;
def sides: (.metadata.annotations["'$HEALTH_ANNOTATION'"] // "[]") | fromjson;
'

# Write the health Leases of the node plugins to $TMP/leases.json.
health_leases() {
    kubectl get leases -n "$NAMESPACE" -l "$HEALTH_SELECTOR" -o json >"$TMP/leases.json"
}

# Show, for the MGS of each side of the selected PVs, how many nodes reach it.
show_health() {
    selected_pvs
    health_leases
    jq -r "$JQ_DEFS$JQ_HEALTH_DEFS"'
        ($leases[0].items | map(select(fresh | not)) | length) as $stale
        | ($leases[0].items | map(select(fresh) | {node: .spec.holderIdentity, sides: sides})) as $reports
        | [.[] | {mgs: (handle("primary") | mgs), side: "primary", pv: .metadata.name, active: (active == "primary")},
                 {mgs: (handle("failover") | mgs), side: "failover", pv: .metadata.name, active: (active == "failover")}]
        | group_by(.mgs)[]
        | .[0].mgs as $mgs
        | [$reports[] | {node, state: (first(.sides[] | select(.mgs == $mgs) | .state) // "Unknown")}] as $states
        | [$states[] | select(.state == "Unreachable") | .node] as $unreachable
        | "MGS \($mgs)",
          "    \([.[] | .side] | unique | join(" and ")) side, active for \([.[] | select(.active) | .pv] | unique | length) PV(s)",
          "    nodes: \([$states[] | select(.state == "Reachable")] | length) reachable, \($unreachable | length) unreachable, \([$states[] | select(.state != "Reachable" and .state != "Unreachable")] | length) unknown, \($stale) stale",
          (if ($unreachable | length) > 0 then "    unreachable from: \($unreachable | join(" "))" else empty end)
    ' --argjson now "$(date +%s)" --slurpfile leases "$TMP/leases.json" "$TMP/selected.json"
}

# Print how many nodes recently reached the MGS of the given side of any
# selected PV, from their health Leases.
nodes_reaching() {
    local side=$1
    selected_pvs
    health_leases
    jq -r "$JQ_DEFS$JQ_HEALTH_DEFS"'
        ([.[] | handle($side) | mgs] | unique) as $mgss
        | [$leases[0].items[] | select(fresh)
           | select(any(sides[]; .state == "Reachable" and (.mgs | IN($mgss[]))))]
        | length
    ' --arg side "$side" --argjson now "$(date +%s)" --slurpfile leases "$TMP/leases.json" "$TMP/selected.json"
}

show_status() {
    selected_pvs
    jq -r "$JQ_DEFS"' .[]
        | "\(.metadata.name)\tactive=\(active)\tepoch=\(epoch)\tclaim=\(.spec.claimRef.namespace // "-")/\(.spec.claimRef.name // "-")
    primary:  \(handle("primary"))
    failover: \(handle("failover"))"' "$TMP/selected.json"
    echo "pods:"
    pod_list | awk '{print "    " $1 "/" $2}'
}

undrain() {
    local side=$1 bad
    other_side "$side" >/dev/null
    selected_pvs
    bad=$(jq -r "$JQ_DEFS"' .[] | select(active != "draining")
        | "    \(.metadata.name) is active=\(active)"' "$TMP/selected.json")
    [[ -z $bad ]] || {
        echo "$bad" >&2
        die "the PVs above are not draining"
    }
    set_active "$side" false
}

switchover() {
    local new=$1 old
    old=$(other_side "$new")
    check_active "$new"
    warn_unselected

    log "Planned switchover of the selected PVs from $old to $new"
    $ASSUME_YES || confirm "Restart all pods using the selected PVs?"

    set_active draining false
    restart_pods
    if $RESTART_TIMED_OUT; then
        die "the selected PVs are left draining. Run the switchover again once the pods above are gone," \
            "or roll back with: $0 ${SELECTION[*]} undrain $old"
    fi
    storage_step fence-old "$old" "$new" "make the $old filesystem read-only to the Kubernetes nodes"
    storage_step wait-sync "$old" "$new" "wait for replication from $old to $new to catch up"
    storage_step promote-new "$old" "$new" "make the $new filesystem writable by the Kubernetes nodes"
    set_active "$new" true
    log "Switchover done. The restarted pods now mount the $new handles. Reverse replication."
}

failover() {
    local new=$1 old reaching
    old=$(other_side "$new")
    check_active "$new"
    warn_unselected

    log "Unplanned failover of the selected PVs from $old to $new"
    reaching=$(nodes_reaching "$old")
    if ((reaching > 0)); then
        log "WARNING: $reaching node(s) still reach the MGS of the $old side over LNet." \
            "If the $old filesystem is up, a planned switchover loses no writes."
    fi
    $ASSUME_YES || confirm "Fail over and restart all pods using the selected PVs?"

    storage_step promote-new "$old" "$new" "make the $new filesystem writable by the Kubernetes nodes"
    set_active "$new" true
    restart_pods
    if [[ -n $HOOK ]]; then
        if ! storage_step fence-old "$old" "$new" ""; then
            log "WARNING: could not fence the $old filesystem"
        fi
    fi
    log "Failover done. The restarted pods now mount the $new handles."
    if $RESTART_TIMED_OUT; then
        log "WARNING: some pods are still terminating, see above. If both sides share an fsname, their" \
            "nodes don't mount the $new side of it until those pods are gone."
    fi
    log "Before the $old filesystem serves clients again, make it read-only to the Kubernetes nodes and reverse replication."
}

while getopts "Al:p:n:t:k:yh" opt; do
    case $opt in
    A)
        SELECT_ALL=true
        SELECTION+=(-A)
        ;;
    l)
        SELECTOR=$OPTARG
        SELECTION+=(-l "$OPTARG")
        ;;
    p)
        PVS+=("$OPTARG")
        SELECTION+=(-p "$OPTARG")
        ;;
    n) NAMESPACE=$OPTARG ;;
    t) TIMEOUT=$OPTARG ;;
    k) HOOK=$OPTARG ;;
    y) ASSUME_YES=true ;;
    h)
        usage
        exit 0
        ;;
    *)
        usage >&2
        exit 2
        ;;
    esac
done
shift $((OPTIND - 1))

selections=0
$SELECT_ALL && selections=$((selections + 1))
[[ -n $SELECTOR ]] && selections=$((selections + 1))
[[ ${#PVS[@]} -gt 0 ]] && selections=$((selections + 1))
[[ $selections -eq 1 ]] || die "select PVs with exactly one of -A, -l or -p"
[[ $# -ge 1 ]] || {
    usage >&2
    exit 2
}
[[ $TIMEOUT =~ ^[0-9]+$ ]] || die "timeout must be a number of seconds"
CMD=$1
SIDE=${2:-}

case $CMD in
status) show_status ;;
health) show_health ;;
pods) pod_list | awk '{print $1, $2}' ;;
drain) set_active draining false ;;
undrain) undrain "$SIDE" ;;
activate)
    other_side "$SIDE" >/dev/null
    set_active "$SIDE" true
    ;;
restart-pods)
    restart_pods
    if $RESTART_TIMED_OUT; then
        exit 1
    fi
    ;;
switchover) switchover "$SIDE" ;;
failover) failover "$SIDE" ;;
*)
    usage >&2
    exit 2
    ;;
esac
