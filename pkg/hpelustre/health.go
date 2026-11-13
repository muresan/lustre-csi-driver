/*
 * Copyright 2026 Hewlett Packard Enterprise Development LP
 * Other additional copyright holders may be indicated within.
 *
 * The entirety of this work is licensed under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hpelustre

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
)

// The LNet health of an MGS, as seen from this node, is whether any of its
// NIDs answers an LNet ping (see lnetping.go). An MGS that doesn't answer means that mounting
// from it would block, typically because its site is down. The converse
// doesn't hold: an MGS that answers doesn't mean that the MDTs and OSTs of
// its filesystem are up.
//
// Each node reports the health of both sides of all failover PVs in a Lease
// of its own, in the driver's namespace, for automation to decide whether to
// fail over:
//
//	apiVersion: coordination.k8s.io/v1
//	kind: Lease
//	metadata:
//	  name: lustre-csi-health-<node>
//	  labels:
//	    app.kubernetes.io/component: lnet-health
//	  annotations:
//	    lustre-csi.hpe.com/lnet-health: '[{"mgs":"10.0.0.1@o2ib:10.0.0.3@o2ib","state":"Unreachable",...}]'
//	spec:
//	  holderIdentity: <node>
//	  renewTime: ...
//	  leaseDurationSeconds: ...
//
// A Lease that wasn't renewed within its duration is stale. Nodes also record
// Events when an MGS becomes unreachable, or reachable again.

type sideState string

const (
	SideReachable   sideState = "Reachable"
	SideUnreachable sideState = "Unreachable"
	// The MGS could not be pinged, e.g. because LNet isn't up on this node.
	SideUnknown sideState = "Unknown"

	HealthLeaseAnnotation   = "lustre-csi.hpe.com/lnet-health"
	HealthLeaseNamePrefix   = "lustre-csi-health-"
	healthLeaseComponent    = "lnet-health"
	healthLeasePartOf       = "lustre-csi-driver"
	healthEventUnreachable  = "LustreMGSUnreachable"
	healthEventReachable    = "LustreMGSReachable"
	lnetModulePath          = "/sys/module/lnet"
	serviceAccountNamespace = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

	// How old a cached health may be when deciding whether to mount from an
	// MGS.
	mountCheckMaxAge = 15 * time.Second
	// How old a cached health may be when deciding how long to wait on an
	// unmount.
	unmountCheckMaxAge = 2 * time.Minute
	// How many PV names an Event lists.
	healthEventMaxPVs = 5
)

// sideHealth is the LNet health of an MGS, as reported in health Leases.
type sideHealth struct {
	// The MGS NID list, as in volume handles.
	MGS   string    `json:"mgs"`
	State sideState `json:"state"`
	// When State last changed.
	Since time.Time `json:"since"`
	// When the MGS was last pinged.
	Checked time.Time `json:"checked"`
	Detail  string    `json:"detail,omitempty"`
}

type pingResult struct {
	state  sideState
	detail string
}

// pingFunc pings a NID, giving up once ctx is done.
type pingFunc func(ctx context.Context, nid string) pingResult

// healthChecker pings MGSs and caches the results.
type healthChecker struct {
	ping    pingFunc
	timeout time.Duration // of each ping

	mu    sync.Mutex
	sides map[string]*sideEntry // keyed by MGS NID list
}

type sideEntry struct {
	health  sideHealth
	probing chan struct{} // closed when the probe in progress completes
}

func newHealthChecker(ping pingFunc, timeout time.Duration) *healthChecker {
	return &healthChecker{ping: ping, timeout: timeout, sides: map[string]*sideEntry{}}
}

// check returns the health of the MGS with the given NID list, pinging it
// unless it was pinged within maxAge. Concurrent checks share a ping.
func (h *healthChecker) check(mgs string, maxAge time.Duration) sideHealth {
	h.mu.Lock()
	e, ok := h.sides[mgs]
	if !ok {
		e = &sideEntry{health: sideHealth{MGS: mgs, State: SideUnknown}}
		h.sides[mgs] = e
	}
	if !e.health.Checked.IsZero() && time.Since(e.health.Checked) <= maxAge {
		defer h.mu.Unlock()
		return e.health
	}
	if e.probing == nil {
		e.probing = make(chan struct{})
		go h.probe(e)
	}
	probing := e.probing
	h.mu.Unlock()

	timer := time.NewTimer(h.timeout + 2*pingGrace)
	defer timer.Stop()
	select {
	case <-probing:
	case <-timer.C:
		// The ping is stuck. Report the last known health.
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return e.health
}

// cached returns the last known health of the MGS that shares a NID with
// mgs, if it was pinged within maxAge, without pinging it.
func (h *healthChecker) cached(mgs string, maxAge time.Duration) sideHealth {
	h.mu.Lock()
	defer h.mu.Unlock()
	if e, ok := h.sides[mgs]; ok && time.Since(e.health.Checked) <= maxAge {
		return e.health
	}
	// Mount sources in /proc/self/mountinfo may be spelled differently from
	// volume handles.
	for key, e := range h.sides {
		if sameMGS(key, mgs) && time.Since(e.health.Checked) <= maxAge {
			return e.health
		}
	}
	return sideHealth{MGS: mgs, State: SideUnknown}
}

func (h *healthChecker) probe(e *sideEntry) {
	h.mu.Lock()
	mgs := e.health.MGS
	h.mu.Unlock()

	result := h.pingMGS(mgs)
	now := time.Now()

	h.mu.Lock()
	defer h.mu.Unlock()
	if result.state != e.health.State || e.health.Since.IsZero() {
		e.health.Since = now
	}
	e.health.State = result.state
	e.health.Detail = result.detail
	e.health.Checked = now
	close(e.probing)
	e.probing = nil
}

// pingMGS pings all the NIDs of an MGS NID list at once. The MGS is
// reachable if any of them answers, and unreachable if none of them does.
func (h *healthChecker) pingMGS(mgs string) pingResult {
	nids := strings.FieldsFunc(mgs, func(r rune) bool { return r == ':' || r == ',' })
	if len(nids) == 0 {
		return pingResult{SideUnknown, fmt.Sprintf("no NIDs in %q", mgs)}
	}

	ctx, cancel := context.WithTimeout(context.Background(), h.timeout+pingGrace)
	defer cancel()
	results := make(chan pingResult, len(nids))
	for _, nid := range nids {
		go func() { results <- h.ping(ctx, nid) }()
	}

	var unreachable, unknown []string
	for range nids {
		r := <-results
		switch r.state {
		case SideReachable:
			return r
		case SideUnreachable:
			unreachable = append(unreachable, r.detail)
		default:
			unknown = append(unknown, r.detail)
		}
	}
	if len(unknown) == 0 {
		return pingResult{SideUnreachable, strings.Join(unreachable, "; ")}
	}
	return pingResult{SideUnknown, strings.Join(append(unknown, unreachable...), "; ")}
}

// healthReporter periodically reports the health of the MGSs of the sides of
// failover PVs.
type healthReporter struct {
	checker  *healthChecker
	interval time.Duration
	// sides returns the MGS NID lists of both sides of all failover PVs,
	// each with the names of the PVs currently active on it.
	sides   func() map[string][]string
	publish func(ctx context.Context, report []sideHealth) error
	event   func(eventType, reason, message string)

	last map[string]sideState
}

func (r *healthReporter) run(stopCh <-chan struct{}) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-stopCh
		cancel()
	}()

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		r.report(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (r *healthReporter) report(ctx context.Context) {
	sides := r.sides()
	mgss := make([]string, 0, len(sides))
	for mgs := range sides {
		mgss = append(mgss, mgs)
	}
	sort.Strings(mgss)

	report := make([]sideHealth, len(mgss))
	var wg sync.WaitGroup
	for i, mgs := range mgss {
		wg.Add(1)
		go func() {
			defer wg.Done()
			report[i] = r.checker.check(mgs, r.interval/2)
		}()
	}
	wg.Wait()

	if r.last == nil {
		r.last = map[string]sideState{}
	}
	seen := map[string]bool{}
	for _, h := range report {
		seen[h.MGS] = true
		prev := r.last[h.MGS]
		r.last[h.MGS] = h.State
		switch {
		case h.State == SideUnreachable && prev != SideUnreachable:
			msg := fmt.Sprintf("MGS %s does not answer LNet pings from this node%s: %s",
				h.MGS, describeActivePVs(sides[h.MGS]), h.Detail)
			klog.Warning(msg)
			r.event(corev1.EventTypeWarning, healthEventUnreachable, msg)
		case h.State == SideReachable && prev == SideUnreachable:
			msg := fmt.Sprintf("MGS %s answers LNet pings from this node again%s",
				h.MGS, describeActivePVs(sides[h.MGS]))
			klog.Info(msg)
			r.event(corev1.EventTypeNormal, healthEventReachable, msg)
		}
	}
	for mgs := range r.last {
		if !seen[mgs] {
			delete(r.last, mgs)
		}
	}

	publishCtx, cancel := context.WithTimeout(ctx, max(r.interval, 10*time.Second))
	defer cancel()
	if err := r.publish(publishCtx, report); err != nil {
		klog.Warningf("Could not report LNet health: %v", err)
	}
}

func describeActivePVs(pvs []string) string {
	switch {
	case len(pvs) == 0:
		return ", which no PV is active on"
	case len(pvs) <= healthEventMaxPVs:
		return fmt.Sprintf(", which PV(s) %s are active on", strings.Join(pvs, ", "))
	default:
		return fmt.Sprintf(", which PVs %s and %d more are active on",
			strings.Join(pvs[:healthEventMaxPVs], ", "), len(pvs)-healthEventMaxPVs)
	}
}

func healthLeaseName(nodeName string) string {
	name := HealthLeaseNamePrefix + nodeName
	if len(name) > 253 {
		sum := sha256.Sum256([]byte(nodeName))
		name = HealthLeaseNamePrefix + hex.EncodeToString(sum[:16])
	}
	return name
}

func healthLeaseLabels() map[string]string {
	return map[string]string{
		"app.kubernetes.io/component": healthLeaseComponent,
		"app.kubernetes.io/part-of":   healthLeasePartOf,
	}
}

// leasePublisher returns a function that writes a health report to the
// node's health Lease, creating it if need be. The Lease is owned by the
// node, so that it is deleted along with it.
func leasePublisher(client kubernetes.Interface, namespace, nodeName string, duration time.Duration) func(context.Context, []sideHealth) error {
	name := healthLeaseName(nodeName)
	seconds := int32(duration.Seconds())
	return func(ctx context.Context, report []sideHealth) error {
		data, err := json.Marshal(report)
		if err != nil {
			return err
		}
		now := metav1.NewMicroTime(time.Now())
		leases := client.CoordinationV1().Leases(namespace)

		lease, err := leases.Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			lease = &coordinationv1.Lease{
				ObjectMeta: metav1.ObjectMeta{
					Name:        name,
					Namespace:   namespace,
					Labels:      healthLeaseLabels(),
					Annotations: map[string]string{HealthLeaseAnnotation: string(data)},
				},
				Spec: coordinationv1.LeaseSpec{
					HolderIdentity:       &nodeName,
					LeaseDurationSeconds: &seconds,
					AcquireTime:          &now,
					RenewTime:            &now,
				},
			}
			node, err := client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
			if err == nil {
				lease.OwnerReferences = []metav1.OwnerReference{{
					APIVersion: "v1",
					Kind:       "Node",
					Name:       node.Name,
					UID:        node.UID,
				}}
			} else {
				klog.Warningf("Could not get node %s to own its health Lease: %v", nodeName, err)
			}
			_, err = leases.Create(ctx, lease, metav1.CreateOptions{})
			return err
		}
		if err != nil {
			return err
		}

		if lease.Annotations == nil {
			lease.Annotations = map[string]string{}
		}
		lease.Annotations[HealthLeaseAnnotation] = string(data)
		lease.Spec.HolderIdentity = &nodeName
		lease.Spec.LeaseDurationSeconds = &seconds
		lease.Spec.RenewTime = &now
		_, err = leases.Update(ctx, lease, metav1.UpdateOptions{})
		return err
	}
}

// nodeEventRecorder returns a function that records Events about the node,
// as kubelet does.
func nodeEventRecorder(client kubernetes.Interface, driverName, nodeName string) func(eventType, reason, message string) {
	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: client.CoreV1().Events("")})
	recorder := broadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: driverName, Host: nodeName})
	ref := &corev1.ObjectReference{Kind: "Node", Name: nodeName, UID: types.UID(nodeName)}
	return func(eventType, reason, message string) {
		recorder.Event(ref, eventType, reason, message)
	}
}

// healthNamespace returns the namespace to keep health Leases in: the given
// one, or else the driver's own.
func healthNamespace(namespace string) (string, error) {
	if namespace != "" {
		return namespace, nil
	}
	data, err := os.ReadFile(serviceAccountNamespace)
	if err != nil {
		return "", fmt.Errorf("could not determine the driver's namespace: %w", err)
	}
	return strings.TrimSpace(string(data)), nil
}
