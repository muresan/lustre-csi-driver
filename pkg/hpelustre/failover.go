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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

// A PV opts in to failover with a second Lustre mount source in its volume
// attributes, next to the primary one in its volumeHandle:
//
//	metadata:
//	  annotations:
//	    lustre-csi.hpe.com/failover-active: primary
//	    lustre-csi.hpe.com/failover-epoch: "1"
//	spec:
//	  csi:
//	    volumeHandle: "10.0.0.1@o2ib:10.0.0.3@o2ib:/fz"
//	    volumeAttributes:
//	      failoverVolumeHandle: "10.1.0.1@o2ib:10.1.0.3@o2ib:/fz"
//
// The failover-active annotation selects the source to mount: "primary" (the
// default), "failover", or "draining" to refuse all new mounts. The epoch is
// bumped on every failover.
//
// A mount made under an older epoch, or from the side that is no longer
// active, is stale: it is force-unmounted. Draining makes nothing stale, so
// that pods can be stopped gracefully during a planned switchover.
//
// Independently of the PVs, a node never mounts a Lustre fsname from one MGS
// while it still has the same fsname published from another MGS, because the
// primary and failover filesystems may share an fsname.
const (
	VolumeContextFailoverVolumeHandle = "failoverVolumeHandle"

	FailoverActiveAnnotation = "lustre-csi.hpe.com/failover-active"
	FailoverEpochAnnotation  = "lustre-csi.hpe.com/failover-epoch"

	FailoverPrimary  = "primary"
	FailoverFailover = "failover"
	FailoverDraining = "draining"

	volumeHandleIndex    = "volumeHandle"
	failoverResyncPeriod = 10 * time.Minute
	// How soon to retry a failed force-unmount of a stale mount.
	fenceRetryInterval = 30 * time.Second
)

type failoverState struct {
	active string // FailoverPrimary, FailoverFailover or FailoverDraining
	epoch  int64
}

// failoverMount records which side and epoch a published target path was
// mounted under. It is persisted so that it survives driver restarts.
type failoverMount struct {
	TargetPath string `json:"targetPath"`
	VolumeID   string `json:"volumeID"`
	Side       string `json:"side"`
	Epoch      int64  `json:"epoch"`
	Source     string `json:"source"`
}

type failoverManager struct {
	stateDir          string
	forceUnmountStale bool
	pvs               cache.Indexer // this driver's PVs, indexed by volume handle
	synced            cache.InformerSynced
	// forceUnmount is replaced in tests.
	forceUnmount func(targetPath string) error
	fenceRetry   time.Duration

	mu     sync.Mutex
	mounts map[string]*failoverMount // keyed by target path
	fenced map[string]bool           // stale target paths force-unmounted, or being so
}

func parseFailoverState(pv *corev1.PersistentVolume) (failoverState, error) {
	state := failoverState{active: FailoverPrimary}

	if active, ok := pv.Annotations[FailoverActiveAnnotation]; ok {
		state.active = strings.TrimSpace(active)
	}
	switch state.active {
	case FailoverPrimary, FailoverFailover, FailoverDraining:
	default:
		return state, fmt.Errorf("PV %s: annotation %s=%q must be %q, %q or %q", pv.Name,
			FailoverActiveAnnotation, state.active, FailoverPrimary, FailoverFailover, FailoverDraining)
	}

	if e, ok := pv.Annotations[FailoverEpochAnnotation]; ok {
		epoch, err := strconv.ParseInt(strings.TrimSpace(e), 10, 64)
		if err != nil || epoch < 0 {
			return state, fmt.Errorf("PV %s: annotation %s=%q must be a non-negative integer",
				pv.Name, FailoverEpochAnnotation, e)
		}
		state.epoch = epoch
	}

	return state, nil
}

// isStale reports whether a mount made under rec must no longer be used.
// Nothing is stale while draining.
func (s failoverState) isStale(rec *failoverMount) bool {
	return s.active != FailoverDraining && (rec.Side != s.active || rec.Epoch < s.epoch)
}

// splitSource splits a Lustre mount source of the form
// "<mgs nids>:/<fsname>[/subdir]". The NID list may itself contain ':'
// between failover MGS nodes, but never ":/".
func splitSource(source string) (nids, fsname string, err error) {
	i := strings.Index(source, ":/")
	if i > 0 {
		fsname, _, _ = strings.Cut(source[i+2:], "/")
	}
	if i <= 0 || fsname == "" {
		return "", "", fmt.Errorf("%q is not of the form <mgs nids>:/<fsname>", source)
	}
	return source[:i], fsname, nil
}

// sameMGS reports whether two MGS NID lists share a NID, so likely name the
// same MGS even if written differently.
func sameMGS(a, b string) bool {
	split := func(r rune) bool { return r == ':' || r == ',' }
	for _, x := range strings.FieldsFunc(a, split) {
		for _, y := range strings.FieldsFunc(b, split) {
			if x == y {
				return true
			}
		}
	}
	return false
}

func volumeHandleIndexFunc(driverName string) cache.IndexFunc {
	return func(obj interface{}) ([]string, error) {
		pv, ok := obj.(*corev1.PersistentVolume)
		if !ok || pv.Spec.CSI == nil || pv.Spec.CSI.Driver != driverName {
			return nil, nil
		}
		return []string{pv.Spec.CSI.VolumeHandle}, nil
	}
}

func newFailoverManager(
	client kubernetes.Interface,
	driverName, stateDir string,
	forceUnmountStale bool,
	stopCh <-chan struct{},
) (*failoverManager, error) {
	factory := informers.NewSharedInformerFactory(client, failoverResyncPeriod)
	informer := factory.Core().V1().PersistentVolumes().Informer()
	err := informer.AddIndexers(cache.Indexers{volumeHandleIndex: volumeHandleIndexFunc(driverName)})
	if err != nil {
		return nil, err
	}

	m := newFailoverManagerWithIndexer(stateDir, forceUnmountStale, informer.GetIndexer(), informer.HasSynced)
	if err := m.load(); err != nil {
		return nil, err
	}

	// The periodic resync also retries failed force-unmounts.
	onPV := func(obj interface{}) {
		if pv, ok := obj.(*corev1.PersistentVolume); ok && pv.Spec.CSI != nil && pv.Spec.CSI.Driver == driverName {
			m.reconcile(pv.Spec.CSI.VolumeHandle)
		}
	}
	_, err = informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    onPV,
		UpdateFunc: func(_, obj interface{}) { onPV(obj) },
	})
	if err != nil {
		return nil, err
	}

	factory.Start(stopCh)
	return m, nil
}

func newFailoverManagerWithIndexer(
	stateDir string,
	forceUnmountStale bool,
	pvs cache.Indexer,
	synced cache.InformerSynced,
) *failoverManager {
	return &failoverManager{
		stateDir:          stateDir,
		forceUnmountStale: forceUnmountStale,
		pvs:               pvs,
		synced:            synced,
		forceUnmount:      forceUnmountStaleMount,
		fenceRetry:        fenceRetryInterval,
		mounts:            map[string]*failoverMount{},
		fenced:            map[string]bool{},
	}
}

func (m *failoverManager) statePath(targetPath string) string {
	sum := sha256.Sum256([]byte(targetPath))
	return filepath.Join(m.stateDir, hex.EncodeToString(sum[:])+".json")
}

// load reads the records of published target paths left by a previous
// instance of the driver.
func (m *failoverManager) load() error {
	if err := os.MkdirAll(m.stateDir, 0o700); err != nil {
		return fmt.Errorf("could not create failover state dir %q: %w", m.stateDir, err)
	}
	files, err := filepath.Glob(filepath.Join(m.stateDir, "*.json"))
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			return fmt.Errorf("could not read failover state %q: %w", f, err)
		}
		rec := &failoverMount{}
		if err := json.Unmarshal(data, rec); err != nil || rec.TargetPath == "" {
			klog.Warningf("Ignoring corrupt failover state %q: %v", f, err)
			continue
		}
		m.mounts[rec.TargetPath] = rec
		klog.V(2).Infof("Loaded failover state for %s: volume %s side %q epoch %d",
			rec.TargetPath, rec.VolumeID, rec.Side, rec.Epoch)
	}
	return nil
}

// getState returns the failover state of the PVs with the given volume
// handle and a failover volume handle, which must all agree.
func (m *failoverManager) getState(volumeID string) (failoverState, error) {
	if !m.synced() {
		return failoverState{}, status.Error(codes.Unavailable, "PVs are not yet synced from the API server")
	}
	objs, err := m.pvs.ByIndex(volumeHandleIndex, volumeID)
	if err != nil {
		return failoverState{}, status.Errorf(codes.Internal, "Could not look up PVs of volume %s: %v", volumeID, err)
	}

	var state failoverState
	var names []string
	for _, obj := range objs {
		pv := obj.(*corev1.PersistentVolume)
		if pv.Spec.CSI.VolumeAttributes[VolumeContextFailoverVolumeHandle] == "" {
			continue
		}
		s, err := parseFailoverState(pv)
		if err != nil {
			return failoverState{}, status.Error(codes.FailedPrecondition, err.Error())
		}
		if len(names) > 0 && s != state {
			return failoverState{}, status.Errorf(codes.FailedPrecondition,
				"PVs %s and %s share volume handle %s but disagree on their failover annotations",
				strings.Join(names, ", "), pv.Name, volumeID)
		}
		state = s
		names = append(names, pv.Name)
	}
	if len(names) == 0 {
		return failoverState{}, status.Errorf(codes.FailedPrecondition,
			"No PV with volume handle %s and a %s attribute", volumeID, VolumeContextFailoverVolumeHandle)
	}
	return state, nil
}

// resolve returns the mount source for a volume with a failover volume
// handle, and the record to pass to record() once it is mounted at
// targetPath.
func (m *failoverManager) resolve(volumeID, failoverHandle, targetPath string) (string, *failoverMount, error) {
	if _, _, err := splitSource(failoverHandle); err != nil {
		return "", nil, status.Errorf(codes.InvalidArgument, "%s: %v", VolumeContextFailoverVolumeHandle, err)
	}
	state, err := m.getState(volumeID)
	if err != nil {
		return "", nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	side, epoch := state.active, state.epoch
	rec, published := m.mounts[targetPath]
	if published {
		// Already published, e.g. kubelet is retrying or the node rebooted.
		if state.isStale(rec) {
			return "", nil, status.Errorf(codes.Unavailable,
				"%s was published from the %s of volume %s at epoch %d, which has since failed over; the pod must be restarted",
				targetPath, rec.Side, volumeID, rec.Epoch)
		}
		side, epoch = rec.Side, rec.Epoch
	} else if state.active == FailoverDraining {
		return "", nil, status.Errorf(codes.Unavailable, "Volume %s is draining", volumeID)
	}

	source := volumeID
	if side == FailoverFailover {
		source = failoverHandle
	}
	nids, fsname, err := splitSource(source)
	if err != nil {
		return "", nil, status.Errorf(codes.InvalidArgument, "Volume ID: %v", err)
	}

	if !published {
		m.pruneOrphansLocked()
		if n := m.countConflictsLocked(nids, fsname); n > 0 {
			return "", nil, status.Errorf(codes.Unavailable,
				"%d mount(s) of Lustre fsname %q from a different MGS than %s are still published on this node",
				n, fsname, nids)
		}
	}

	return source, &failoverMount{VolumeID: volumeID, Side: side, Epoch: epoch, Source: source}, nil
}

// record persists rec as the state of the volume published at targetPath.
func (m *failoverManager) record(targetPath string, rec *failoverMount) error {
	rec.TargetPath = targetPath
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}

	m.mu.Lock()
	path := m.statePath(targetPath)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		m.mu.Unlock()
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		m.mu.Unlock()
		return err
	}
	m.mounts[targetPath] = rec
	m.mu.Unlock()

	// The volume may have failed over while it was being mounted.
	m.reconcile(rec.VolumeID)
	return nil
}

// forget drops the state of the volume published at targetPath, once it has
// been unpublished.
func (m *failoverManager) forget(targetPath string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.forgetLocked(targetPath)
}

func (m *failoverManager) forgetLocked(targetPath string) error {
	if _, ok := m.mounts[targetPath]; !ok {
		return nil
	}
	if err := os.Remove(m.statePath(targetPath)); err != nil && !os.IsNotExist(err) {
		return err
	}
	delete(m.mounts, targetPath)
	delete(m.fenced, targetPath)
	return nil
}

// pruneOrphansLocked forgets volumes whose kubelet volume directory is gone,
// meaning they were cleaned up without NodeUnpublishVolume being called.
func (m *failoverManager) pruneOrphansLocked() {
	for path := range m.mounts {
		if _, err := os.Lstat(filepath.Dir(path)); os.IsNotExist(err) {
			klog.Warningf("Forgetting failover state of orphaned volume %s", path)
			if err := m.forgetLocked(path); err != nil {
				klog.Warningf("Could not forget failover state of %s: %v", path, err)
			}
		}
	}
}

func (m *failoverManager) countConflictsLocked(nids, fsname string) int {
	n := 0
	for _, rec := range m.mounts {
		recNids, recFsname, err := splitSource(rec.Source)
		if err == nil && recFsname == fsname && !sameMGS(recNids, nids) {
			n++
		}
	}
	return n
}

// reconcile force-unmounts this node's stale mounts of a volume.
func (m *failoverManager) reconcile(volumeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	used := false
	for _, rec := range m.mounts {
		if rec.VolumeID == volumeID {
			used = true
			break
		}
	}
	if !used {
		return
	}

	state, err := m.getState(volumeID)
	if err != nil {
		klog.Errorf("Could not reconcile failover state of volume %s: %v", volumeID, err)
		return
	}

	for path, rec := range m.mounts {
		if rec.VolumeID != volumeID || !state.isStale(rec) || m.fenced[path] {
			continue
		}
		m.fenced[path] = true
		if !m.forceUnmountStale {
			klog.Warningf("%s is a stale mount of volume %s (%s epoch %d, now %s epoch %d); not force-unmounting it",
				path, volumeID, rec.Side, rec.Epoch, state.active, state.epoch)
			continue
		}
		klog.Warningf("Force-unmounting %s, a stale mount of volume %s (%s epoch %d, now %s epoch %d)",
			path, volumeID, rec.Side, rec.Epoch, state.active, state.epoch)
		go m.fence(path, volumeID)
	}
}

func (m *failoverManager) fence(targetPath, volumeID string) {
	err := m.forceUnmount(targetPath)

	m.mu.Lock()
	defer m.mu.Unlock()
	if err != nil {
		klog.Errorf("Could not force-unmount stale mount %s, will retry in %v: %v", targetPath, m.fenceRetry, err)
		delete(m.fenced, targetPath)
		time.AfterFunc(m.fenceRetry, func() { m.reconcile(volumeID) })
		return
	}
	klog.Infof("Force-unmounted stale mount %s", targetPath)
}

// sides returns the MGS NID lists of both sides of all failover PVs, each
// with the sorted names of the PVs currently active on it.
func (m *failoverManager) sides() map[string][]string {
	sides := map[string][]string{}
	for _, handle := range m.pvs.ListIndexFuncValues(volumeHandleIndex) {
		objs, err := m.pvs.ByIndex(volumeHandleIndex, handle)
		if err != nil {
			continue
		}
		for _, obj := range objs {
			pv := obj.(*corev1.PersistentVolume)
			failoverHandle := pv.Spec.CSI.VolumeAttributes[VolumeContextFailoverVolumeHandle]
			if failoverHandle == "" {
				continue
			}
			primary, _, err := splitSource(pv.Spec.CSI.VolumeHandle)
			if err != nil {
				continue
			}
			failover, _, err := splitSource(failoverHandle)
			if err != nil {
				continue
			}
			for _, mgs := range []string{primary, failover} {
				if _, ok := sides[mgs]; !ok {
					sides[mgs] = nil
				}
			}
			state, err := parseFailoverState(pv)
			if err != nil {
				continue
			}
			switch state.active {
			case FailoverPrimary:
				sides[primary] = append(sides[primary], pv.Name)
			case FailoverFailover:
				sides[failover] = append(sides[failover], pv.Name)
			}
		}
	}
	for _, pvs := range sides {
		sort.Strings(pvs)
	}
	return sides
}

// forceUnmountStaleMount force-unmounts a mount of the side that its volume
// has failed away from. MNT_FORCE makes Lustre abort the superblock's
// outstanding RPCs, so processes in the pod that are blocked on an
// unreachable server get EIO rather than hanging; the pod's bind mount shares
// that superblock. The mount point is then removed so that a container
// restarted in the pod fails to start, rather than writing into the node's
// local directory.
func forceUnmountStaleMount(targetPath string) error {
	err := syscall.Unmount(targetPath, syscall.MNT_FORCE)
	if err != nil && !errors.Is(err, syscall.EINVAL) && !errors.Is(err, syscall.ENOENT) {
		return err
	}
	if err := os.Remove(targetPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
