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
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
)

const (
	testDriver         = "lustre-csi.hpe.com"
	testVolumeID       = "10.0.0.1@o2ib,10.0.0.2@o2ib:10.0.0.3@o2ib:/fz/projA"
	testFailoverHandle = "10.1.0.1@o2ib:10.1.0.3@o2ib:/fz/projA"
)

func failoverPV(name, volumeID, active, epoch string) *corev1.PersistentVolume {
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name, Annotations: map[string]string{}},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:       testDriver,
					VolumeHandle: volumeID,
					VolumeAttributes: map[string]string{
						VolumeContextFailoverVolumeHandle: testFailoverHandle,
					},
				},
			},
		},
	}
	if active != "" {
		pv.Annotations[FailoverActiveAnnotation] = active
	}
	if epoch != "" {
		pv.Annotations[FailoverEpochAnnotation] = epoch
	}
	return pv
}

type testFailover struct {
	*failoverManager
	unmounted chan string
}

func newTestFailover(t *testing.T, stateDir string) *testFailover {
	pvs := cache.NewIndexer(cache.MetaNamespaceKeyFunc,
		cache.Indexers{volumeHandleIndex: volumeHandleIndexFunc(testDriver)})
	m := newFailoverManagerWithIndexer(stateDir, true, pvs, func() bool { return true })
	unmounted := make(chan string, 10)
	m.forceUnmount = func(targetPath string) error {
		unmounted <- targetPath
		return nil
	}
	require.NoError(t, m.load())
	return &testFailover{failoverManager: m, unmounted: unmounted}
}

func (f *testFailover) setPV(t *testing.T, pv *corev1.PersistentVolume) {
	require.NoError(t, f.pvs.Update(pv))
	f.reconcile(pv.Spec.CSI.VolumeHandle)
}

func (f *testFailover) publish(t *testing.T, volumeID, targetPath string) string {
	source, rec, err := f.resolve(volumeID, testFailoverHandle, targetPath)
	require.NoError(t, err)
	require.NoError(t, f.record(targetPath, rec))
	return source
}

func (f *testFailover) requireUnmounted(t *testing.T, targetPath string) {
	t.Helper()
	select {
	case path := <-f.unmounted:
		assert.Equal(t, targetPath, path)
	case <-time.After(5 * time.Second):
		t.Fatalf("%s was not force-unmounted", targetPath)
	}
}

// newTargetPath returns a kubelet-like target path whose volume directory
// exists, so that it is not pruned as an orphan.
func newTargetPath(t *testing.T, name string) string {
	dir := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	return filepath.Join(dir, "mount")
}

func requireCode(t *testing.T, err error, code codes.Code) {
	t.Helper()
	require.Error(t, err)
	assert.Equal(t, code, status.Code(err), err.Error())
}

func TestParseFailoverState(t *testing.T) {
	state, err := parseFailoverState(failoverPV("pv", testVolumeID, "", ""))
	require.NoError(t, err)
	assert.Equal(t, failoverState{active: FailoverPrimary}, state)

	state, err = parseFailoverState(failoverPV("pv", testVolumeID, "failover", "3"))
	require.NoError(t, err)
	assert.Equal(t, failoverState{active: FailoverFailover, epoch: 3}, state)

	for _, tc := range [][2]string{{"secondary", "1"}, {"", "x"}, {"", "-1"}} {
		_, err := parseFailoverState(failoverPV("pv", testVolumeID, tc[0], tc[1]))
		assert.Error(t, err, tc)
	}
}

func TestSplitSource(t *testing.T) {
	nids, fsname, err := splitSource(testVolumeID)
	require.NoError(t, err)
	assert.Equal(t, "10.0.0.1@o2ib,10.0.0.2@o2ib:10.0.0.3@o2ib", nids)
	assert.Equal(t, "fz", fsname)

	_, fsname, err = splitSource("10.0.0.1@o2ib:/fz")
	require.NoError(t, err)
	assert.Equal(t, "fz", fsname)

	for _, source := range []string{"fz/projA", ":/fz", "10.0.0.1@o2ib:/", "10.0.0.1@o2ib"} {
		_, _, err := splitSource(source)
		assert.Error(t, err, source)
	}
}

func TestSameMGS(t *testing.T) {
	assert.True(t, sameMGS("10.0.0.1@o2ib,10.0.0.2@o2ib:10.0.0.3@o2ib", "10.0.0.3@o2ib"))
	assert.False(t, sameMGS("10.0.0.1@o2ib:10.0.0.3@o2ib", "10.1.0.1@o2ib:10.1.0.3@o2ib"))
}

func TestResolve(t *testing.T) {
	f := newTestFailover(t, t.TempDir())
	target := newTargetPath(t, "pv1")

	_, _, err := f.resolve(testVolumeID, testFailoverHandle, target)
	requireCode(t, err, codes.FailedPrecondition)

	f.setPV(t, failoverPV("pv1", testVolumeID, "", ""))
	source, rec, err := f.resolve(testVolumeID, testFailoverHandle, target)
	require.NoError(t, err)
	assert.Equal(t, testVolumeID, source)
	assert.Equal(t, &failoverMount{VolumeID: testVolumeID, Side: FailoverPrimary, Source: testVolumeID}, rec)

	f.setPV(t, failoverPV("pv1", testVolumeID, "failover", "4"))
	source, rec, err = f.resolve(testVolumeID, testFailoverHandle, target)
	require.NoError(t, err)
	assert.Equal(t, testFailoverHandle, source)
	assert.Equal(t, &failoverMount{VolumeID: testVolumeID, Side: FailoverFailover, Epoch: 4, Source: testFailoverHandle}, rec)

	_, _, err = f.resolve(testVolumeID, "fz/projA", target)
	requireCode(t, err, codes.InvalidArgument)

	f.setPV(t, failoverPV("pv1", testVolumeID, "draining", "4"))
	_, _, err = f.resolve(testVolumeID, testFailoverHandle, target)
	requireCode(t, err, codes.Unavailable)

	f.setPV(t, failoverPV("pv1", testVolumeID, "bogus", "4"))
	_, _, err = f.resolve(testVolumeID, testFailoverHandle, target)
	requireCode(t, err, codes.FailedPrecondition)

	f.synced = func() bool { return false }
	_, _, err = f.resolve(testVolumeID, testFailoverHandle, target)
	requireCode(t, err, codes.Unavailable)
}

func TestResolveSharedVolumeHandle(t *testing.T) {
	f := newTestFailover(t, t.TempDir())
	target := newTargetPath(t, "pv1")

	f.setPV(t, failoverPV("pv1", testVolumeID, "failover", "2"))
	f.setPV(t, failoverPV("pv2", testVolumeID, "failover", "2"))
	other := failoverPV("pv3", testVolumeID, "", "")
	delete(other.Spec.CSI.VolumeAttributes, VolumeContextFailoverVolumeHandle)
	f.setPV(t, other)
	_, rec, err := f.resolve(testVolumeID, testFailoverHandle, target)
	require.NoError(t, err)
	assert.Equal(t, FailoverFailover, rec.Side)

	f.setPV(t, failoverPV("pv2", testVolumeID, "failover", "3"))
	_, _, err = f.resolve(testVolumeID, testFailoverHandle, target)
	requireCode(t, err, codes.FailedPrecondition)
}

func TestFailoverFencesStaleMounts(t *testing.T) {
	f := newTestFailover(t, t.TempDir())
	f.setPV(t, failoverPV("pv1", testVolumeID, "primary", "1"))

	stale := newTargetPath(t, "pv1")
	assert.Equal(t, testVolumeID, f.publish(t, testVolumeID, stale))

	// Republishing the same target path keeps using the same side.
	source, _, err := f.resolve(testVolumeID, testFailoverHandle, stale)
	require.NoError(t, err)
	assert.Equal(t, testVolumeID, source)

	f.setPV(t, failoverPV("pv1", testVolumeID, "failover", "2"))
	f.requireUnmounted(t, stale)

	// Further resyncs don't force-unmount it again.
	f.reconcile(testVolumeID)
	assert.Empty(t, f.unmounted)

	_, _, err = f.resolve(testVolumeID, testFailoverHandle, stale)
	requireCode(t, err, codes.Unavailable)

	// New mounts of the same fsname from the other MGS wait for the stale
	// mount to be unpublished.
	fresh := newTargetPath(t, "pv2")
	_, _, err = f.resolve(testVolumeID, testFailoverHandle, fresh)
	requireCode(t, err, codes.Unavailable)

	require.NoError(t, f.forget(stale))
	source, rec, err := f.resolve(testVolumeID, testFailoverHandle, fresh)
	require.NoError(t, err)
	assert.Equal(t, testFailoverHandle, source)
	assert.Equal(t, int64(2), rec.Epoch)
}

func TestFsnameConflictAcrossVolumes(t *testing.T) {
	f := newTestFailover(t, t.TempDir())
	volumeB := "10.0.0.3@o2ib:/fz/projB"
	f.setPV(t, failoverPV("pv1", testVolumeID, "primary", "1"))
	f.setPV(t, failoverPV("pv2", volumeB, "failover", "1"))

	f.publish(t, testVolumeID, newTargetPath(t, "pv1"))

	// projB is on the failover MGS, but this node still has fz from the
	// primary MGS, written with a different but overlapping NID list.
	_, _, err := f.resolve(volumeB, testFailoverHandle, newTargetPath(t, "pv2"))
	requireCode(t, err, codes.Unavailable)

	// A different fsname is not a conflict.
	f.setPV(t, failoverPV("pv3", "10.2.0.1@o2ib:/other", "primary", "1"))
	_, _, err = f.resolve("10.2.0.1@o2ib:/other", testFailoverHandle, newTargetPath(t, "pv3"))
	require.NoError(t, err)
}

func TestFailoverEpochBumpOnSameSide(t *testing.T) {
	f := newTestFailover(t, t.TempDir())
	f.setPV(t, failoverPV("pv1", testVolumeID, "primary", "1"))

	target := newTargetPath(t, "pv1")
	f.publish(t, testVolumeID, target)

	f.setPV(t, failoverPV("pv1", testVolumeID, "primary", "2"))
	f.requireUnmounted(t, target)
}

func TestDrainingDoesNotFence(t *testing.T) {
	f := newTestFailover(t, t.TempDir())
	f.setPV(t, failoverPV("pv1", testVolumeID, "primary", "1"))

	target := newTargetPath(t, "pv1")
	f.publish(t, testVolumeID, target)

	f.setPV(t, failoverPV("pv1", testVolumeID, "draining", "1"))
	time.Sleep(100 * time.Millisecond)
	assert.Empty(t, f.unmounted)

	// An already published volume can still be republished while draining.
	source, _, err := f.resolve(testVolumeID, testFailoverHandle, target)
	require.NoError(t, err)
	assert.Equal(t, testVolumeID, source)
}

func TestFailoverDisabledForceUnmount(t *testing.T) {
	f := newTestFailover(t, t.TempDir())
	f.forceUnmountStale = false
	f.setPV(t, failoverPV("pv1", testVolumeID, "primary", "1"))
	f.publish(t, testVolumeID, newTargetPath(t, "pv1"))

	f.setPV(t, failoverPV("pv1", testVolumeID, "failover", "2"))
	time.Sleep(100 * time.Millisecond)
	assert.Empty(t, f.unmounted)

	// Stale mounts still block mounting the other side.
	_, _, err := f.resolve(testVolumeID, testFailoverHandle, newTargetPath(t, "pv2"))
	requireCode(t, err, codes.Unavailable)
}

func TestFailoverStatePersists(t *testing.T) {
	stateDir := t.TempDir()
	f := newTestFailover(t, stateDir)
	f.setPV(t, failoverPV("pv1", testVolumeID, "primary", "1"))

	target := newTargetPath(t, "pv1")
	f.publish(t, testVolumeID, target)

	// A restarted driver still knows the mount is from the primary.
	restarted := newTestFailover(t, stateDir)
	restarted.setPV(t, failoverPV("pv1", testVolumeID, "failover", "2"))
	restarted.requireUnmounted(t, target)

	require.NoError(t, restarted.forget(target))
	files, err := filepath.Glob(filepath.Join(stateDir, "*"))
	require.NoError(t, err)
	assert.Empty(t, files)
}

func TestFailoverPrunesOrphans(t *testing.T) {
	f := newTestFailover(t, t.TempDir())
	f.forceUnmountStale = false
	f.setPV(t, failoverPV("pv1", testVolumeID, "primary", "1"))

	target := newTargetPath(t, "pv1")
	f.publish(t, testVolumeID, target)
	f.setPV(t, failoverPV("pv1", testVolumeID, "failover", "2"))

	// kubelet removed the volume directory without NodeUnpublishVolume.
	require.NoError(t, os.RemoveAll(filepath.Dir(target)))
	_, _, err := f.resolve(testVolumeID, testFailoverHandle, newTargetPath(t, "pv2"))
	require.NoError(t, err)
}

func TestFailoverSides(t *testing.T) {
	f := newTestFailover(t, t.TempDir())
	volumeB := "10.0.0.3@o2ib:/fz/projB"
	f.setPV(t, failoverPV("pv1", testVolumeID, "primary", "1"))
	f.setPV(t, failoverPV("pv2", volumeB, "failover", "1"))
	f.setPV(t, failoverPV("pv3", volumeB, "draining", "1"))
	plain := failoverPV("pv4", "10.2.0.1@o2ib:/other", "", "")
	delete(plain.Spec.CSI.VolumeAttributes, VolumeContextFailoverVolumeHandle)
	f.setPV(t, plain)

	assert.Equal(t, map[string][]string{
		"10.0.0.1@o2ib,10.0.0.2@o2ib:10.0.0.3@o2ib": {"pv1"},
		"10.0.0.3@o2ib":               nil,
		"10.1.0.1@o2ib:10.1.0.3@o2ib": {"pv2"},
	}, f.sides())
}

func TestFailoverRetriesFailedFence(t *testing.T) {
	f := newTestFailover(t, t.TempDir())
	f.fenceRetry = 10 * time.Millisecond
	var attempts atomic.Int32
	f.forceUnmount = func(targetPath string) error {
		if attempts.Add(1) == 1 {
			return unix.EBUSY
		}
		f.unmounted <- targetPath
		return nil
	}
	f.setPV(t, failoverPV("pv1", testVolumeID, "primary", "1"))
	target := newTargetPath(t, "pv1")
	f.publish(t, testVolumeID, target)

	f.setPV(t, failoverPV("pv1", testVolumeID, "failover", "2"))
	f.requireUnmounted(t, target)
	assert.Equal(t, int32(2), attempts.Load())
}

// Each side may have its own fsname, as is usual. Then no fsname is mounted
// from two MGSs, and a replacement pod mounts the new side at once, even on a
// node that still has the stale mount.
func TestFailoverDifferentFsnames(t *testing.T) {
	primary := "10.0.0.1@o2ib:10.0.0.3@o2ib:/fza/projA"
	failover := "10.1.0.1@o2ib:10.1.0.3@o2ib:/fzb/projA"
	pv := func(active, epoch string) *corev1.PersistentVolume {
		pv := failoverPV("pv1", primary, active, epoch)
		pv.Spec.CSI.VolumeAttributes[VolumeContextFailoverVolumeHandle] = failover
		return pv
	}
	f := newTestFailover(t, t.TempDir())
	f.setPV(t, pv("primary", "1"))

	stale := newTargetPath(t, "pod1")
	source, rec, err := f.resolve(primary, failover, stale)
	require.NoError(t, err)
	assert.Equal(t, primary, source)
	require.NoError(t, f.record(stale, rec))

	f.setPV(t, pv("failover", "2"))
	f.requireUnmounted(t, stale)

	source, rec, err = f.resolve(primary, failover, newTargetPath(t, "pod2"))
	require.NoError(t, err)
	assert.Equal(t, failover, source)
	assert.Equal(t, FailoverFailover, rec.Side)
	assert.Equal(t, int64(2), rec.Epoch)

	assert.Equal(t, map[string][]string{
		"10.0.0.1@o2ib:10.0.0.3@o2ib": nil,
		"10.1.0.1@o2ib:10.1.0.3@o2ib": {"pv1"},
	}, f.sides())
}
