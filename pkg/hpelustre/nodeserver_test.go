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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	mount "k8s.io/mount-utils"
)

// fakeMounts stands in for /proc/self/mountinfo and umount(2).
type fakeMounts struct {
	mu       sync.Mutex
	mounts   map[string]*mount.MountInfo
	unmounts []int         // flags of each unmount
	block    chan struct{} // if set, unmounts wait for it to be closed
}

func (f *fakeMounts) add(path, source string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.mounts == nil {
		f.mounts = map[string]*mount.MountInfo{}
	}
	f.mounts[path] = &mount.MountInfo{MountPoint: path, FsType: "lustre", Source: source}
}

func (f *fakeMounts) find(path string) (*mount.MountInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.mounts[path], nil
}

func (f *fakeMounts) unmount(path string, flags int) error {
	f.mu.Lock()
	block := f.block
	f.mu.Unlock()
	if block != nil {
		<-block
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unmounts = append(f.unmounts, flags)
	if f.mounts[path] == nil {
		return unix.EINVAL
	}
	delete(f.mounts, path)
	return nil
}

func (f *fakeMounts) unmountFlags() []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int(nil), f.unmounts...)
}

// fakeMounter mounts like mount.FakeMounter, unless told to fail.
type fakeMounter struct {
	*mount.FakeMounter
	err     error
	onMount func(source, target string)
}

func (m *fakeMounter) MountSensitiveWithoutSystemdWithMountFlags(
	source, target, fstype string, options, sensitiveOptions, mountFlags []string,
) error {
	if m.onMount != nil {
		m.onMount(source, target)
	}
	if m.err != nil {
		return m.err
	}
	return m.FakeMounter.MountSensitiveWithoutSystemdWithMountFlags(
		source, target, fstype, options, sensitiveOptions, mountFlags)
}

type nodeTestDriver struct {
	*Driver
	mounts  *fakeMounts
	mounter *fakeMounter
	// statVolume blocks while this is open, unless statErr is set.
	statBlock chan struct{}
	statErr   error
	statCalls atomic.Int32
}

func newTestDriver(t *testing.T) *nodeTestDriver {
	d := NewDriver(&DriverOptions{
		NodeID:              "node1",
		DriverName:          testDriver,
		OperationTimeout:    5 * time.Second,
		StatfsTimeout:       200 * time.Millisecond,
		LNetPingTimeout:     50 * time.Millisecond,
		CheckMGSBeforeMount: true,
	})
	td := &nodeTestDriver{
		Driver:    d,
		mounts:    &fakeMounts{},
		mounter:   &fakeMounter{FakeMounter: mount.NewFakeMounter(nil)},
		statBlock: make(chan struct{}),
	}
	close(td.statBlock)
	d.mounter = &mount.SafeFormatAndMount{Interface: td.mounter}
	d.findMount = td.mounts.find
	d.unmount = td.mounts.unmount
	d.lustreModulesLoaded = func() bool { return true }
	d.statVolume = func(path string) (*csi.NodeGetVolumeStatsResponse, error) {
		td.statCalls.Add(1)
		<-td.statBlock
		if td.statErr != nil {
			return nil, td.statErr
		}
		return &csi.NodeGetVolumeStatsResponse{}, nil
	}
	return td
}

// hangStatfs makes statfs block until the test ends, like it does on a
// Lustre filesystem whose servers are unreachable.
func (td *nodeTestDriver) hangStatfs(t *testing.T) {
	td.statBlock = make(chan struct{})
	t.Cleanup(func() { close(td.statBlock) })
}

func (td *nodeTestDriver) unpublish(target string) error {
	_, err := td.NodeUnpublishVolume(context.Background(),
		&csi.NodeUnpublishVolumeRequest{VolumeId: testVolumeID, TargetPath: target})
	return err
}

func publishRequest(target string) *csi.NodePublishVolumeRequest {
	return &csi.NodePublishVolumeRequest{
		VolumeId:   testVolumeID,
		TargetPath: target,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{FsType: "lustre"},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			},
		},
		VolumeContext: map[string]string{VolumeContextFailoverVolumeHandle: testFailoverHandle},
	}
}

// newMountedTarget returns a target path that is mounted from the primary.
func (td *nodeTestDriver) newMountedTarget(t *testing.T) string {
	target := newTargetPath(t, "pv1")
	require.NoError(t, os.Mkdir(target, 0o755))
	td.mounts.add(target, testVolumeID)
	return target
}

func TestUnpublishNotMounted(t *testing.T) {
	td := newTestDriver(t)
	target := newTargetPath(t, "pv1")
	require.NoError(t, os.Mkdir(target, 0o755))

	require.NoError(t, td.unpublish(target))
	assert.NoDirExists(t, target)
	assert.Empty(t, td.mounts.unmountFlags())

	// Unpublishing again is fine.
	require.NoError(t, td.unpublish(target))
}

func TestUnpublishResponsive(t *testing.T) {
	td := newTestDriver(t)
	target := td.newMountedTarget(t)

	require.NoError(t, td.unpublish(target))
	assert.Equal(t, []int{0}, td.mounts.unmountFlags())
	assert.NoDirExists(t, target)
}

func TestUnpublishUnresponsive(t *testing.T) {
	td := newTestDriver(t)
	td.hangStatfs(t)
	target := td.newMountedTarget(t)

	start := time.Now()
	require.NoError(t, td.unpublish(target))
	assert.GreaterOrEqual(t, time.Since(start), td.statfsTimeout)
	assert.Equal(t, []int{unix.MNT_FORCE}, td.mounts.unmountFlags())
	assert.NoDirExists(t, target)
}

func TestUnpublishBroken(t *testing.T) {
	td := newTestDriver(t)
	td.statErr = status.Error(codes.Internal, "input/output error")
	target := td.newMountedTarget(t)

	require.NoError(t, td.unpublish(target))
	assert.Equal(t, []int{unix.MNT_FORCE}, td.mounts.unmountFlags())
}

func TestUnpublishUnreachableMGSWaitsLess(t *testing.T) {
	td := newTestDriver(t)
	td.statfsTimeout = time.Minute
	td.hangStatfs(t)
	p := &fakePinger{}
	td.health = newHealthChecker(p.ping, td.lnetPingTimeout)
	mgs, _, err := splitSource(testVolumeID)
	require.NoError(t, err)
	require.Equal(t, SideUnreachable, td.health.check(mgs, time.Minute).State)
	target := td.newMountedTarget(t)

	start := time.Now()
	require.NoError(t, td.unpublish(target))
	assert.Less(t, time.Since(start), 10*time.Second)
	assert.Equal(t, []int{unix.MNT_FORCE}, td.mounts.unmountFlags())
}

func TestUnpublishBusyTarget(t *testing.T) {
	td := newTestDriver(t)
	td.operationTimeout = 100 * time.Millisecond
	td.mounts.block = make(chan struct{})
	target := td.newMountedTarget(t)

	requireCode(t, td.unpublish(target), codes.DeadlineExceeded)
	requireCode(t, td.unpublish(target), codes.Aborted)

	close(td.mounts.block)
	require.Eventually(t, func() bool { return td.unpublish(target) == nil },
		5*time.Second, 10*time.Millisecond)
	assert.NoDirExists(t, target)
	assert.Len(t, td.mounts.unmountFlags(), 1)
}

func TestVolumeStatsBounded(t *testing.T) {
	td := newTestDriver(t)
	req := &csi.NodeGetVolumeStatsRequest{VolumeId: testVolumeID, VolumePath: "/mnt/pv1"}

	_, err := td.NodeGetVolumeStats(context.Background(), req)
	require.NoError(t, err)

	td.hangStatfs(t)
	start := time.Now()
	_, err = td.NodeGetVolumeStats(context.Background(), req)
	requireCode(t, err, codes.DeadlineExceeded)
	assert.GreaterOrEqual(t, time.Since(start), td.statfsTimeout)

	// Later calls don't each leave another statfs blocked.
	start = time.Now()
	_, err = td.NodeGetVolumeStats(context.Background(), req)
	requireCode(t, err, codes.DeadlineExceeded)
	assert.Less(t, time.Since(start), td.statfsTimeout/2)
	assert.Equal(t, int32(2), td.statCalls.Load())
}

func newPublishingDriver(t *testing.T) (*nodeTestDriver, *testFailover, *fakePinger) {
	td := newTestDriver(t)
	f := newTestFailover(t, t.TempDir())
	f.setPV(t, failoverPV("pv1", testVolumeID, "primary", "1"))
	td.failover = f.failoverManager
	p := &fakePinger{}
	td.health = newHealthChecker(p.ping, td.lnetPingTimeout)
	return td, f, p
}

func TestPublishRefusesUnreachableMGS(t *testing.T) {
	td, f, _ := newPublishingDriver(t)
	target := newTargetPath(t, "pv1")

	_, err := td.NodePublishVolume(context.Background(), publishRequest(target))
	requireCode(t, err, codes.Unavailable)
	assert.Contains(t, err.Error(), "fail the volume over")
	mounts, _ := td.mounter.List()
	assert.Empty(t, mounts)
	assert.Empty(t, f.mounts)
}

func TestPublishMountsReachableMGS(t *testing.T) {
	td, f, p := newPublishingDriver(t)
	p.set("10.0.0.1@o2ib", SideReachable)
	target := newTargetPath(t, "pv1")

	// The side is recorded before it is mounted.
	td.mounter.onMount = func(source, mountTarget string) {
		assert.Equal(t, testVolumeID, source)
		f.mu.Lock()
		defer f.mu.Unlock()
		if assert.Contains(t, f.mounts, mountTarget) {
			assert.Equal(t, FailoverPrimary, f.mounts[mountTarget].Side)
		}
	}
	_, err := td.NodePublishVolume(context.Background(), publishRequest(target))
	require.NoError(t, err)
	mounts, _ := td.mounter.List()
	assert.Len(t, mounts, 1)
	assert.Contains(t, f.mounts, target)
}

func TestPublishMountsWhenMGSHealthUnknown(t *testing.T) {
	td, _, p := newPublishingDriver(t)
	for _, nid := range []string{"10.0.0.1@o2ib", "10.0.0.2@o2ib", "10.0.0.3@o2ib"} {
		p.set(nid, SideUnknown)
	}

	_, err := td.NodePublishVolume(context.Background(), publishRequest(newTargetPath(t, "pv1")))
	require.NoError(t, err)
}

func TestPublishWithoutMGSCheck(t *testing.T) {
	td, _, _ := newPublishingDriver(t)
	td.checkMGSBeforeMount = false

	_, err := td.NodePublishVolume(context.Background(), publishRequest(newTargetPath(t, "pv1")))
	require.NoError(t, err)
}

func TestPublishForgetsSideWhenMountFails(t *testing.T) {
	td, f, p := newPublishingDriver(t)
	p.set("10.0.0.1@o2ib", SideReachable)
	td.mounter.err = errors.New("mount failed")
	target := newTargetPath(t, "pv1")

	_, err := td.NodePublishVolume(context.Background(), publishRequest(target))
	requireCode(t, err, codes.Internal)
	assert.Empty(t, f.mounts)
	files, err := filepath.Glob(filepath.Join(f.stateDir, "*"))
	require.NoError(t, err)
	assert.Empty(t, files)
	assert.NoDirExists(t, target)
}

func TestFindMount(t *testing.T) {
	root, err := findMount("/")
	require.NoError(t, err)
	require.NotNil(t, root)
	assert.Equal(t, "/", root.MountPoint)

	notMounted, err := findMount(t.TempDir())
	require.NoError(t, err)
	assert.Nil(t, notMounted)
}

// A deployment's pods each mount four failover PVs, one per pair of
// filesystems with their own fsnames, several pods on the same node. One
// filesystem loses power, and its pair alone fails over.
func TestFailoverPairsWithManyPodsPerNode(t *testing.T) {
	td := newTestDriver(t)
	f := newTestFailover(t, t.TempDir())
	td.failover = f.failoverManager
	p := &fakePinger{}
	td.health = newHealthChecker(p.ping, td.lnetPingTimeout)

	type pair struct{ pv, primary, failover, primaryMGS string }
	var pairs []pair
	pairPV := func(pr pair, active, epoch string) *corev1.PersistentVolume {
		pv := failoverPV(pr.pv, pr.primary, active, epoch)
		pv.Spec.CSI.VolumeAttributes[VolumeContextFailoverVolumeHandle] = pr.failover
		return pv
	}
	for i := 1; i <= 4; i++ {
		pr := pair{
			pv:         fmt.Sprintf("pv%d", i),
			primary:    fmt.Sprintf("10.0.%d.1@o2ib:10.0.%d.2@o2ib:/fs%da", i, i, i),
			failover:   fmt.Sprintf("10.1.%d.1@o2ib:10.1.%d.2@o2ib:/fs%db", i, i, i),
			primaryMGS: fmt.Sprintf("10.0.%d.1@o2ib:10.0.%d.2@o2ib", i, i),
		}
		pairs = append(pairs, pr)
		p.set(fmt.Sprintf("10.0.%d.1@o2ib", i), SideReachable)
		p.set(fmt.Sprintf("10.1.%d.1@o2ib", i), SideReachable)
		f.setPV(t, pairPV(pr, "primary", "1"))
	}

	root := t.TempDir()
	target := func(pod string, pr pair) string {
		dir := filepath.Join(root, pod, pr.pv)
		require.NoError(t, os.MkdirAll(dir, 0o755))
		return filepath.Join(dir, "mount")
	}
	publish := func(pod string, pr pair) error {
		req := publishRequest(target(pod, pr))
		req.VolumeId = pr.primary
		req.VolumeContext[VolumeContextFailoverVolumeHandle] = pr.failover
		_, err := td.NodePublishVolume(context.Background(), req)
		return err
	}
	mountsOf := func(source string) int {
		mounts, err := td.mounter.List()
		require.NoError(t, err)
		n := 0
		for _, m := range mounts {
			if m.Device == source {
				n++
			}
		}
		return n
	}

	// Three pods on the node mount the four pairs, all at once.
	var wg sync.WaitGroup
	for _, pod := range []string{"pod1", "pod2", "pod3"} {
		for _, pr := range pairs {
			wg.Add(1)
			go func() {
				defer wg.Done()
				assert.NoError(t, publish(pod, pr), "%s %s", pod, pr.pv)
			}()
		}
	}
	wg.Wait()
	for _, pr := range pairs {
		assert.Equal(t, 3, mountsOf(pr.primary), pr.primary)
	}

	// The primary of the second pair loses power. A fourth pod scheduled on
	// the node mounts the other pairs, but not that one.
	p.set("10.0.2.1@o2ib", SideUnreachable)
	require.Equal(t, SideUnreachable, td.health.check(pairs[1].primaryMGS, 0).State)
	for _, pr := range pairs {
		err := publish("pod4", pr)
		if pr.pv == "pv2" {
			requireCode(t, err, codes.Unavailable)
		} else {
			assert.NoError(t, err, pr.pv)
		}
	}

	// The second pair alone fails over: only its three mounts are fenced.
	f.setPV(t, pairPV(pairs[1], "failover", "2"))
	var fenced []string
	for range 3 {
		select {
		case path := <-f.unmounted:
			fenced = append(fenced, path)
		case <-time.After(5 * time.Second):
			t.Fatalf("only fenced %v", fenced)
		}
	}
	assert.ElementsMatch(t, []string{
		target("pod1", pairs[1]), target("pod2", pairs[1]), target("pod3", pairs[1]),
	}, fenced)
	time.Sleep(100 * time.Millisecond)
	assert.Empty(t, f.unmounted)

	// The fourth pod now mounts the backup of the second pair, although the
	// node still has the stale mounts of its primary.
	require.NoError(t, publish("pod4", pairs[1]))
	assert.Equal(t, 1, mountsOf(pairs[1].failover))
	for _, pr := range []pair{pairs[0], pairs[2], pairs[3]} {
		assert.Equal(t, 4, mountsOf(pr.primary), pr.primary)
	}
	assert.Len(t, f.sides(), 8)
}
