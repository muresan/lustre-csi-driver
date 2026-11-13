/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package hpelustre

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	volumehelper "github.com/HewlettPackard/lustre-csi-driver/pkg/util"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"golang.org/x/net/context"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/volume"
	mount "k8s.io/mount-utils"
)

// NodePublishVolume mount the volume from staging to target path
func (d *Driver) NodePublishVolume(
	ctx context.Context,
	req *csi.NodePublishVolumeRequest,
) (*csi.NodePublishVolumeResponse, error) {

	if req.GetVolumeCapability() == nil {
		return nil, status.Error(codes.InvalidArgument,
			"Volume capability missing in request")
	}
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument,
			"Volume ID missing in request")
	}
	target := req.GetTargetPath()
	if len(target) == 0 {
		return nil, status.Error(codes.InvalidArgument,
			"Target path not provided")
	}
	if req.GetVolumeContext() == nil {
		return nil, status.Error(codes.InvalidArgument,
			"Volume context must be provided")
	}

	_, err := d.ops.do(ctx, target, "NodePublishVolume", d.operationTimeout, false,
		func() (any, error) { return nil, d.publishVolume(req) })
	if err != nil {
		return nil, err
	}
	return &csi.NodePublishVolumeResponse{}, nil
}

// publishVolume mounts a volume at its target path. It may run on after
// NodePublishVolume gave up waiting on it.
func (d *Driver) publishVolume(req *csi.NodePublishVolumeRequest) error {
	volCap := req.GetVolumeCapability()
	userMountFlags := volCap.GetMount().GetMountFlags()
	volumeType := volCap.GetMount().GetFsType()
	volumeID := req.GetVolumeId()
	target := req.GetTargetPath()
	context := req.GetVolumeContext()

	vol, err := getVolume(volumeID, context)
	if err != nil {
		return err
	}

	//source := getSourceString(vol.mgsIPAddress, vol.hpeLustreName)
	source := volumeID

	var failoverRec *failoverMount
	if failoverHandle := context[VolumeContextFailoverVolumeHandle]; failoverHandle != "" {
		if d.failover == nil {
			return status.Errorf(codes.FailedPrecondition,
				"Volume %s has a %s, but failover is not enabled",
				volumeID, VolumeContextFailoverVolumeHandle)
		}
		source, failoverRec, err = d.failover.resolve(volumeID, failoverHandle, target)
		if err != nil {
			return err
		}
		klog.V(2).Infof(
			"NodePublishVolume: volume %s resolved to its %s %s, epoch %d",
			volumeID, failoverRec.Side, source, failoverRec.Epoch,
		)
	}

	mountOptions, readOnly := getMountOptions(req, userMountFlags)

	if len(vol.subDir) > 0 && !d.enableHpeLustreMockMount {
		interpolatedSubDir := interpolateSubDirVariables(context, vol)

		if isSubpath := ensureStrictSubpath(interpolatedSubDir); !isSubpath {
			return status.Error(
				codes.InvalidArgument,
				"Context sub-dir must be strict subpath",
			)
		}

		if readOnly {
			klog.V(2).Info("NodePublishVolume: not attempting to create sub-dir on read-only volume, assuming existing path")
		} else {
			klog.V(2).Infof(
				"NodePublishVolume: sub-dir will be created at %q",
				interpolatedSubDir,
			)

			if err = d.createSubDir(vol, target, interpolatedSubDir, mountOptions); err != nil {
				return err
			}
		}

		source = filepath.Join(source, interpolatedSubDir)
		klog.V(2).Infof(
			"NodePublishVolume: full mount source with sub-dir: %q",
			source,
		)
	}

	mnt, err := d.ensureMountPoint(target)
	if err != nil {
		return status.Errorf(codes.Internal,
			"Could not mount target %q: %v",
			target,
			err)
	}
	if mnt {
		klog.V(2).Infof(
			"NodePublishVolume: volume %s is already mounted on %s",
			volumeID,
			target,
		)
		return d.recordFailover(target, failoverRec)
	}

	if err := d.checkMGS(source, failoverRec); err != nil {
		return err
	}

	// Record which side is mounted before mounting it, so that a mount is
	// never left without a record of its side should the driver stop
	// before the mount completes.
	if err := d.recordFailover(target, failoverRec); err != nil {
		return err
	}

	//klog.V(2).Infof(
	//	"NodePublishVolume: volume %s mounting %s at %s with mountOptions: %v",
	//	volumeID, source, target, mountOptions,
	//)
	klog.V(2).Infof(
		"NodePublishVolume: volume %s mounting at %s, type %s, with mountOptions: %v",
		source, target, volumeType, mountOptions,
	)
	if d.enableHpeLustreMockMount {
		klog.Warningf(
			"NodePublishVolume: mock mount on volumeID(%s), this is only for"+
				"TESTING!!!",
			volumeID,
		)
		if err := volumehelper.MakeDir(target); err != nil {
			klog.Errorf("MakeDir failed on target: %s (%v)", target, err)
			d.forgetFailover(target, failoverRec)
			return err
		}
		return nil
	}

	err = mountVolumeAtPath(d, source, target, volumeType, mountOptions)
	if err != nil {
		d.forgetFailover(target, failoverRec)
		if removeErr := os.Remove(target); removeErr != nil {
			return status.Errorf(
				codes.Internal,
				"Could not remove mount target %q: %v",
				target,
				removeErr,
			)
		}
		return status.Errorf(codes.Internal,
			"Could not mount %q at %q: %v", source, target, err)
	}

	//klog.V(2).Infof(
	//	"NodePublishVolume: volume %s mount %s at %s successfully",
	//	volumeID,
	//	source,
	//	target,
	//)
	klog.V(2).Infof(
		"NodePublishVolume: volume mount %s at %s successfully",
		source,
		target,
	)

	return nil
}

// recordFailover records which side a volume with a failover volume handle
// is published from.
func (d *Driver) recordFailover(target string, rec *failoverMount) error {
	if rec == nil {
		return nil
	}
	if err := d.failover.record(target, rec); err != nil {
		return status.Errorf(codes.Internal,
			"Could not record failover state for %q: %v", target, err)
	}
	return nil
}

// forgetFailover drops the record of a volume that failed to be published.
func (d *Driver) forgetFailover(target string, rec *failoverMount) {
	if rec == nil {
		return
	}
	if err := d.failover.forget(target); err != nil {
		klog.Warningf("Could not forget failover state of %q: %v", target, err)
	}
}

// checkMGS refuses to mount source if its MGS doesn't answer LNet pings,
// rather than letting the mount block for minutes until it fails.
func (d *Driver) checkMGS(source string, rec *failoverMount) error {
	if d.health == nil || !d.checkMGSBeforeMount || d.enableHpeLustreMockMount ||
		(d.swapSourceFrom != "" && source == d.swapSourceFrom) {
		return nil
	}
	mgs, _, err := splitSource(source)
	if err != nil {
		return nil
	}
	h := d.health.check(mgs, mountCheckMaxAge)
	if h.State != SideUnreachable {
		return nil
	}
	hint := ""
	if rec != nil {
		hint = fmt.Sprintf(". It is the %s side of volume %s: if its site is down, fail the volume over",
			rec.Side, rec.VolumeID)
	}
	return status.Errorf(codes.Unavailable,
		"Not mounting %s: its MGS has not answered LNet pings from this node since %s (%s)%s",
		source, h.Since.Format(time.RFC3339), h.Detail, hint)
}

func interpolateSubDirVariables(context map[string]string, vol *lustreVolume) string {
	subDirReplaceMap := map[string]string{}

	// get metadata values
	for k, v := range context {
		switch strings.ToLower(k) {
		case podNameKey:
			subDirReplaceMap[podNameMetadata] = v
		case podNamespaceKey:
			subDirReplaceMap[podNamespaceMetadata] = v
		case podUIDKey:
			subDirReplaceMap[podUIDMetadata] = v
		case serviceAccountNameKey:
			subDirReplaceMap[serviceAccountNameMetadata] = v
		case pvcNamespaceKey:
			subDirReplaceMap[pvcNamespaceMetadata] = v
		case pvcNameKey:
			subDirReplaceMap[pvcNameMetadata] = v
		case pvNameKey:
			subDirReplaceMap[pvNameMetadata] = v
		}
	}

	interpolatedSubDir := volumehelper.ReplaceWithMap(vol.subDir, subDirReplaceMap)
	return interpolatedSubDir
}

func getMountOptions(req *csi.NodePublishVolumeRequest, userMountFlags []string) ([]string, bool) {
	readOnly := false
	mountOptions := []string{}
	if req.GetReadonly() {
		readOnly = true
		mountOptions = append(mountOptions, "ro")
	}
	for _, userMountFlag := range userMountFlags {
		if userMountFlag == "ro" {
			readOnly = true

			if req.GetReadonly() {
				continue
			}
		}
		mountOptions = append(mountOptions, userMountFlag)
	}
	return mountOptions, readOnly
}

func getVolume(volumeID string, context map[string]string) (*lustreVolume, error) {
	return &lustreVolume{}, nil
}

// The original getVolume(). It's attempting to interpret more complex volume
// references. Not currently in use.
func xx_getVolume(volumeID string, context map[string]string) (*lustreVolume, error) {
	volName := ""

	volFromID, err := getLustreVolFromID(volumeID)
	if err != nil {
		klog.Warningf("error parsing volume ID '%v'", err)
	} else {
		volName = volFromID.name
	}

	vol, err := newLustreVolume(volumeID, volName, context)
	if err != nil {
		return nil, err
	}

	if volFromID != nil && *volFromID != *vol {
		klog.Warningf("volume context does not match values in volume ID for volumeID %v", volumeID)
	}

	return vol, nil
}

func mountVolumeAtPath(d *Driver, source, target string, volumeType string, mountOptions []string) error {
	// The first mount on a node loads the Lustre kernel modules. As a
	// precaution, mounts are serialized until they are loaded, but no longer:
	// a mount from an unreachable MGS blocks for minutes, and must not hold
	// up the mounts of other volumes.
	if !d.lustreModulesLoaded() {
		d.kernelModuleLock.Lock()
		defer d.kernelModuleLock.Unlock()
	}
	klog.Infof("Mount source '%s' (%s) at '%s', with options (%s)", source, volumeType, target, mountOptions)
	if d.swapSourceFrom != "" && source == d.swapSourceFrom {
		klog.Warningf("Swapping source '%s' to '%s' (%s) in mountVolumeAtPath", d.swapSourceFrom, d.swapSourceTo, d.swapSourceToFSType)
		source = d.swapSourceTo
		volumeType = d.swapSourceToFSType
	}
	err := d.mounter.MountSensitiveWithoutSystemdWithMountFlags(
		source,
		target,
		volumeType, // "lustre",
		mountOptions,
		nil,
		[]string{"--no-mtab"},
	)
	return err
}

// NodeUnpublishVolume unmount the volume from the target path
func (d *Driver) NodeUnpublishVolume(
	ctx context.Context,
	req *csi.NodeUnpublishVolumeRequest,
) (*csi.NodeUnpublishVolumeResponse, error) {

	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument,
			"Volume ID missing in request")
	}

	targetPath := req.GetTargetPath()
	if len(targetPath) == 0 {
		return nil, status.Error(codes.InvalidArgument,
			"Target path missing in request")
	}

	klog.V(2).Infof("NodeUnpublishVolume: unmounting volume %s on %s",
		volumeID, targetPath)
	_, err := d.ops.do(ctx, targetPath, "NodeUnpublishVolume", d.operationTimeout, false,
		func() (any, error) { return nil, d.unpublishVolume(targetPath) })
	if err != nil {
		return nil, err
	}
	klog.V(2).Infof(
		"NodeUnpublishVolume: unmount volume %s on %s successfully",
		volumeID,
		targetPath,
	)

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

// unpublishVolume unmounts a volume from its target path. It may run on after
// NodeUnpublishVolume gave up waiting on it.
func (d *Driver) unpublishVolume(targetPath string) error {
	if err := unmountVolumeAtPath(d, targetPath); err != nil {
		return status.Errorf(codes.Internal,
			"failed to unmount target %q: %v", targetPath, err)
	}
	if d.failover != nil {
		if err := d.failover.forget(targetPath); err != nil {
			return status.Errorf(codes.Internal,
				"failed to forget failover state of %q: %v", targetPath, err)
		}
	}
	return nil
}

// How many mounts stacked on a target path to unmount.
const maxStackedMounts = 8

// unmountVolumeAtPath unmounts targetPath and removes it, without blocking
// on a filesystem whose servers are unreachable. It tells whether
// targetPath is mounted from /proc/self/mountinfo rather than by stat'ing
// it, which would block. It unmounts with umount(2), which doesn't look up
// the mounted filesystem's root, rather than with umount(8), which stats
// it. And it force-unmounts a filesystem that doesn't answer statfs in time,
// since a plain unmount would then block on flushing it, and Lustre aborts
// the outstanding RPCs of a forced unmount.
func unmountVolumeAtPath(d *Driver, targetPath string) error {
	for range maxStackedMounts {
		mnt, err := d.findMount(targetPath)
		if err != nil {
			return err
		}
		if mnt == nil {
			break
		}
		flags := 0
		if err := d.checkResponsive(targetPath, mnt); err != nil {
			klog.Warningf("%s does not respond, force-unmounting it: %v", targetPath, err)
			flags = unix.MNT_FORCE
		}
		klog.V(4).Infof("Unmounting %s (%s from %s), flags %#x", targetPath, mnt.FsType, mnt.Source, flags)
		err = d.unmount(targetPath, flags)
		if errors.Is(err, unix.EINVAL) || errors.Is(err, unix.ENOENT) {
			// Not or no longer a mount point.
			break
		}
		if err != nil {
			return fmt.Errorf("could not unmount %s: %w", targetPath, err)
		}
	}
	if err := os.Remove(targetPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("could not remove %s: %w", targetPath, err)
	}
	return nil
}

// checkResponsive checks that the filesystem mounted at path answers statfs
// in time. It waits less if the filesystem's MGS was last seen not answering
// LNet pings.
func (d *Driver) checkResponsive(path string, mnt *mount.MountInfo) error {
	timeout := d.statfsTimeout
	if d.health != nil && mnt.FsType == "lustre" {
		if mgs, _, err := splitSource(mnt.Source); err == nil {
			if h := d.health.cached(mgs, unmountCheckMaxAge); h.State == SideUnreachable {
				timeout = min(timeout, d.lnetPingTimeout)
			}
		}
	}
	_, err := d.volumeStats(context.Background(), path, timeout)
	return err
}

// findMount returns the topmost mount at path, or nil if path is not a mount
// point. Unlike stat, reading /proc/self/mountinfo never calls into the
// mounted filesystem, so it can't block on unreachable servers.
func findMount(path string) (*mount.MountInfo, error) {
	infos, err := mount.ParseMountInfo("/proc/self/mountinfo")
	if err != nil {
		return nil, err
	}
	path = filepath.Clean(path)
	var found *mount.MountInfo
	for i := range infos {
		if infos[i].MountPoint == path {
			found = &infos[i]
		}
	}
	return found, nil
}

// lustreModulesLoaded reports whether the Lustre client kernel modules are
// loaded.
func lustreModulesLoaded() bool {
	_, err := os.Stat("/sys/module/lustre")
	return err == nil
}

// Staging and Unstaging is not able to be supported with how Lustre is mounted
//
// This was discovered during a proof of concept implementation and the issue
// is as follows:
//
// When the kubelet process attempts to unstage / unmount a Lustre mount that
// has been staged to a global mount point, it performs extra checks to ensure
// that the same device is not mounted anywhere else in the filesystem. For
// usual configurations, this would be a reasonable check to ensure that we
// aren't trying to remove something that is still in use elsewhere in the
// system. However, the way Lustre mounts are configured is not compatible
// with the check it performs.
//
// The kubelet process does this by checking all of the mount points on the
// node to see if any have the following:
// 1) The same 'root' value of the mount that is being cleaned
// 2) The same device number of the mount that is being cleaned
// And that those mounts are in a different path tree.
// If so, it returns this error: "the device mount path %q is still mounted
// by other references %v", deviceMountPath, refs) and fails the unmount.
// See pkg/volume/util/operationexecutor/operation_generator.go
// calling GetDeviceMountRefs(deviceMountPath) around line 947.
//
// All Lustre mounts on a system, no matter where in the lustrefs they are
// mounted to, all have '/' as the root and they all have the same major and
// minor device numbers, so as far as this check is concerned, every lustre
// mount is the same device, even though individual Lustre mount points can
// be unmounted without affecting others and should not be a concern.
//
// With a single Lustre volume mount, this works fine. It stages to a
// globalpath dir, pods can bind mount into that, and when the last pod is
// done, unstage is called and the global mount point can be cleaned up,
// because that is the only lustre mount so kubelet has no issue with
// 'other mounts' on the same node.
//
// The problem occurs when two different volumes are trying to mount a
// Lustre cluster. In that case, pods for the first volume can come up
// as expected with their global mount path, then pods for the second
// volume with their global mount path. The error occurs when the pods
// for one of these volumes are deleted and an unstage action should occur,
// because the other volume has its own Lustre mount, so it fails this
// check. For example, it's trying to unmount
// /var/...<firstvolume>.../globalpath, but there's another volume at
// /var/...<secondvolume>.../globalpath with the same root '/' and major
// and minor device numbers.
//
// It errors out, fails the unmount, and never calls unstage, even
// though all of the pods using that volume have already been deleted.
// This leaves the box with as many global mount directories still mounted
// to the Lustre cluster as you've ever staged, but without any way to see
// this other than looking at the mounts on the node or in the kubelet logs.
func (d *Driver) NodeStageVolume(_ context.Context, _ *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

// Staging and Unstaging is not able to be supported with how Lustre is mounted
//
// See NodeStageVolume for more details
func (d *Driver) NodeUnstageVolume(_ context.Context, _ *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

// NodeGetCapabilities return the capabilities of the Node plugin
func (d *Driver) NodeGetCapabilities(
	_ context.Context, _ *csi.NodeGetCapabilitiesRequest,
) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: d.NSCap,
	}, nil
}

// NodeGetInfo return info of the node on which this plugin is running
func (d *Driver) NodeGetInfo(
	_ context.Context,
	_ *csi.NodeGetInfoRequest,
) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{
		NodeId: d.NodeID,
	}, nil
}

// NodeGetVolumeStats get volume stats
func (d *Driver) NodeGetVolumeStats(
	ctx context.Context,
	req *csi.NodeGetVolumeStatsRequest,
) (*csi.NodeGetVolumeStatsResponse, error) {
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument,
			"NodeGetVolumeStats volume ID was empty")
	}
	volumePath := req.GetVolumePath()
	if len(volumePath) == 0 {
		return nil, status.Error(codes.InvalidArgument,
			"NodeGetVolumeStats volume path was empty")
	}

	return d.volumeStats(ctx, volumePath, d.statfsTimeout)
}

// volumeStats returns the usage of the volume mounted at path, giving up
// after timeout. Calls share a statfs in progress, which counts towards the
// timeout, so that kubelet's periodic calls on a volume whose servers are
// unreachable don't each leave another thread blocked in statfs.
func (d *Driver) volumeStats(ctx context.Context, path string, timeout time.Duration) (*csi.NodeGetVolumeStatsResponse, error) {
	val, err := d.statfsOps.do(ctx, path, "statfs", timeout, true,
		func() (any, error) { return d.statVolume(path) })
	if err != nil {
		return nil, err
	}
	return val.(*csi.NodeGetVolumeStatsResponse), nil
}

// statVolume returns the usage of the volume mounted at volumePath.
func statVolume(volumePath string) (*csi.NodeGetVolumeStatsResponse, error) {
	if _, err := os.Lstat(volumePath); err != nil {
		if os.IsNotExist(err) {
			return nil, status.Errorf(codes.NotFound,
				"path %s does not exist", volumePath)
		}
		return nil, status.Errorf(codes.Internal,
			"failed to stat file %s: %v", volumePath, err)
	}

	volumeMetrics, err := volume.NewMetricsStatFS(volumePath).GetMetrics()
	if err != nil {
		return nil, status.Errorf(codes.Internal,
			"failed to get metrics: %v", err)
	}

	available, ok := volumeMetrics.Available.AsInt64()
	if !ok {
		return nil, status.Errorf(codes.Internal,
			"failed to transform volume available size(%v)",
			volumeMetrics.Available)
	}
	capacity, ok := volumeMetrics.Capacity.AsInt64()
	if !ok {
		return nil, status.Errorf(codes.Internal,
			"failed to transform volume capacity size(%v)",
			volumeMetrics.Capacity)
	}
	used, ok := volumeMetrics.Used.AsInt64()
	if !ok {
		return nil, status.Errorf(codes.Internal,
			"failed to transform volume used size(%v)", volumeMetrics.Used)
	}

	inodesFree, ok := volumeMetrics.InodesFree.AsInt64()
	if !ok {
		return nil, status.Errorf(codes.Internal,
			"failed to transform disk inodes free(%v)",
			volumeMetrics.InodesFree)
	}
	inodes, ok := volumeMetrics.Inodes.AsInt64()
	if !ok {
		return nil, status.Errorf(codes.Internal,
			"failed to transform disk inodes(%v)", volumeMetrics.Inodes)
	}
	inodesUsed, ok := volumeMetrics.InodesUsed.AsInt64()
	if !ok {
		return nil, status.Errorf(codes.Internal,
			"failed to transform disk inodes used(%v)",
			volumeMetrics.InodesUsed)
	}

	return &csi.NodeGetVolumeStatsResponse{
		Usage: []*csi.VolumeUsage{
			{
				Unit:      csi.VolumeUsage_BYTES,
				Available: available,
				Total:     capacity,
				Used:      used,
			},
			{
				Unit:      csi.VolumeUsage_INODES,
				Available: inodesFree,
				Total:     inodes,
				Used:      inodesUsed,
			},
		},
	}, nil
}

// ensureMountPoint: create mount point if not exists
// return <true, nil> if it's already a mounted point
// otherwise return <false, nil>
func (d *Driver) ensureMountPoint(target string) (bool, error) {
	notMnt, err := d.mounter.IsLikelyNotMountPoint(target)
	if err != nil && !os.IsNotExist(err) {
		if IsCorruptedDir(target) {
			notMnt = false
			klog.Warningf("detected corrupted mount for targetPath [%s]",
				target)
		} else {
			return !notMnt, err
		}
	}

	if !notMnt {
		// testing original mount point, make sure the mount link is valid
		_, err := os.ReadDir(target)
		if err == nil {
			klog.V(2).Infof("already mounted to target %s", target)
			return !notMnt, nil
		}
		// mount link is invalid, now unmount and remount later
		klog.Warningf("ReadDir %s failed with %v, unmount this directory",
			target, err)
		if err := d.mounter.Unmount(target); err != nil {
			klog.Errorf("Unmount directory %s failed with %v", target, err)
			return !notMnt, err
		}
		notMnt = true
		return !notMnt, err
	}
	if err := volumehelper.MakeDir(target); err != nil {
		klog.Errorf("MakeDir failed on target: %s (%v)", target, err)
		return !notMnt, err
	}
	return !notMnt, nil
}

func (d *Driver) createSubDir(vol *lustreVolume, mountPath, subDirPath string, mountOptions []string) error {
	if err := d.internalMount(vol, mountPath, mountOptions); err != nil {
		return err
	}

	defer func() {
		if err := d.internalUnmount(mountPath); err != nil {
			klog.Warningf("failed to unmount lustre server: %v", err.Error())
		}
	}()

	internalVolumePath, err := getInternalVolumePath(d.workingMountDir, mountPath, subDirPath)
	if err != nil {
		return err
	}

	klog.V(2).Infof("Making subdirectory at %q", internalVolumePath)

	if err := os.MkdirAll(internalVolumePath, 0o775); err != nil {
		return status.Errorf(codes.Internal, "failed to make subdirectory: %v", err.Error())
	}

	return nil
}

func getSourceString(mgsIPAddress, lustreName string) string {
	if lustreName[0] != '/' {
		lustreName = "/" + lustreName
	}
	var src string
	if strings.Contains(mgsIPAddress, "@") {
		src = fmt.Sprintf("%s:%s", mgsIPAddress, lustreName)
	} else {
		src = fmt.Sprintf("%s@tcp:%s", mgsIPAddress, lustreName)
	}
	return src
}

func getInternalMountPath(workingMountDir, mountPath string) (string, error) {
	mountPath = strings.Trim(mountPath, "/")

	if isSubpath := ensureStrictSubpath(mountPath); !isSubpath {
		return "", status.Errorf(
			codes.Internal,
			"invalid mount path %q",
			mountPath,
		)
	}

	return filepath.Join(workingMountDir, mountPath), nil
}

func getInternalVolumePath(workingMountDir, mountPath, subDirPath string) (string, error) {
	internalMountPath, err := getInternalMountPath(workingMountDir, mountPath)
	if err != nil {
		return "", err
	}

	if isSubpath := ensureStrictSubpath(subDirPath); !isSubpath {
		return "", status.Errorf(
			codes.InvalidArgument,
			"sub-dir %q must be strict subpath",
			subDirPath,
		)
	}

	return filepath.Join(internalMountPath, subDirPath), nil
}

func (d *Driver) internalMount(vol *lustreVolume, mountPath string, mountOptions []string) error {
	source := getSourceString(vol.mgsIPAddress, vol.hpeLustreName)

	target, err := getInternalMountPath(d.workingMountDir, mountPath)
	if err != nil {
		return err
	}

	klog.V(4).Infof("internally mounting %v", target)

	mnt, err := d.ensureMountPoint(target)
	if err != nil {
		return status.Errorf(codes.Internal,
			"Could not mount target %q: %v",
			target,
			err)
	}

	if mnt {
		klog.Warningf(
			"volume %q is already mounted on %q",
			vol.id,
			target,
		)

		err = d.internalUnmount(mountPath)
		if err != nil {
			return status.Errorf(codes.Internal,
				"Could not unmount existing volume at %q: %v",
				target,
				err)
		}
	}

	klog.V(2).Infof(
		"volume %q mounting %q at %q with mountOptions: %v",
		vol.id, source, target, mountOptions,
	)

	err = mountVolumeAtPath(d, source, target, "lustre", mountOptions)
	if err != nil {
		if removeErr := os.Remove(target); removeErr != nil {
			return status.Errorf(
				codes.Internal,
				"Could not remove mount target %q: %v",
				target,
				removeErr,
			)
		}

		return status.Errorf(codes.Internal,
			"Could not mount %q at %q: %v", source, target, err)
	}

	return nil
}

func (d *Driver) internalUnmount(mountPath string) error {
	target, err := getInternalMountPath(d.workingMountDir, mountPath)
	if err != nil {
		return err
	}

	klog.V(4).Infof("internally unmounting %v", target)

	err = mount.CleanupMountWithForce(target, *d.forceMounter, true, 10*time.Second)
	if err != nil {
		err = status.Errorf(codes.Internal, "failed to unmount staging target %q: %v", target, err)
	}

	return err
}

// Ensures that the given subpath, when joined with any base path, will be a path
// within the given base path, and not equal to it. This ensures that this
// subpath value can be safely created or deleted under the base path without
// affecting other data in the base path.
func ensureStrictSubpath(subPath string) bool {
	return filepath.IsLocal(subPath) && filepath.Clean(subPath) != "."
}

// Convert context parameters to a lustreVolume
func newLustreVolume(volumeID, volumeName string, params map[string]string) (*lustreVolume, error) {
	var mgsIPAddress, subDir string

	// validate parameters (case-insensitive).
	for k, v := range params {
		switch strings.ToLower(k) {
		case VolumeContextMGSIPAddress:
			mgsIPAddress = v
		case VolumeContextSubDir:
			subDir = v
			subDir = strings.Trim(subDir, "/")

			if len(subDir) == 0 {
				return nil, status.Error(
					codes.InvalidArgument,
					"Context sub-dir must not be empty or root if provided",
				)
			}
		}
	}

	if len(mgsIPAddress) == 0 {
		return nil, status.Error(
			codes.InvalidArgument,
			"Context mgs-ip-address must be provided",
		)
	}

	vol := &lustreVolume{
		name:          volumeName,
		mgsIPAddress:  mgsIPAddress,
		hpeLustreName: volumeID, // DefaultLustreFsName,
		subDir:        subDir,
		id:            volumeID,
	}

	return vol, nil
}
