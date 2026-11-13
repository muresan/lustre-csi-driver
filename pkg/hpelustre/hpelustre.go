/*
Copyright 2019 The Kubernetes Authors.

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
	"cmp"
	"fmt"
	"strings"
	"sync"
	"time"

	csicommon "github.com/HewlettPackard/lustre-csi-driver/pkg/csi-common"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"golang.org/x/sys/unix"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	mount "k8s.io/mount-utils"
	utilexec "k8s.io/utils/exec"
)

const (
	DefaultLustreFsName = "lustrefs"
	separator           = "#"

	podNameKey            = "csi.storage.k8s.io/pod.name"
	podNamespaceKey       = "csi.storage.k8s.io/pod.namespace"
	podUIDKey             = "csi.storage.k8s.io/pod.uid"
	serviceAccountNameKey = "csi.storage.k8s.io/serviceaccount.name"
	pvcNameKey            = "csi.storage.k8s.io/pvc/name"
	pvcNamespaceKey       = "csi.storage.k8s.io/pvc/namespace"
	pvNameKey             = "csi.storage.k8s.io/pv/name"

	podNameMetadata            = "${pod.metadata.name}"
	podNamespaceMetadata       = "${pod.metadata.namespace}"
	podUIDMetadata             = "${pod.metadata.uid}"
	serviceAccountNameMetadata = "${serviceAccount.metadata.name}"
	pvcNameMetadata            = "${pvc.metadata.name}"
	pvcNamespaceMetadata       = "${pvc.metadata.namespace}"
	pvNameMetadata             = "${pv.metadata.name}"
)

var (
	controllerServiceCapabilities = []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_SINGLE_NODE_MULTI_WRITER,
	}

	volumeCapabilities = []csi.VolumeCapability_AccessMode_Mode{
		csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY,
		csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER,
		csi.VolumeCapability_AccessMode_SINGLE_NODE_MULTI_WRITER,
		csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY,
		csi.VolumeCapability_AccessMode_MULTI_NODE_SINGLE_WRITER,
		csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
	}

	nodeServiceCapabilities = []csi.NodeServiceCapability_RPC_Type{
		csi.NodeServiceCapability_RPC_GET_VOLUME_STATS,
		csi.NodeServiceCapability_RPC_SINGLE_NODE_MULTI_WRITER,
	}
)

type lustreVolume struct {
	name          string
	id            string
	mgsIPAddress  string
	hpeLustreName string
	subDir        string
}

// DriverOptions defines driver parameters specified in driver deployment
type DriverOptions struct {
	NodeID                   string
	DriverName               string
	EnableHpeLustreMockMount bool
	WorkingMountDir          string

	// Used for testing. Allows the .spec.csi.volumeHandle to be swapped with
	// another value.
	SwapSourceFrom     string
	SwapSourceTo       string
	SwapSourceToFSType string

	// Whether PVs may have a failoverVolumeHandle.
	EnableFailover bool
	// Node-local directory recording which side each failover volume was
	// mounted from.
	FailoverStateDir string
	// Whether to force-unmount mounts of the side that a volume has failed
	// away from.
	FailoverForceUnmountStale bool

	// How long NodePublishVolume and NodeUnpublishVolume wait for a mount or
	// an unmount, which then continues in the background.
	OperationTimeout time.Duration
	// How long a mounted volume may take to answer statfs before it is
	// deemed unresponsive.
	StatfsTimeout time.Duration
	// Whether to ping MGSs over LNet.
	LNetPing        bool
	LNetPingTimeout time.Duration
	// Whether to refuse to mount from an MGS that doesn't answer LNet pings.
	CheckMGSBeforeMount bool
	// How often to report the LNet health of the MGSs of failover PVs. 0 to
	// not report it.
	HealthReportInterval time.Duration
	// Namespace of the health Leases. The driver's own if empty.
	HealthNamespace string
}

const (
	defaultOperationTimeout = 90 * time.Second
	defaultStatfsTimeout    = 30 * time.Second
	defaultLNetPingTimeout  = 5 * time.Second
)

// Driver implements all interfaces of CSI drivers
type Driver struct {
	csicommon.CSIDriver
	csicommon.DefaultIdentityServer
	csicommon.DefaultControllerServer
	csicommon.DefaultNodeServer
	// enableHpeLustreMockMount is only for testing, DO NOT set as true in non-testing scenario
	enableHpeLustreMockMount bool
	mounter                  *mount.SafeFormatAndMount
	forceMounter             *mount.MounterForceUnmounter
	// Directory to temporarily mount to for subdirectory creation
	workingMountDir string
	// Serializes mounts until the Lustre kernel modules are loaded.
	kernelModuleLock sync.Mutex

	// Used for testing. Allows the .spec.csi.volumeHandle to be swapped with
	// another value. The "type" indicates the type of the new volume
	// (e.g., "xfs", "ext4", etc.).
	swapSourceFrom     string
	swapSourceTo       string
	swapSourceToFSType string

	enableFailover            bool
	failoverStateDir          string
	failoverForceUnmountStale bool
	failover                  *failoverManager

	// Node operations in progress, by target path.
	ops *targetOps
	// statfs calls in progress, by volume path.
	statfsOps            *targetOps
	operationTimeout     time.Duration
	statfsTimeout        time.Duration
	lnetPing             bool
	lnetPingTimeout      time.Duration
	checkMGSBeforeMount  bool
	healthReportInterval time.Duration
	healthNamespace      string
	// LNet health of MGSs. Nil if MGSs are not pinged.
	health *healthChecker

	// Replaced in tests.
	statVolume          func(path string) (*csi.NodeGetVolumeStatsResponse, error)
	findMount           func(path string) (*mount.MountInfo, error)
	unmount             func(path string, flags int) error
	lustreModulesLoaded func() bool
}

// NewDriver Creates a NewCSIDriver object. Assumes vendor version is equal to driver version &
// does not support optional driver plugin info manifest field. Refer to CSI spec for more details.
func NewDriver(options *DriverOptions) *Driver {
	d := Driver{
		enableHpeLustreMockMount: options.EnableHpeLustreMockMount,
		workingMountDir:          options.WorkingMountDir,
	}
	d.Name = options.DriverName
	d.Version = driverVersion
	d.NodeID = options.NodeID

	d.DefaultControllerServer.Driver = &d.CSIDriver
	d.DefaultIdentityServer.Driver = &d.CSIDriver
	d.DefaultNodeServer.Driver = &d.CSIDriver

	d.swapSourceFrom = options.SwapSourceFrom
	d.swapSourceTo = options.SwapSourceTo
	d.swapSourceToFSType = options.SwapSourceToFSType

	d.enableFailover = options.EnableFailover
	d.failoverStateDir = options.FailoverStateDir
	d.failoverForceUnmountStale = options.FailoverForceUnmountStale

	d.ops = newTargetOps()
	d.statfsOps = newTargetOps()
	d.operationTimeout = cmp.Or(options.OperationTimeout, defaultOperationTimeout)
	d.statfsTimeout = cmp.Or(options.StatfsTimeout, defaultStatfsTimeout)
	d.lnetPing = options.LNetPing
	d.lnetPingTimeout = cmp.Or(options.LNetPingTimeout, defaultLNetPingTimeout)
	d.checkMGSBeforeMount = options.CheckMGSBeforeMount
	d.healthReportInterval = options.HealthReportInterval
	d.healthNamespace = options.HealthNamespace

	d.statVolume = statVolume
	d.findMount = findMount
	d.unmount = unix.Unmount
	d.lustreModulesLoaded = lustreModulesLoaded

	return &d
}

// Run driver initialization
func (d *Driver) Run(endpoint string, testBool bool) {
	versionMeta, err := GetVersionYAML(d.Name)
	if err != nil {
		klog.Fatalf("%v", err)
	}
	klog.Infof("\nDRIVER INFORMATION:\n-------------------\n%s\n\nStreaming logs below:", versionMeta)

	d.mounter = &mount.SafeFormatAndMount{
		Interface: mount.New(""),
		Exec:      utilexec.New(),
	}
	forceUnmounter, ok := d.mounter.Interface.(mount.MounterForceUnmounter)
	if ok {
		klog.V(4).Infof("Using force unmounter interface")
		d.forceMounter = &forceUnmounter
	} else {
		klog.Fatalf("Mounter does not support force unmount")
	}

	if d.lnetPing {
		d.health = newHealthChecker(lnetPing(lnetDevice, lnetModulePath, d.lnetPingTimeout), d.lnetPingTimeout)
		klog.Infof("Pinging MGSs over LNet, with a timeout of %v", d.lnetPingTimeout)
	}

	if d.enableFailover {
		config, err := rest.InClusterConfig()
		if err != nil {
			klog.Fatalf("Failover requires in-cluster API access: %v", err)
		}
		client, err := kubernetes.NewForConfig(config)
		if err != nil {
			klog.Fatalf("%v", err)
		}
		stopCh := make(chan struct{})
		d.failover, err = newFailoverManager(client, d.Name, d.failoverStateDir,
			d.failoverForceUnmountStale, stopCh)
		if err != nil {
			klog.Fatalf("Could not start failover support: %v", err)
		}
		klog.Infof("Failover enabled")
		if d.health != nil && d.healthReportInterval > 0 {
			d.startHealthReporter(client, stopCh)
		}
	}

	// TODO_JUSJIN: revisit these caps
	// Initialize default library driver
	// TODO_CHYIN: move this to {service}.go
	d.AddControllerServiceCapabilities(controllerServiceCapabilities)
	d.AddVolumeCapabilityAccessModes(volumeCapabilities)
	d.AddNodeServiceCapabilities(nodeServiceCapabilities)

	s := csicommon.NewNonBlockingGRPCServer()
	// Driver d act as IdentityServer, ControllerServer and NodeServer
	s.Start(endpoint, d, d, d, testBool)
	s.Wait()
}

// startHealthReporter reports the LNet health of the MGSs of failover PVs in
// this node's health Lease, once PVs are synced.
func (d *Driver) startHealthReporter(client kubernetes.Interface, stopCh <-chan struct{}) {
	namespace, err := healthNamespace(d.healthNamespace)
	if err != nil {
		klog.Warningf("Not reporting LNet health: %v", err)
		return
	}
	reporter := &healthReporter{
		checker:  d.health,
		interval: d.healthReportInterval,
		sides:    d.failover.sides,
		publish:  leasePublisher(client, namespace, d.NodeID, 3*d.healthReportInterval),
		event:    nodeEventRecorder(client, d.Name, d.NodeID),
	}
	go func() {
		if cache.WaitForCacheSync(stopCh, d.failover.synced) {
			reporter.run(stopCh)
		}
	}()
	klog.Infof("Reporting LNet health every %v in Lease %s/%s",
		d.healthReportInterval, namespace, healthLeaseName(d.NodeID))
}

func IsCorruptedDir(dir string) bool {
	_, pathErr := mount.PathExists(dir)
	return pathErr != nil && mount.IsCorruptedMnt(pathErr)
}

func getLustreVolFromID(id string) (*lustreVolume, error) {
	segments := strings.Split(id, separator)
	if len(segments) < 3 {
		return nil, fmt.Errorf("could not split volume ID %q into lustre name and ip address", id)
	}

	name := segments[0]
	vol := &lustreVolume{
		name:          name,
		id:            id,
		hpeLustreName: DefaultLustreFsName,
		mgsIPAddress:  segments[2],
	}

	if len(segments) >= 4 {
		vol.subDir = strings.Trim(segments[3], "/")
	}

	return vol, nil
}
