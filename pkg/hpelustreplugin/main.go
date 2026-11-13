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

package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/HewlettPackard/lustre-csi-driver/pkg/hpelustre"
	"k8s.io/klog/v2"
)

var NnfDriverName = "lustre-csi.hpe.com"

var (
	swapSrc       = ""
	swapDst       = ""
	swapDstFSType = ""
)

var (
	endpoint                 = flag.String("endpoint", "unix://tmp/csi.sock", "CSI endpoint")
	nodeID                   = flag.String("nodeid", "", "node id")
	version                  = flag.Bool("version", false, "Print the version and exit.")
	driverName               = flag.String("drivername", NnfDriverName, "name of the driver")
	enableHpeLustreMockMount = flag.Bool("enable-hpelustre-mock-mount", false, "Whether enable mock mount(only for testing)")
	workingMountDir          = flag.String("working-mount-dir", "/tmp", "working directory for provisioner to mount lustre filesystems temporarily")
	swapSourceFrom           = flag.String("swap-source-from", "", "source as specified in PV's spec.csi.volumeHandle to be swapped")
	swapSourceTo             = flag.String("swap-source-to", "", "source to be used in place of the PV's spec.csi.volumeHandle")
	swapSourceToFSType       = flag.String("swap-source-to-fstype", "", "fs type of the --swap-source-to volume")
	enableFailover           = flag.Bool("enable-failover", false, "allow PVs to have a failoverVolumeHandle; requires API access to PVs")
	failoverStateDir         = flag.String("failover-state-dir", "/var/lib/kubelet/plugins/lustre-csi.hpe.com/failover", "node-local directory recording which side each failover volume was mounted from")
	failoverForceUnmount     = flag.Bool("failover-force-unmount-stale", true, "force-unmount mounts of the side that a volume has failed away from")
	operationTimeout         = flag.Duration("operation-timeout", 90*time.Second, "how long NodePublishVolume and NodeUnpublishVolume wait for a mount or an unmount. One that takes longer, e.g. because its Lustre servers are unreachable, continues in the background, and calls on the same target path fail with Aborted until it completes")
	statfsTimeout            = flag.Duration("statfs-timeout", 30*time.Second, "how long a mounted volume may take to answer statfs before it is deemed unresponsive: NodeGetVolumeStats gives up on it, and NodeUnpublishVolume force-unmounts it")
	lnetPing                 = flag.Bool("lnet-ping", true, "ping MGS NIDs over LNet, through /dev/lnet, to refuse mounts from unreachable MGSs and to report the LNet health of failover PVs")
	lnetPingTimeout          = flag.Duration("lnet-ping-timeout", 5*time.Second, "how long an MGS NID may take to answer an LNet ping, at most 2m")
	checkMGSBeforeMount      = flag.Bool("check-mgs-before-mount", true, "refuse to mount from an MGS whose NIDs answer no LNet ping, rather than wait for the mount to fail")
	healthReportInterval     = flag.Duration("health-report-interval", 30*time.Second, "how often to report the LNet health of the MGSs of failover PVs in a per-node Lease; 0 to not report it. Requires --enable-failover and --lnet-ping")
	healthNamespace          = flag.String("health-namespace", "", "namespace of the per-node health Leases; the driver's own if empty")
)

func main() {
	klog.InitFlags(nil)
	err := flag.Set("logtostderr", "true")
	if err != nil {
		klog.Fatalln(err)
	}
	flag.Parse()
	if *version {
		info, err := hpelustre.GetVersionYAML(*driverName)
		if err != nil {
			klog.Fatalln(err)
		}
		klog.V(2).Info(info)
		fmt.Println(info) //nolint:forbidigo // Print version info to stdout for access through kubectl exec
		os.Exit(0)
	}

	if swapSourceFrom != nil && *swapSourceFrom != "" {
		swapSrc = *swapSourceFrom
	}
	if swapSourceTo != nil && *swapSourceTo != "" {
		swapDst = *swapSourceTo
	}
	if swapSourceToFSType != nil && *swapSourceToFSType != "" {
		swapDstFSType = *swapSourceToFSType
	}
	if swapSrc != "" && swapDst == "" || swapSrc == "" && swapDst != "" {
		klog.Fatalln("Both --swap-source-from and --swap-source-to must be specified together")
	}
	if swapDst != "" && swapDstFSType == "" {
		klog.Fatalln("--swap-source-to-fstype must be specified when --swap-source-to is used")
	}
	if swapSrc != "" && swapDst != "" {
		klog.Infof("Swapping source '%s' to '%s' (%s) in volume handles", swapSrc, swapDst, swapDstFSType)
	}
	if *lnetPingTimeout < time.Second || *lnetPingTimeout > 2*time.Minute {
		klog.Fatalln("--lnet-ping-timeout must be between 1s and 2m")
	}
	if *operationTimeout <= 0 || *statfsTimeout <= 0 || *healthReportInterval < 0 {
		klog.Fatalln("--operation-timeout and --statfs-timeout must be positive, and --health-report-interval not negative")
	}

	handle()
	os.Exit(0)
}

func handle() {
	driverOptions := hpelustre.DriverOptions{
		NodeID:                    *nodeID,
		DriverName:                *driverName,
		EnableHpeLustreMockMount:  *enableHpeLustreMockMount,
		WorkingMountDir:           *workingMountDir,
		SwapSourceFrom:            swapSrc,
		SwapSourceTo:              swapDst,
		SwapSourceToFSType:        swapDstFSType,
		EnableFailover:            *enableFailover,
		FailoverStateDir:          *failoverStateDir,
		FailoverForceUnmountStale: *failoverForceUnmount,
		OperationTimeout:          *operationTimeout,
		StatfsTimeout:             *statfsTimeout,
		LNetPing:                  *lnetPing,
		LNetPingTimeout:           *lnetPingTimeout,
		CheckMGSBeforeMount:       *checkMGSBeforeMount,
		HealthReportInterval:      *healthReportInterval,
		HealthNamespace:           *healthNamespace,
	}
	driver := hpelustre.NewDriver(&driverOptions)
	if driver == nil {
		klog.Fatalln("Failed to initialize HPE Lustre CSI driver")
	}
	driver.Run(*endpoint, false)
}
