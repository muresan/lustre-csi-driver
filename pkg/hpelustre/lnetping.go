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
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// LNet ping, as "lctl ping" does it: with the IOC_LIBCFS_PING ioctl on
// /dev/lnet, so that the driver needs none of Lustre's user-space tools. The
// ABI is that of Lustre 2.15 on 64-bit Linux, from
// lnet/include/uapi/linux/lnet/{libcfs_ioctl,lnet-types,lnet-idl,nidstr}.h.
// The kernel side is LNetCtl() and lnet_ping() in lnet/lnet/api-ni.c.
//
// An ABI mismatch makes the ioctl fail with an error such as EINVAL or
// ENOTTY, which leaves the health of the MGS unknown rather than unreachable,
// so mounts are not refused because of it.

const (
	lnetDevice = "/dev/lnet"

	// LIBCFS_IOCTL_VERSION
	libcfsIoctlVersion = 0x0001000a
	// IOC_LIBCFS_PING, _IOWR('e', 61, long)
	iocLibcfsPing = 3<<30 | 8<<16 | 'e'<<8 | 61
	// LNET_PID_ANY, which the kernel turns into LNET_PID_LUSTRE.
	lnetPIDAny = 0xffffffff
	// lctl's limit on ping timeouts.
	maxLNetPingTimeout = 120 * time.Second
	// How many NIs of the pinged node to make room for; lctl's number.
	lnetPingMaxIDs = 16
	// How long a ping may take beyond its timeout, for the kernel to clean up.
	pingGrace = 2 * time.Second
)

// libcfsIoctlData is struct libcfs_ioctl_data, without inline buffers.
type libcfsIoctlData struct {
	Len     uint32 // ioc_hdr.ioc_len
	Version uint32 // ioc_hdr.ioc_version
	NID     uint64
	U64     [1]uint64
	Flags   uint32
	Count   uint32
	Net     uint32
	U32     [7]uint32
	Inllen1 uint32
	_       uint32
	Inlbuf1 uint64
	Inllen2 uint32
	_       uint32
	Inlbuf2 uint64
	Plen1   uint32
	_       uint32
	Pbuf1   uint64
	Plen2   uint32
	_       uint32
	Pbuf2   uint64
}

// lnetProcessID is struct lnet_process_id.
type lnetProcessID struct {
	NID uint64
	PID uint32
	_   uint32
}

// lnd is a type of LNet network, from enum in nidstr.h.
type lnd struct {
	name string
	typ  uint32
	// Whether addresses are IPv4 addresses, rather than numbers.
	ip bool
}

// The LNDs that "lctl ping" knows, in the order it matches their names. LOLND
// is left out: there's no point pinging oneself.
var lnds = []lnd{
	{name: "tcp", typ: 2, ip: true},
	{name: "o2ib", typ: 5, ip: true},
	{name: "gni", typ: 13},
	{name: "gip", typ: 14, ip: true},
	{name: "ptlf", typ: 15},
	{name: "kfi", typ: 16},
}

// parseNID encodes a NID such as 10.0.0.1@o2ib1 or 0x1f@kfi into an
// lnet_nid_t, as libcfs_str2nid() does. A NID without a network is on tcp.
// Host names are resolved like IPv4 addresses.
func parseNID(ctx context.Context, s string) (uint64, error) {
	addrStr, netStr, found := strings.Cut(strings.TrimSpace(s), "@")
	if !found {
		netStr = "tcp"
	}

	var l *lnd
	for i := range lnds {
		if strings.HasPrefix(netStr, lnds[i].name) {
			l = &lnds[i]
			break
		}
	}
	if l == nil {
		return 0, fmt.Errorf("unknown LNet network %q", netStr)
	}
	num := uint64(0)
	if rest := netStr[len(l.name):]; rest != "" {
		var err error
		num, err = strconv.ParseUint(rest, 10, 16)
		if err != nil {
			return 0, fmt.Errorf("bad LNet network number in %q", netStr)
		}
	}

	var addr uint64
	var err error
	if l.ip {
		addr, err = parseIPv4Addr(ctx, addrStr)
	} else {
		addr, err = parseNumAddr(addrStr)
	}
	if err != nil {
		return 0, err
	}

	net := uint64(l.typ)<<16 | num
	return net<<32 | addr, nil
}

// parseIPv4Addr parses an IPv4 address, or a host name that resolves to one,
// into a host-order number, as libcfs_ip_str2addr() does.
func parseIPv4Addr(ctx context.Context, s string) (uint64, error) {
	if parts := strings.Split(s, "."); len(parts) == 4 {
		addr := uint64(0)
		for _, part := range parts {
			b, err := strconv.ParseUint(part, 10, 8)
			if err != nil {
				return 0, fmt.Errorf("bad IPv4 address %q", s)
			}
			addr = addr<<8 | b
		}
		return addr, nil
	}
	if s == "" || !(('a' <= s[0] && s[0] <= 'z') || ('A' <= s[0] && s[0] <= 'Z')) {
		return 0, fmt.Errorf("bad IPv4 address %q", s)
	}
	ips, err := net.DefaultResolver.LookupIP(ctx, "ip4", s)
	if err != nil {
		return 0, err
	}
	if len(ips) == 0 {
		return 0, fmt.Errorf("%s has no IPv4 address", s)
	}
	ip := ips[0].To4()
	return uint64(ip[0])<<24 | uint64(ip[1])<<16 | uint64(ip[2])<<8 | uint64(ip[3]), nil
}

// parseNumAddr parses a decimal or 0x-prefixed hexadecimal address, as
// libcfs_num_str2addr() does.
func parseNumAddr(s string) (uint64, error) {
	var addr uint64
	var err error
	if hex, ok := strings.CutPrefix(strings.ToLower(s), "0x"); ok {
		addr, err = strconv.ParseUint(hex, 16, 32)
	} else {
		addr, err = strconv.ParseUint(s, 10, 32)
	}
	if err != nil {
		return 0, fmt.Errorf("bad LNet address %q", s)
	}
	return addr, nil
}

// lnetPing returns a pingFunc that pings NIDs through device, waiting up to
// timeout for each to answer. It doesn't touch device unless lnetModule, the
// LNet kernel module's sysfs directory, exists.
func lnetPing(device, lnetModule string, timeout time.Duration) pingFunc {
	timeoutMs := uint32(max(time.Second, min(timeout, maxLNetPingTimeout)).Milliseconds())
	return func(ctx context.Context, nidStr string) pingResult {
		if _, err := os.Stat(lnetModule); err != nil {
			return pingResult{SideUnknown, "LNet is not loaded on this node"}
		}
		nid, err := parseNID(ctx, nidStr)
		if err != nil {
			return pingResult{SideUnknown, fmt.Sprintf("%s: %v", nidStr, err)}
		}

		// The ioctl can't be interrupted, but returns within the timeout.
		done := make(chan error, 1)
		go func() { done <- pingNID(device, nid, timeoutMs) }()
		select {
		case err := <-done:
			return classifyPing(nidStr, err)
		case <-ctx.Done():
			return pingResult{SideUnknown, fmt.Sprintf("%s: LNet ping did not complete", nidStr)}
		}
	}
}

// pingNID pings a NID with the IOC_LIBCFS_PING ioctl.
func pingNID(device string, nid uint64, timeoutMs uint32) error {
	fd, err := unix.Open(device, unix.O_RDWR|unix.O_CLOEXEC, 0)
	if err != nil {
		return &os.PathError{Op: "open", Path: device, Err: err}
	}
	defer unix.Close(fd)

	data := &libcfsIoctlData{}
	ids := make([]lnetProcessID, lnetPingMaxIDs)
	// The kernel writes the pinged node's NIs to ids through data.Pbuf1.
	var pinner runtime.Pinner
	pinner.Pin(data)
	pinner.Pin(&ids[0])
	defer pinner.Unpin()

	data.Len = uint32(unsafe.Sizeof(*data))
	data.Version = libcfsIoctlVersion
	data.NID = nid
	data.U32[0] = lnetPIDAny
	data.U32[1] = timeoutMs
	data.Plen1 = uint32(uintptr(len(ids)) * unsafe.Sizeof(ids[0]))
	data.Pbuf1 = uint64(uintptr(unsafe.Pointer(&ids[0])))

	_, _, errno := unix.Syscall(unix.SYS_IOCTL, uintptr(fd), iocLibcfsPing, uintptr(unsafe.Pointer(data)))
	if errno != 0 {
		return errno
	}
	return nil
}

// classifyPing tells whether the error of a ping means that the NID is
// unreachable, or only that it could not be pinged.
func classifyPing(nid string, err error) pingResult {
	var pathErr *os.PathError
	switch {
	case err == nil:
		return pingResult{SideReachable, ""}
	case errors.As(err, &pathErr):
		return pingResult{SideUnknown, fmt.Sprintf("%s: LNet is not available on this node: %v", nid, err)}
	case errors.Is(err, unix.EIO), errors.Is(err, unix.ETIMEDOUT),
		errors.Is(err, unix.EHOSTUNREACH), errors.Is(err, unix.ENETUNREACH),
		errors.Is(err, unix.EHOSTDOWN), errors.Is(err, unix.ECONNREFUSED),
		errors.Is(err, unix.ECONNRESET), errors.Is(err, unix.ECONNABORTED):
		// No answer: lnet_ping() fails with EIO, or with the error of
		// sending the ping.
		return pingResult{SideUnreachable, fmt.Sprintf("%s: %v", nid, err)}
	case errors.Is(err, unix.ENETDOWN), errors.Is(err, unix.ESHUTDOWN):
		return pingResult{SideUnknown, fmt.Sprintf("%s: LNet is not up on this node: %v", nid, err)}
	default:
		return pingResult{SideUnknown, fmt.Sprintf("%s: could not ping: %v", nid, err)}
	}
}
