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
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// The expected values were printed by libcfs_str2nid() and by a C program
// built against the uapi headers of lustre-client 2.15.7.1_cray_20.

func TestParseNID(t *testing.T) {
	for nid, want := range map[string]uint64{
		"10.0.0.1@o2ib":      0x500000a000001,
		"10.0.0.1@o2ib1":     0x500010a000001,
		"10.0.0.1@o2ib65535": 0x5ffff0a000001,
		"192.168.1.2@tcp":    0x20000c0a80102,
		"192.168.1.2@tcp7":   0x20007c0a80102,
		"10.0.0.1":           0x200000a000001,
		"01.2.3.4@tcp":       0x2000001020304,
		"0x1f@kfi":           0x1000000000001f,
		"0X1F@kfi":           0x1000000000001f,
		"31@kfi":             0x1000000000001f,
		"27@kfi2":            0x1000020000001b,
		"4294967295@kfi":     0x100000ffffffff,
		"12@gni":             0xd00000000000c,
		"10.1.2.3@gip":       0xe00000a010203,
		"5@ptlf":             0xf000000000005,
	} {
		got, err := parseNID(context.Background(), nid)
		if assert.NoError(t, err, nid) {
			assert.Equal(t, want, got, "%s: got %#x, want %#x", nid, got, want)
		}
	}

	for _, nid := range []string{
		"10.0.0.256@o2ib", "1.2.3@tcp", "1.2.3.4.5@tcp", "10.0.0.1@o2ibx",
		"10.0.0.1@foo", "@tcp", "x1f@kfi", "4294967296@kfi", "10.0.0.1@o2ib65536",
		"0@lo",
	} {
		_, err := parseNID(context.Background(), nid)
		assert.Error(t, err, nid)
	}
}

func TestParseNIDHostname(t *testing.T) {
	ips, err := net.DefaultResolver.LookupIP(context.Background(), "ip4", "localhost")
	if err != nil || len(ips) == 0 || !ips[0].Equal(net.IPv4(127, 0, 0, 1)) {
		t.Skip("localhost doesn't resolve to 127.0.0.1 here")
	}
	nid, err := parseNID(context.Background(), "localhost@tcp")
	require.NoError(t, err)
	assert.Equal(t, uint64(0x200007f000001), nid)
}

func TestLNetPingABI(t *testing.T) {
	var data libcfsIoctlData
	assert.Equal(t, uintptr(128), unsafe.Sizeof(data))
	assert.Equal(t, uintptr(8), unsafe.Offsetof(data.NID))
	assert.Equal(t, uintptr(24), unsafe.Offsetof(data.Flags))
	assert.Equal(t, uintptr(28), unsafe.Offsetof(data.Count))
	assert.Equal(t, uintptr(36), unsafe.Offsetof(data.U32))
	assert.Equal(t, uintptr(64), unsafe.Offsetof(data.Inllen1))
	assert.Equal(t, uintptr(72), unsafe.Offsetof(data.Inlbuf1))
	assert.Equal(t, uintptr(96), unsafe.Offsetof(data.Plen1))
	assert.Equal(t, uintptr(104), unsafe.Offsetof(data.Pbuf1))
	assert.Equal(t, uintptr(112), unsafe.Offsetof(data.Plen2))
	assert.Equal(t, uintptr(120), unsafe.Offsetof(data.Pbuf2))
	assert.Equal(t, uintptr(16), unsafe.Sizeof(lnetProcessID{}))
	assert.Equal(t, uint32(0xc008653d), uint32(iocLibcfsPing))
	assert.Equal(t, uint32(0x1000a), uint32(libcfsIoctlVersion))
}

func TestClassifyPing(t *testing.T) {
	assert.Equal(t, SideReachable, classifyPing("n", nil).state)
	for _, err := range []error{unix.EIO, unix.ETIMEDOUT, unix.EHOSTUNREACH, unix.ENETUNREACH} {
		assert.Equal(t, SideUnreachable, classifyPing("n", err).state, err.Error())
	}
	for _, err := range []error{
		unix.ENETDOWN, unix.ESHUTDOWN, unix.EINVAL, unix.ENOTTY, unix.EACCES, unix.EPROTO,
		&os.PathError{Op: "open", Path: lnetDevice, Err: unix.ENOENT},
	} {
		assert.Equal(t, SideUnknown, classifyPing("n", err).state, err.Error())
	}
}

func TestLNetPingWithoutLNet(t *testing.T) {
	dir := t.TempDir()
	module := filepath.Join(dir, "lnet")
	ping := lnetPing(filepath.Join(dir, "dev-lnet"), module, time.Second)

	r := ping(context.Background(), "10.0.0.1@o2ib")
	assert.Equal(t, SideUnknown, r.state)
	assert.Contains(t, r.detail, "not loaded")

	require.NoError(t, os.Mkdir(module, 0o755))
	r = ping(context.Background(), "10.0.0.1@o2ib")
	assert.Equal(t, SideUnknown, r.state)
	assert.Contains(t, r.detail, "not available")

	r = ping(context.Background(), "10.0.0.1@nonet")
	assert.Equal(t, SideUnknown, r.state)

	// A device that isn't /dev/lnet rejects the ioctl.
	notLNet := filepath.Join(dir, "file")
	require.NoError(t, os.WriteFile(notLNet, nil, 0o600))
	r = lnetPing(notLNet, module, time.Second)(context.Background(), "10.0.0.1@o2ib")
	assert.Equal(t, SideUnknown, r.state)
	assert.Contains(t, r.detail, "could not ping")
}
