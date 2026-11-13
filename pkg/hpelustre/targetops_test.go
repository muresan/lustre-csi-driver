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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func noop() (any, error) { return nil, nil }

func TestTargetOpsReturnsResult(t *testing.T) {
	ops := newTargetOps()
	val, err := ops.do(context.Background(), "k", "op", time.Second, false,
		func() (any, error) { return 42, nil })
	require.NoError(t, err)
	assert.Equal(t, 42, val)

	_, err = ops.do(context.Background(), "k", "op", time.Second, false,
		func() (any, error) { return nil, status.Error(codes.Internal, "boom") })
	requireCode(t, err, codes.Internal)
}

func TestTargetOpsTimeoutKeepsKeyBusy(t *testing.T) {
	ops := newTargetOps()
	release := make(chan struct{})
	_, err := ops.do(context.Background(), "k", "NodePublishVolume", 50*time.Millisecond, false,
		func() (any, error) { <-release; return nil, nil })
	requireCode(t, err, codes.DeadlineExceeded)

	// The operation runs on: others on its key fail at once...
	start := time.Now()
	_, err = ops.do(context.Background(), "k", "NodeUnpublishVolume", time.Minute, false,
		func() (any, error) { t.Error("ran a second operation on a busy key"); return nil, nil })
	requireCode(t, err, codes.Aborted)
	assert.Less(t, time.Since(start), time.Second)

	// ...while other keys are not held up.
	_, err = ops.do(context.Background(), "other", "NodePublishVolume", time.Second, false, noop)
	require.NoError(t, err)

	// The key is free again once the operation completes.
	close(release)
	require.Eventually(t, func() bool {
		_, err := ops.do(context.Background(), "k", "NodePublishVolume", time.Second, false, noop)
		return err == nil
	}, 5*time.Second, 10*time.Millisecond)
}

func TestTargetOpsSharedWaitsForRunningOp(t *testing.T) {
	ops := newTargetOps()
	release := make(chan struct{})
	first := make(chan error, 1)
	go func() {
		_, err := ops.do(context.Background(), "k", "statfs", time.Minute, true,
			func() (any, error) { <-release; return "first", nil })
		first <- err
	}()
	require.Eventually(t, func() bool {
		ops.mu.Lock()
		defer ops.mu.Unlock()
		return ops.ops["k"] != nil
	}, 5*time.Second, time.Millisecond)

	second := make(chan any, 1)
	go func() {
		val, err := ops.do(context.Background(), "k", "statfs", time.Minute, true,
			func() (any, error) { return "second", nil })
		assert.NoError(t, err)
		second <- val
	}()
	// Let the second call join the first operation before it completes.
	time.Sleep(100 * time.Millisecond)
	close(release)
	assert.Equal(t, "first", <-second)
	require.NoError(t, <-first)
}

func TestTargetOpsSharedDoesNotWaitAgain(t *testing.T) {
	ops := newTargetOps()
	release := make(chan struct{})
	defer close(release)
	timeout := 200 * time.Millisecond
	_, err := ops.do(context.Background(), "k", "statfs", timeout, true,
		func() (any, error) { <-release; return nil, nil })
	requireCode(t, err, codes.DeadlineExceeded)

	// The statfs in progress has already run for the timeout.
	start := time.Now()
	_, err = ops.do(context.Background(), "k", "statfs", timeout, true, noop)
	requireCode(t, err, codes.DeadlineExceeded)
	assert.Less(t, time.Since(start), timeout/2)
}

func TestTargetOpsContextDone(t *testing.T) {
	ops := newTargetOps()
	release := make(chan struct{})
	defer close(release)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := ops.do(ctx, "k", "NodePublishVolume", time.Minute, false,
		func() (any, error) { <-release; return nil, nil })
	requireCode(t, err, codes.DeadlineExceeded)
}
