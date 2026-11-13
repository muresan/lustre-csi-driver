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
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
)

// targetOps runs node operations with a bounded wait, at most one at a time
// per key, which is a target path.
//
// A system call on a Lustre filesystem whose servers are unreachable can
// block for minutes, or until the servers return, in the kernel where it
// can't be cancelled. A CSI call gives up waiting on such an operation after
// a timeout, and leaves it to complete in the background. Its key stays busy
// until it does, so that kubelet's retries don't pile up more blocked
// operations, while operations on other target paths are not held up at all.
type targetOps struct {
	mu  sync.Mutex
	ops map[string]*targetOp
}

type targetOp struct {
	name    string
	started time.Time
	done    chan struct{} // closed once val and err are set
	val     any
	err     error
}

func newTargetOps() *targetOps {
	return &targetOps{ops: map[string]*targetOp{}}
}

// do runs op under key, and waits for it until it has run for timeout or ctx
// is done. If an operation is already running under key, do fails with
// Aborted, unless shared is set: then it waits for the result of that
// operation instead, but no longer than until it has run for timeout. Shared
// operations under a key must be idempotent and give the same kind of result.
func (t *targetOps) do(
	ctx context.Context,
	key, name string,
	timeout time.Duration,
	shared bool,
	op func() (any, error),
) (any, error) {
	t.mu.Lock()
	o, busy := t.ops[key]
	if busy && !shared {
		t.mu.Unlock()
		return nil, status.Errorf(codes.Aborted, "%s of %s is still in progress after %v",
			o.name, key, roundDuration(time.Since(o.started)))
	}
	if !busy {
		o = &targetOp{name: name, started: time.Now(), done: make(chan struct{})}
		t.ops[key] = o
		go func() {
			val, err := op()
			if elapsed := time.Since(o.started); elapsed > timeout {
				klog.Infof("%s of %s completed in the background after %v: %v",
					name, key, roundDuration(elapsed), err)
			}
			t.mu.Lock()
			delete(t.ops, key)
			t.mu.Unlock()
			o.val, o.err = val, err
			close(o.done)
		}()
	}
	t.mu.Unlock()

	timer := time.NewTimer(time.Until(o.started.Add(timeout)))
	defer timer.Stop()
	select {
	case <-o.done:
		return o.val, o.err
	case <-timer.C:
	case <-ctx.Done():
	}
	select {
	case <-o.done:
		return o.val, o.err
	default:
	}

	elapsed := roundDuration(time.Since(o.started))
	klog.Warningf("%s of %s has not completed after %v; leaving it to complete in the background",
		o.name, key, elapsed)
	return nil, status.Errorf(codes.DeadlineExceeded,
		"%s of %s has not completed after %v, and continues in the background; its Lustre servers may be unreachable",
		o.name, key, elapsed)
}

func roundDuration(d time.Duration) time.Duration {
	if d < 10*time.Second {
		return d.Round(100 * time.Millisecond)
	}
	return d.Round(time.Second)
}
