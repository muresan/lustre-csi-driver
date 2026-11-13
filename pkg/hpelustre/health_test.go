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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
)

const (
	testMGS        = "10.0.0.1@o2ib,10.0.0.2@o2ib:10.0.0.3@o2ib"
	testFailoverMG = "10.1.0.1@o2ib:10.1.0.3@o2ib"
)

// fakePinger answers pings from a table. NIDs not in it are unreachable.
type fakePinger struct {
	mu      sync.Mutex
	results map[string]sideState
	calls   int
	block   chan struct{} // if set, pings wait for it to be closed
}

func (p *fakePinger) ping(ctx context.Context, nid string) pingResult {
	p.mu.Lock()
	p.calls++
	state, ok := p.results[nid]
	block := p.block
	p.mu.Unlock()

	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return pingResult{SideUnknown, nid + ": cancelled"}
		}
	}
	if !ok {
		return pingResult{SideUnreachable, nid + ": input/output error"}
	}
	return pingResult{state, nid + ": " + string(state)}
}

func (p *fakePinger) set(nid string, state sideState) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.results == nil {
		p.results = map[string]sideState{}
	}
	p.results[nid] = state
}

func (p *fakePinger) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

func TestPingMGS(t *testing.T) {
	p := &fakePinger{}
	h := newHealthChecker(p.ping, time.Second)

	r := h.pingMGS(testMGS)
	assert.Equal(t, SideUnreachable, r.state)
	for _, nid := range []string{"10.0.0.1@o2ib", "10.0.0.2@o2ib", "10.0.0.3@o2ib"} {
		assert.Contains(t, r.detail, nid)
	}

	// Any NID of any MGS node answering will do.
	p.set("10.0.0.3@o2ib", SideReachable)
	assert.Equal(t, SideReachable, h.pingMGS(testMGS).state)

	// Unreachable only if all NIDs surely are.
	p.set("10.0.0.3@o2ib", SideUnknown)
	assert.Equal(t, SideUnknown, h.pingMGS(testMGS).state)

	assert.Equal(t, SideUnknown, h.pingMGS("").state)
}

func TestHealthCheckCaches(t *testing.T) {
	p := &fakePinger{}
	h := newHealthChecker(p.ping, time.Second)

	assert.Equal(t, SideUnreachable, h.check(testMGS, time.Minute).State)
	assert.Equal(t, 3, p.callCount())
	assert.Equal(t, SideUnreachable, h.check(testMGS, time.Minute).State)
	assert.Equal(t, 3, p.callCount())
	h.check(testMGS, 0)
	assert.Equal(t, 6, p.callCount())
}

func TestHealthCheckSharesPing(t *testing.T) {
	p := &fakePinger{block: make(chan struct{})}
	h := newHealthChecker(p.ping, 10*time.Second)

	var wg sync.WaitGroup
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.Equal(t, SideUnreachable, h.check(testMGS, time.Minute).State)
		}()
	}
	require.Eventually(t, func() bool { return p.callCount() == 3 }, 5*time.Second, time.Millisecond)
	close(p.block)
	wg.Wait()
	assert.Equal(t, 3, p.callCount())
}

func TestHealthSince(t *testing.T) {
	p := &fakePinger{}
	h := newHealthChecker(p.ping, time.Second)

	first := h.check(testMGS, 0)
	assert.Equal(t, SideUnreachable, first.State)
	again := h.check(testMGS, 0)
	assert.Equal(t, first.Since, again.Since)
	assert.True(t, again.Checked.After(first.Checked) || again.Checked.Equal(first.Checked))

	p.set("10.0.0.1@o2ib", SideReachable)
	back := h.check(testMGS, 0)
	assert.Equal(t, SideReachable, back.State)
	assert.True(t, back.Since.After(first.Since))
}

func TestHealthCached(t *testing.T) {
	p := &fakePinger{}
	h := newHealthChecker(p.ping, time.Second)

	assert.Equal(t, SideUnknown, h.cached(testMGS, time.Minute).State)
	h.check(testMGS, time.Minute)
	assert.Equal(t, SideUnreachable, h.cached(testMGS, time.Minute).State)
	// /proc/self/mountinfo may spell the MGS differently.
	assert.Equal(t, SideUnreachable, h.cached("10.0.0.3@o2ib", time.Minute).State)
	assert.Equal(t, SideUnknown, h.cached("10.9.9.9@o2ib", time.Minute).State)
	time.Sleep(time.Millisecond)
	assert.Equal(t, SideUnknown, h.cached(testMGS, time.Nanosecond).State)
	assert.Equal(t, 3, p.callCount())
}

func TestHealthReporter(t *testing.T) {
	p := &fakePinger{}
	p.set("10.1.0.1@o2ib", SideReachable)
	var events []string
	var published [][]sideHealth
	r := &healthReporter{
		checker: newHealthChecker(p.ping, time.Second),
		sides: func() map[string][]string {
			return map[string][]string{testMGS: {"pv1", "pv2"}, testFailoverMG: nil}
		},
		publish: func(_ context.Context, report []sideHealth) error {
			published = append(published, report)
			return nil
		},
		event: func(eventType, reason, message string) {
			events = append(events, strings.Join([]string{eventType, reason, message}, " "))
		},
	}

	r.report(context.Background())
	require.Len(t, events, 1)
	assert.Contains(t, events[0], corev1.EventTypeWarning+" "+healthEventUnreachable+" MGS "+testMGS)
	assert.Contains(t, events[0], "pv1, pv2")
	require.Len(t, published, 1)
	require.Len(t, published[0], 2)
	assert.Equal(t, testMGS, published[0][0].MGS)
	assert.Equal(t, SideUnreachable, published[0][0].State)
	assert.Equal(t, testFailoverMG, published[0][1].MGS)
	assert.Equal(t, SideReachable, published[0][1].State)

	// No news, no events.
	r.report(context.Background())
	assert.Len(t, events, 1)

	p.set("10.0.0.2@o2ib", SideReachable)
	r.report(context.Background())
	require.Len(t, events, 2)
	assert.Contains(t, events[1], corev1.EventTypeNormal+" "+healthEventReachable+" MGS "+testMGS)
	assert.Len(t, published, 3)
}

func TestHealthLeaseName(t *testing.T) {
	assert.Equal(t, "lustre-csi-health-node1", healthLeaseName("node1"))
	long := healthLeaseName(strings.Repeat("n", 250))
	assert.LessOrEqual(t, len(long), 253)
	assert.True(t, strings.HasPrefix(long, HealthLeaseNamePrefix))
}

func TestDescribeActivePVs(t *testing.T) {
	assert.Contains(t, describeActivePVs(nil), "no PV")
	assert.Contains(t, describeActivePVs([]string{"a", "b"}), "a, b")
	many := describeActivePVs([]string{"a", "b", "c", "d", "e", "f", "g"})
	assert.Contains(t, many, "a, b, c, d, e and 2 more")
}
