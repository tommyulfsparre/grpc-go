/*
 *
 * Copyright 2026 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package transport

import (
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/internal/envconfig"
	"google.golang.org/grpc/internal/testutils"
)

func (s) TestBDPEstimator_ConcurrentAccess(*testing.T) {
	b := &bdpEstimator{
		bdp:               initialWindowSize,
		updateFlowControl: func(uint32) {},
	}

	const iterations = 1000
	var wg sync.WaitGroup
	wg.Go(func() {
		for i := 0; i < iterations; i++ {
			b.timesnap(bdpPing.data)
		}
	})
	wg.Go(func() {
		for i := 0; i < iterations; i++ {
			b.calculate(bdpPing.data)
		}
	})
	wg.Go(func() {
		for i := 0; i < iterations; i++ {
			b.add(100)
		}
	})
	wg.Wait()
}

// ackBDPPing acks the outstanding bdp ping as if it had been sent one
// millisecond ago. The rtt samples then stay close to one millisecond. So
// bandwidth samples differ mostly by size and a much larger sample sets a new
// bwMax.
func ackBDPPing(b *bdpEstimator) {
	b.mu.Lock()
	b.sentAt = time.Now().Add(-time.Millisecond)
	b.mu.Unlock()
	b.calculate(bdpPing.data)
}

// addUntilPing adds frames of n bytes until add() starts a ping. It returns
// the number of frames before that one.
func addUntilPing(t *testing.T, b *bdpEstimator, n uint32) int {
	t.Helper()
	maxFrames := 2 * maxPingBackoff * (bdpLimit / 2) / int(n)
	for i := range maxFrames {
		if b.add(n) {
			return i
		}
	}
	t.Fatalf("add() did not start a ping within %d frames of %d bytes", maxFrames, n)
	return 0
}

// TestBDPEstimator_SmallSamplesBackOff checks that the gap between pings grows
// with every sample that does not grow the estimate, up to maxPingBackoff.
func (s) TestBDPEstimator_SmallSamplesBackOff(t *testing.T) {
	testutils.SetEnvConfig(t, &envconfig.EnableBDPPingBackoff, true)
	var updates int
	b := &bdpEstimator{
		bdp:               initialWindowSize,
		updateFlowControl: func(uint32) { updates++ },
	}

	const frameSize = 100
	const frames = 200_000
	var pingAt []int
	for i := range frames {
		if b.add(frameSize) {
			pingAt = append(pingAt, i)
			ackBDPPing(b)
		}
	}

	if updates != 0 || b.bdp != initialWindowSize {
		t.Fatalf("Estimate updated %d times to %d, want no updates", updates, b.bdp)
	}
	if len(pingAt) == 0 || pingAt[0] != 0 {
		t.Fatalf("First ping at frame %v, want frame 0", pingAt)
	}
	// The threshold counts the frame that started the last ping and the frame
	// that starts the next one.
	for k := 1; k < len(pingAt); k++ {
		threshold := min(k, maxPingBackoff) * (initialWindowSize / 2)
		wantGap := (threshold+frameSize-1)/frameSize - 1
		if gotGap := pingAt[k] - pingAt[k-1]; gotGap != wantGap {
			t.Fatalf("Gap before ping %d = %d frames, want %d", k, gotGap, wantGap)
		}
	}
}

// TestBDPEstimator_GrowingSampleResetsBackoff checks that a large sample still
// grows the estimate at the capped backoff. The next frame then starts a ping.
func (s) TestBDPEstimator_GrowingSampleResetsBackoff(t *testing.T) {
	testutils.SetEnvConfig(t, &envconfig.EnableBDPPingBackoff, true)
	var gotUpdate uint32
	b := &bdpEstimator{
		bdp:               initialWindowSize,
		updateFlowControl: func(n uint32) { gotUpdate = n },
	}
	for range maxPingBackoff + 1 {
		addUntilPing(t, b, 100)
		ackBDPPing(b)
	}
	if b.backoff != maxPingBackoff {
		t.Fatalf("backoff = %d, want %d", b.backoff, maxPingBackoff)
	}

	// The 100-byte frame that started the last ping counts toward the
	// threshold.
	const frameSize = 16 * 1024
	threshold := maxPingBackoff * (initialWindowSize / 2)
	wantFrames := (threshold-100+frameSize-1)/frameSize - 1
	if frames := addUntilPing(t, b, frameSize); frames != wantFrames {
		t.Fatalf("Ping started after %d frames of %d bytes, want %d", frames, frameSize, wantFrames)
	}
	// About one window of data arrives before the ack. With the frame that
	// started the ping the sample is four frames.
	const framesBeforeAck = 3
	for range framesBeforeAck {
		if b.add(frameSize) {
			t.Fatal("add() started a ping while one is outstanding")
		}
	}
	ackBDPPing(b)

	wantBDP := uint32(gamma * (framesBeforeAck + 1) * frameSize)
	if gotUpdate != wantBDP || b.bdp != wantBDP {
		t.Fatalf("Estimate = %d with update %d, want %d", b.bdp, gotUpdate, wantBDP)
	}
	if b.backoff != 0 {
		t.Fatalf("backoff = %d after the estimate grew, want 0", b.backoff)
	}
	if !b.add(100) {
		t.Fatal("add() did not start a ping on the first frame after the estimate grew")
	}
}

// TestBDPEstimator_AckWithoutPingIgnored checks that an ack that arrives while
// the estimator is backing off does not count the data received over many
// round trips as one sample.
func (s) TestBDPEstimator_AckWithoutPingIgnored(t *testing.T) {
	testutils.SetEnvConfig(t, &envconfig.EnableBDPPingBackoff, true)
	b := &bdpEstimator{
		bdp:               initialWindowSize,
		updateFlowControl: func(n uint32) { t.Fatalf("Estimate updated to %d, want no update", n) },
	}
	for range maxPingBackoff {
		addUntilPing(t, b, 100)
		ackBDPPing(b)
	}
	// Stay below the ping threshold but well above beta*bdp.
	for range initialWindowSize / 1024 * 2 {
		if b.add(1024) {
			t.Fatal("add() started a ping before the backoff threshold")
		}
	}
	ackBDPPing(b)
	if b.bdp != initialWindowSize {
		t.Fatalf("Estimate = %d, want %d", b.bdp, initialWindowSize)
	}
}

// TestBDPEstimator_AckBeforeFirstPing checks that an ack before the first BDP
// ping leaves rtt finite. A later large sample then still grows the estimate.
// The guard does not depend on the backoff.
func (s) TestBDPEstimator_AckBeforeFirstPing(t *testing.T) {
	for _, backoff := range []bool{true, false} {
		t.Run(fmt.Sprintf("backoff=%v", backoff), func(t *testing.T) {
			testutils.SetEnvConfig(t, &envconfig.EnableBDPPingBackoff, backoff)
			var gotUpdate uint32
			b := &bdpEstimator{
				bdp:               initialWindowSize,
				updateFlowControl: func(n uint32) { gotUpdate = n },
			}
			b.calculate(bdpPing.data)
			if math.IsNaN(b.rtt) || math.IsInf(b.rtt, 0) {
				t.Fatalf("rtt = %v after an ack before the first ping, want a finite value", b.rtt)
			}

			const frameSize = 16 * 1024
			const frames = 4
			for i := range frames {
				if got, want := b.add(frameSize), i == 0; got != want {
					t.Fatalf("add() on frame %d = %v, want %v", i, got, want)
				}
			}
			ackBDPPing(b)
			if wantBDP := uint32(gamma * frames * frameSize); gotUpdate != wantBDP || b.bdp != wantBDP {
				t.Fatalf("Estimate = %d with update %d, want %d", b.bdp, gotUpdate, wantBDP)
			}
		})
	}
}

func (s) TestBDPEstimator_BackoffDisabled(t *testing.T) {
	testutils.SetEnvConfig(t, &envconfig.EnableBDPPingBackoff, false)
	b := &bdpEstimator{
		bdp:               initialWindowSize,
		updateFlowControl: func(n uint32) { t.Fatalf("Estimate updated to %d, want no update", n) },
	}
	for i := range 1000 {
		if !b.add(100) {
			t.Fatalf("add() on frame %d did not start a ping, want a ping on every frame", i)
		}
		ackBDPPing(b)
	}
}
