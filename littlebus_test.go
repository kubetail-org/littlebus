// Copyright 2026 The Kubetail Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package littlebus

import (
	"sync"
	"testing"
	"time"
)

func recvWithTimeout[T any](t *testing.T, ch <-chan T, d time.Duration) (T, bool) {
	t.Helper()
	select {
	case v, ok := <-ch:
		return v, ok
	case <-time.After(d):
		var zero T
		return zero, false
	}
}

func TestPublishSubscribeSingle(t *testing.T) {
	lb := New[string, string]()
	defer lb.Close()

	sub := lb.Subscribe("topic")
	lb.Publish("topic", "hello")

	v, ok := recvWithTimeout(t, sub.Ch(), time.Second)
	if !ok || v != "hello" {
		t.Fatalf("expected hello, got %q ok=%v", v, ok)
	}
}

func TestPublishNoSubscribersIsNoOp(t *testing.T) {
	lb := New[string, int]()
	defer lb.Close()
	// must not panic, must not block
	lb.Publish("nobody", 1)
}

func TestSubscribeOnlyReceivesItsTopic(t *testing.T) {
	lb := New[string, string]()
	defer lb.Close()

	a := lb.Subscribe("a")
	b := lb.Subscribe("b")

	lb.Publish("a", "msg-a")
	lb.Publish("b", "msg-b")

	if v, _ := recvWithTimeout(t, a.Ch(), time.Second); v != "msg-a" {
		t.Fatalf("a got %q", v)
	}
	if v, _ := recvWithTimeout(t, b.Ch(), time.Second); v != "msg-b" {
		t.Fatalf("b got %q", v)
	}
}

func TestMultipleSubscribersAllReceive(t *testing.T) {
	lb := New[string, int]()
	defer lb.Close()

	subs := make([]Subscription[int], 5)
	for i := range subs {
		subs[i] = lb.Subscribe("t")
	}
	lb.Publish("t", 42)
	for i, s := range subs {
		v, ok := recvWithTimeout(t, s.Ch(), time.Second)
		if !ok || v != 42 {
			t.Fatalf("sub %d got %d ok=%v", i, v, ok)
		}
	}
}

func TestUnsubscribeClosesChannel(t *testing.T) {
	lb := New[string, int]()
	defer lb.Close()

	sub := lb.Subscribe("t")
	sub.Unsubscribe()

	select {
	case _, ok := <-sub.Ch():
		if ok {
			t.Fatal("expected closed channel")
		}
	case <-time.After(time.Second):
		t.Fatal("channel not closed after Unsubscribe")
	}
}

func TestUnsubscribeStopsDelivery(t *testing.T) {
	lb := New[string, int]()
	defer lb.Close()

	sub := lb.Subscribe("t")
	sub.Unsubscribe()
	// Should not panic even if publish happens after unsubscribe
	lb.Publish("t", 1)
}

func TestCloseClosesSubscriptions(t *testing.T) {
	lb := New[string, int]()
	sub := lb.Subscribe("t")
	lb.Close()

	select {
	case _, ok := <-sub.Ch():
		if ok {
			t.Fatal("expected closed channel after Close")
		}
	case <-time.After(time.Second):
		t.Fatal("subscription channel not closed after Close")
	}
}

func TestCloseIdempotent(t *testing.T) {
	lb := New[string, int]()
	lb.Close()
	lb.Close() // must not panic
}

func TestPublishAfterCloseIsNoOp(t *testing.T) {
	lb := New[string, int]()
	lb.Close()
	lb.Publish("t", 1) // must not panic
}

func TestSubscribeAfterCloseReturnsClosed(t *testing.T) {
	lb := New[string, int]()
	lb.Close()
	sub := lb.Subscribe("t")
	select {
	case _, ok := <-sub.Ch():
		if ok {
			t.Fatal("expected closed channel for subscribe after close")
		}
	case <-time.After(time.Second):
		t.Fatal("subscribe-after-close channel not closed")
	}
}

func TestDropNewestDefault(t *testing.T) {
	lb := New[string, int]()
	defer lb.Close()

	sub := lb.Subscribe("t") // default buffer 1, drop newest
	lb.Publish("t", 1)
	lb.Publish("t", 2) // dropped
	lb.Publish("t", 3) // dropped

	v, ok := recvWithTimeout(t, sub.Ch(), time.Second)
	if !ok || v != 1 {
		t.Fatalf("expected 1, got %d", v)
	}
	if dc := sub.DropCount(); dc != 2 {
		t.Fatalf("expected DropCount 2, got %d", dc)
	}
}

func TestDropOldest(t *testing.T) {
	lb := New[string, int]()
	defer lb.Close()

	sub := lb.Subscribe("t", WithBufferSize(2), WithOverflowPolicy(PolicyDropOldest))
	lb.Publish("t", 1)
	lb.Publish("t", 2)
	lb.Publish("t", 3) // evicts 1
	lb.Publish("t", 4) // evicts 2

	got := []int{}
	for i := 0; i < 2; i++ {
		v, ok := recvWithTimeout(t, sub.Ch(), time.Second)
		if !ok {
			t.Fatalf("recv failed at %d", i)
		}
		got = append(got, v)
	}
	if got[0] != 3 || got[1] != 4 {
		t.Fatalf("expected [3 4], got %v", got)
	}
}

func TestWithBufferSizePanicsOnZero(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for WithBufferSize(0)")
		}
	}()
	lb := New[string, int]()
	defer lb.Close()
	lb.Subscribe("t", WithBufferSize(0))
}

func TestWithBufferSizePanicsOnNegative(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for WithBufferSize(-1)")
		}
	}()
	lb := New[string, int]()
	defer lb.Close()
	lb.Subscribe("t", WithBufferSize(-1))
}

func TestFIFOOrdering(t *testing.T) {
	lb := New[string, int]()
	defer lb.Close()

	sub := lb.Subscribe("t", WithBufferSize(100))
	for i := 0; i < 50; i++ {
		lb.Publish("t", i)
	}
	for i := 0; i < 50; i++ {
		v, ok := recvWithTimeout(t, sub.Ch(), time.Second)
		if !ok || v != i {
			t.Fatalf("expected %d, got %d ok=%v", i, v, ok)
		}
	}
}

func TestSubscriberIsolation(t *testing.T) {
	lb := New[string, int]()
	defer lb.Close()

	slow := lb.Subscribe("t") // buffer 1, won't drain
	fast := lb.Subscribe("t", WithBufferSize(100))

	for i := 0; i < 50; i++ {
		lb.Publish("t", i) // must not block even though slow's buffer fills
	}

	for i := 0; i < 50; i++ {
		v, ok := recvWithTimeout(t, fast.Ch(), time.Second)
		if !ok || v != i {
			t.Fatalf("fast: expected %d, got %d", i, v)
		}
	}
	_ = slow
}

func TestUnsubscribeDuringPublishNoPanic(t *testing.T) {
	lb := New[string, int]()
	defer lb.Close()

	var wg sync.WaitGroup
	subs := make([]Subscription[int], 50)
	for i := range subs {
		subs[i] = lb.Subscribe("t")
	}

	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 10000; i++ {
			lb.Publish("t", i)
		}
	}()
	go func() {
		defer wg.Done()
		for _, s := range subs {
			s.Unsubscribe()
		}
	}()
	wg.Wait()
}

func TestPublishNonBlockingUnderSlowSubscriber(t *testing.T) {
	lb := New[string, int]()
	defer lb.Close()

	_ = lb.Subscribe("t") // never reads

	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			lb.Publish("t", i)
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("publish blocked on slow subscriber")
	}
}

func TestTypedTopic(t *testing.T) {
	type Event int
	const (
		EventA Event = iota
		EventB
	)

	lb := New[Event, string]()
	defer lb.Close()

	subA := lb.Subscribe(EventA)
	subB := lb.Subscribe(EventB)
	lb.Publish(EventA, "alpha")
	lb.Publish(EventB, "beta")

	if v, _ := recvWithTimeout(t, subA.Ch(), time.Second); v != "alpha" {
		t.Fatalf("subA got %q", v)
	}
	if v, _ := recvWithTimeout(t, subB.Ch(), time.Second); v != "beta" {
		t.Fatalf("subB got %q", v)
	}
}

func TestConcurrentPublishersSameOrderAcrossSubscribers(t *testing.T) {
	const publishers = 4
	const perPublisher = 250
	const total = publishers * perPublisher

	lb := New[string, int]()
	defer lb.Close()

	subs := make([]Subscription[int], 5)
	for i := range subs {
		subs[i] = lb.Subscribe("t", WithBufferSize(total))
	}

	var wg sync.WaitGroup
	for p := 0; p < publishers; p++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := 0; i < perPublisher; i++ {
				lb.Publish("t", base*perPublisher+i)
			}
		}(p)
	}
	wg.Wait()

	var reference []int
	for si, s := range subs {
		got := make([]int, 0, total)
		for i := 0; i < total; i++ {
			v, ok := recvWithTimeout(t, s.Ch(), time.Second)
			if !ok {
				t.Fatalf("sub %d: recv %d timed out", si, i)
			}
			got = append(got, v)
		}
		if si == 0 {
			reference = got
			continue
		}
		for i := range got {
			if got[i] != reference[i] {
				t.Fatalf("sub %d diverges from sub 0 at index %d: got %d, want %d", si, i, got[i], reference[i])
			}
		}
	}
}

func TestConcurrentPublishSubscribe(t *testing.T) {
	lb := New[string, int]()
	defer lb.Close()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s := lb.Subscribe("t", WithBufferSize(1000))
			defer s.Unsubscribe()
			time.Sleep(10 * time.Millisecond)
		}()
	}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(v int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				lb.Publish("t", v)
			}
		}(i)
	}
	wg.Wait()
}
