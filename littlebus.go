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
	"slices"
	"sync"
	"sync/atomic"
)

type Policy int

const (
	PolicyDropNewest Policy = iota
	PolicyDropOldest
)

type subOptions struct {
	bufferSize int
	policy     Policy
}

type Option func(*subOptions)

func WithBufferSize(n int) Option {
	if n < 1 {
		panic("littlebus: WithBufferSize requires n >= 1")
	}
	return func(o *subOptions) { o.bufferSize = n }
}

func WithOverflowPolicy(p Policy) Option {
	return func(o *subOptions) { o.policy = p }
}

type Subscription[T any] interface {
	Ch() <-chan T
	Unsubscribe()
	DropCount() uint64
}

type subscription[K comparable, T any] struct {
	ch        chan T
	policy    Policy
	dropCount atomic.Uint64
	bus       *LittleBus[K, T]
	topic     K
	closeOnce sync.Once
}

func (s *subscription[K, T]) Ch() <-chan T      { return s.ch }
func (s *subscription[K, T]) DropCount() uint64 { return s.dropCount.Load() }
func (s *subscription[K, T]) Unsubscribe()      { s.bus.removeSub(s.topic, s) }
func (s *subscription[K, T]) close()            { s.closeOnce.Do(func() { close(s.ch) }) }

// deliver runs under the bus lock, so the channel cannot be closed concurrently.
func (s *subscription[K, T]) deliver(msg T) {
	switch s.policy {
	case PolicyDropNewest:
		select {
		case s.ch <- msg:
		default:
			s.dropCount.Add(1)
		}
	case PolicyDropOldest:
		for {
			select {
			case s.ch <- msg:
				return
			default:
			}
			select {
			case <-s.ch:
				s.dropCount.Add(1)
			default:
				// raced with the consumer; retry
			}
		}
	}
}

type LittleBus[K comparable, T any] struct {
	mu     sync.Mutex
	topics map[K][]*subscription[K, T]
	closed bool
}

func New[K comparable, T any]() *LittleBus[K, T] {
	return &LittleBus[K, T]{topics: make(map[K][]*subscription[K, T])}
}

func (lb *LittleBus[K, T]) Subscribe(topic K, opts ...Option) Subscription[T] {
	o := subOptions{bufferSize: 1, policy: PolicyDropNewest}
	for _, opt := range opts {
		opt(&o)
	}

	s := &subscription[K, T]{
		ch:     make(chan T, o.bufferSize),
		policy: o.policy,
		bus:    lb,
		topic:  topic,
	}

	lb.mu.Lock()
	defer lb.mu.Unlock()
	if lb.closed {
		s.close()
		return s
	}
	lb.topics[topic] = append(lb.topics[topic], s)
	return s
}

func (lb *LittleBus[K, T]) Publish(topic K, msg T) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	if lb.closed {
		return
	}
	for _, s := range lb.topics[topic] {
		s.deliver(msg)
	}
}

func (lb *LittleBus[K, T]) removeSub(topic K, target *subscription[K, T]) {
	lb.mu.Lock()
	subs := lb.topics[topic]
	for i, s := range subs {
		if s == target {
			newSubs := slices.Delete(subs, i, i+1)
			subs[len(subs)-1] = nil // slices.Delete doesn't zero the tail until Go 1.22
			if len(newSubs) == 0 {
				delete(lb.topics, topic)
			} else {
				lb.topics[topic] = newSubs
			}
			break
		}
	}
	lb.mu.Unlock()
	target.close()
}

func (lb *LittleBus[K, T]) Close() {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	if lb.closed {
		return
	}
	lb.closed = true
	for _, subs := range lb.topics {
		for _, s := range subs {
			s.close()
		}
	}
	lb.topics = nil
}
