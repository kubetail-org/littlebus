# LittleBus

*LittleBus is a tiny, in-memory broadcast library for Go based around buffered channels*

<img width="480" alt="littlebus" src="https://github.com/user-attachments/assets/3d0d5832-d76c-499d-8c8e-72612c846392" />

## Introduction

LittleBus is a tiny, in-memory async broadcast library for Go that uses generics and buffered channels to enable simple, type-safe broadcast workflows with predictable behavior. Using LittleBus you can publish to many subscribers simultaneously in a thread-safe way with per-subscriber FIFO ordering and bounded memory usage. Although similar patterns have been implemented in existing libraries, we couldn't find a simple one with clear delivery guarantees, so we built this one to use ourselves (for [Kubetail](https://github.com/kubetail-org/kubetail)). We hope you find it useful too.

## Installation

```bash
go get github.com/kubetail-org/littlebus
```

## Basic Usage

```go
import "github.com/kubetail-org/littlebus"

// Initialize (type-args specify topic key type and message type)
lb := littlebus.New[string, string]()

// Publish message (fire-and-forget)
lb.Publish("my-topic", "my-message")

// Create a subscription
sub := lb.Subscribe("my-topic")

// Get messages from the subscription
for msg := range sub.Ch():
  fmt.Println(msg)

// Stop listening for new messages
sub.Unsubscribe()

// Reject new subscribers, close existing subscription channels, silently ignore future publishers
lb.Close()
```

Topics can be any `comparable` type, so you can use typed enums instead of strings to prevent typos and get exhaustiveness from your tooling:

```go
type Event int
const (
    EventUserCreated Event = iota
    EventUserDeleted
)

lb := littlebus.New[Event, User]()
sub := lb.Subscribe(EventUserCreated)
lb.Publish(EventUserCreated, user)
```

## Delivery Guarantees

- **In-memory only**: Messages are not persisted and publishing to a topic with no subscribers silently drops the message
- **Non-blocking publish**: Publish never waits on subscriber progress, regardless of subscriber speed
- **Bounded memory**: each subscription's buffer is a fixed-capacity buffered channel; there is no unbounded queue
- **Subscriber isolation**: a slow or stuck subscriber affects only its own message delivery, never other subscribers or publishers
- **Per-subscriber FIFO ordering**: delivered messages arrive in the order they were published (gaps possible under overflow policy, but never reordering)
- **At-most-once delivery**: each subscriber receives each message zero or one times, never more

If you need lossless delivery, persistence, or cross-process pubsub, you want a real message broker (NATS, Redis Streams, Kafka) — not an in-memory library.

## How It Works

Each subscription owns a buffered channel sized by `WithBufferSize` (default: 1). `Publish` performs a non-blocking send into every current subscriber's channel and returns. Because the channel is the queue, consumers receive directly from the same channel the publisher writes to. When a subscriber's channel is full, the overflow policy decides what happens (see below).

## Slow Subscribers

When a subscriber's channel is full, the configured overflow policy determines which message to drop:

* `DropNewest` (default): the incoming message is discarded. Preserves the earliest unread messages. This maps to a non-blocking send and is exact.

* `DropOldest`: the oldest buffered message is evicted via a non-blocking receive, then the new message is sent. Preserves the most recent messages.

Choose `DropNewest` when historical context matters most (event streams, audit logs). Choose `DropOldest` when recent state matters most (status updates, price ticks). Either way, delivered messages remain in publish order. Each subscription tracks its own drop count, exposed via `sub.DropCount()`. If your publisher needs to know when subscribers are falling behind, poll this and throttle upstream.

Note: `DropOldest` is **best-effort under contention** because the consumer reads from the same channel so the "evicted" message may actually be delivered to the consumer between the receive and the send. In that case the new message still lands in the buffer and ordering is preserved, but `DropCount` may over by the number of such races. The guarantee is "the buffer never grows past its capacity," not "exactly the oldest message is dropped."

## API

### Constructor

#### `New[K comparable, T any]() *LittleBus[K, T]`

Creates a new LittleBus instance parameterized with a topic key type `K` and a message type `T`. Any `comparable` type works as a topic key — `string` is typical, but typed enums are also supported.

```go
lb := littlebus.New[string, string]()
```

### LittleBus

#### `Publish(topic K, msg T)`

Publishes a message to all subscribers of a topic. This is a fire-and-forget operation that does not block on subscriber progress (a stuck consumer cannot stall the publisher). Publishing to a topic with no subscribers, or after `Close()`, is a no-op.

```go
lb.Publish("my-topic", "hello world")
```

#### `Subscribe(topic K, opts... Option) Subscription[T]`

Creates a subscription for a topic. Messages are delivered in order via a channel.

```go
sub := lb.Subscribe("my-topic")
sub := lb.Subscribe("my-topic", littlebus.WithOverflowPolicy(littlebus.PolicyDropOldest))
```

#### `Close()`

Closes all subscription channels and causes future Publish calls to become no-ops. Idempotent.

```go
lb.Close()
```

### Subscription

#### `Ch() <- chan T`

Returns a channel that receives published messages in the order they were published.

```go
sub := lb.Subscribe("my-topic")
for msg := range sub.Ch():
  fmt.Println(msg)
```

#### `Unsubscribe()`

Removes the subscriber from the topic, stopping it from receiving new messages.

```go
sub.Unsubscribe()
```

#### `DropCount() uint64`

Returns the cumulative number of messages dropped for this subscription due to overflow. Useful for detecting slow subscribers.

```go
if sub.DropCount() > 0 {
    log.Printf("subscriber falling behind: %d drops", sub.DropCount())
}
```

### Options

#### WithBufferSize(n int) Option

Capacity of the subscription's buffered channel. Must be `>= 1`; passing `0` or a negative value panics, because an unbuffered channel would force `Publish` to either block or drop every message. Defaults to `1`, which gives the strongest backpressure signal via `DropCount` — increase it when you'd rather absorb bursts than observe them.

#### WithOverflowPolicy(p Policy) Option

Default overflow policy for subscriptions. Defaults to `DropNewest`.

### Policy

```go
type Policy int

const (
    PolicyDropNewest Policy = iota // discard incoming messages when full (default)
    PolicyDropOldest               // evict the oldest queued message to make room
)
```
