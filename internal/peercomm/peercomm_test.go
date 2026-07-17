package peercomm

import (
	"context"
	"errors"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-pubsub/internal/topicstreams"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"google.golang.org/protobuf/proto"
)

func TestRPCQueuePriorityAndCapacity(t *testing.T) {
	q := newRPCQueue(2)
	normal := &pb.RPC{}
	urgent := &pb.RPC{}
	if err := q.push(normal, false, false); err != nil {
		t.Fatal(err)
	}
	if err := q.push(urgent, true, false); err != nil {
		t.Fatal(err)
	}
	if err := q.push(&pb.RPC{}, false, false); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("expected queue full, got %v", err)
	}
	got, err := q.pop(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got != urgent {
		t.Fatal("urgent RPC was not drained first")
	}
	got, err = q.pop(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got != normal {
		t.Fatal("normal RPC was not preserved")
	}
}

func TestRPCQueueCloseRejectsSend(t *testing.T) {
	q := newRPCQueue(1)
	q.close()
	if err := q.push(&pb.RPC{}, false, false); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("expected queue closed, got %v", err)
	}
}

func TestOutboundTerminalCallbackCanRestart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var callbacks atomic.Int32
	var actor *Actor
	registry := NewRegistry(ctx, Config{OutboundQueueSize: 1}, Hooks{
		OutboundOpenFailed: func(peer.ID) {
			if callbacks.Add(1) == 1 {
				actor.Start(time.Hour)
			}
		},
	})
	actor = registry.For(peer.ID("restart-peer"))
	actor.Start(0)
	deadline := time.After(time.Second)
	for callbacks.Load() != 1 {
		select {
		case <-deadline:
			t.Fatalf("expected one terminal callback, got %d", callbacks.Load())
		default:
			runtime.Gosched()
		}
	}
	if _, ok := registry.Existing(actor.peer); !ok {
		t.Fatal("callback restart did not retain actor")
	}
}

func TestRPCQueuePopCancellationReturnsPromptly(t *testing.T) {
	q := newRPCQueue(1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := q.pop(ctx); done <- err }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected cancellation, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("queue pop remained blocked after cancellation")
	}
}

func TestRegistryExistingReportsPresence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registry := NewRegistry(ctx, Config{OutboundQueueSize: 1}, Hooks{})
	pid := peer.ID("existing-peer")
	if actor, ok := registry.Existing(pid); ok || actor != nil {
		t.Fatal("missing actor reported as existing")
	}
	created := registry.For(pid)
	if actor, ok := registry.Existing(pid); !ok || actor != created {
		t.Fatal("existing actor was not returned with ok=true")
	}
}

func TestRetiredActorRejectsSend(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registry := NewRegistry(ctx, Config{
		OutboundQueueSize:         1,
		MaxInboundStreamsPerTopic: 1,
		MaxInboundStreamsPerPeer:  1,
	}, Hooks{})
	actor := registry.For(peer.ID("retiring-peer"))
	admission := actor.OpenInboundTopic(nil, "topic")
	if !admission.Accepted() {
		t.Fatal("topic stream was not admitted")
	}
	actor.CloseInboundTopic(admission)
	select {
	case <-actor.Done():
	case <-time.After(time.Second):
		t.Fatal("actor did not retire after its final stream closed")
	}
	if err := actor.Send(&pb.RPC{}, false); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("retired actor Send returned %v, want %v", err, ErrQueueClosed)
	}
}

type remoteUnsubscribeHost struct {
	host.Host
	stream network.Stream
}

func (h remoteUnsubscribeHost) NewStream(context.Context, peer.ID, ...protocol.ID) (network.Stream, error) {
	return h.stream, nil
}

type remoteUnsubscribeStream struct {
	network.Stream
	writes         atomic.Int32
	payloadStarted chan struct{}
	releasePayload chan struct{}
	readDone       chan struct{}
	reset          chan struct{}
	resetOnce      sync.Once
}

func newRemoteUnsubscribeStream() *remoteUnsubscribeStream {
	return &remoteUnsubscribeStream{
		payloadStarted: make(chan struct{}),
		releasePayload: make(chan struct{}),
		readDone:       make(chan struct{}),
		reset:          make(chan struct{}),
	}
}

func (s *remoteUnsubscribeStream) Write(p []byte) (int, error) {
	if s.writes.Add(1) == 2 {
		close(s.payloadStarted)
		<-s.releasePayload
	}
	return len(p), nil
}

func (s *remoteUnsubscribeStream) Read([]byte) (int, error) {
	<-s.readDone
	return 0, io.EOF
}

func (s *remoteUnsubscribeStream) SetWriteDeadline(time.Time) error { return nil }
func (s *remoteUnsubscribeStream) Reset() error {
	s.resetOnce.Do(func() {
		close(s.readDone)
		close(s.reset)
	})
	return nil
}

func TestQueuedPayloadAfterUnsubscribeCannotRecreateWriter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := newRemoteUnsubscribeStream()
	dropped := make(chan topicstreams.Envelope, 1)
	registry := NewRegistry(ctx, Config{
		Host:              remoteUnsubscribeHost{stream: stream},
		OutboundQueueSize: 1,
		TopicStreamsHooks: topicstreams.OutboundHooks{Drop: func(_ peer.ID, e topicstreams.Envelope) { dropped <- e }},
	}, Hooks{})
	actor := registry.For(peer.ID("peer"))
	topics := topicstreams.NewOutboundStreams(ctx, remoteUnsubscribeHost{stream: stream}, peer.ID("peer"), 1, nil, registry.config.TopicStreamsHooks)
	generation, ok := actor.beginOutboundSession()
	if !ok || !actor.attachOutbound(generation, topics) {
		t.Fatal("failed to establish outbound session")
	}
	actor.SetTopicStreamsEnabled(Session{generation}, true)
	actor.RemoteSubscribed(Session{generation}, "topic")

	first := &pb.RPC{Publish: []*pb.Message{{Topic: proto.String("topic"), Data: []byte("first")}}}
	if result := actor.routeOutbound(ctx, generation, first); result.Accepted != 1 {
		t.Fatalf("first payload was not accepted: %#v", result)
	}
	select {
	case <-stream.payloadStarted:
	case <-time.After(time.Second):
		t.Fatal("outbound topic stream did not start")
	}

	actor.RemoteUnsubscribed(Session{generation}, "topic")
	queued := &pb.RPC{Publish: []*pb.Message{{Topic: proto.String("topic"), Data: []byte("queued")}}}
	result := actor.routeOutbound(ctx, generation, queued)
	if result.Accepted != 0 || result.Dropped != 1 {
		t.Fatalf("stale queued payload disposition: %#v", result)
	}
	select {
	case got := <-dropped:
		if string(got.Publish.Data) != "queued" {
			t.Fatalf("unexpected dropped payload %q", got.Publish.Data)
		}
	case <-time.After(time.Second):
		t.Fatal("stale queued payload was not reported dropped")
	}
	close(stream.releasePayload)
	select {
	case <-stream.reset:
	case <-time.After(time.Second):
		t.Fatal("unsubscribe did not close existing writer")
	}
	if got := stream.writes.Load(); got != 2 {
		t.Fatalf("queued payload recreated writer: got %d writes, want header plus first payload", got)
	}
}

func TestOutboundSessionReplacementRetiresAuthorization(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := newRemoteUnsubscribeStream()
	registry := NewRegistry(ctx, Config{OutboundQueueSize: 1}, Hooks{})
	actor := registry.For(peer.ID("peer"))

	firstGeneration, ok := actor.beginOutboundSession()
	if !ok {
		t.Fatal("failed to begin first session")
	}
	firstStreams := topicstreams.NewOutboundStreams(ctx, remoteUnsubscribeHost{stream: stream}, peer.ID("peer"), 1, nil, topicstreams.OutboundHooks{})
	if !actor.attachOutbound(firstGeneration, firstStreams) {
		t.Fatal("failed to attach first session streams")
	}
	actor.SetTopicStreamsEnabled(Session{firstGeneration}, true)
	actor.RemoteSubscribed(Session{firstGeneration}, "topic")

	secondGeneration, ok := actor.beginOutboundSession()
	if !ok || secondGeneration == firstGeneration {
		t.Fatal("replacement did not create a distinct session")
	}
	secondStreams := topicstreams.NewOutboundStreams(ctx, remoteUnsubscribeHost{stream: stream}, peer.ID("peer"), 1, nil, topicstreams.OutboundHooks{})
	if !actor.attachOutbound(secondGeneration, secondStreams) {
		t.Fatal("failed to attach replacement streams")
	}
	actor.SetTopicStreamsEnabled(Session{secondGeneration}, true)

	rpc := &pb.RPC{Publish: []*pb.Message{{Topic: proto.String("topic")}}}
	if result := actor.routeOutbound(ctx, firstGeneration, rpc); result.Accepted != 0 || result.Control != rpc {
		t.Fatalf("retired generation routed payload: %#v", result)
	}
	if result := actor.routeOutbound(ctx, secondGeneration, rpc); result.Accepted != 0 || result.Dropped != 1 {
		t.Fatalf("replacement inherited authorization: %#v", result)
	}
}

func TestStaleSessionMutationsIgnored(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registry := NewRegistry(ctx, Config{OutboundQueueSize: 1}, Hooks{})
	actor := registry.For(peer.ID("peer"))
	first, ok := actor.beginOutboundSession()
	if !ok {
		t.Fatal("first session")
	}
	second, ok := actor.beginOutboundSession()
	if !ok {
		t.Fatal("second session")
	}
	stale := Session{first}
	current := Session{second}
	actor.SetTopicStreamsEnabled(stale, true)
	actor.RemoteSubscribed(stale, "topic")
	actor.RemoteUnsubscribed(stale, "topic")
	if actor.TopicStreamsEnabled() {
		t.Fatal("stale enable mutated replacement")
	}
	actor.SetTopicStreamsEnabled(current, true)
	if !actor.TopicStreamsEnabled() {
		t.Fatal("current enable was ignored")
	}
	actor.SetTopicStreamsEnabled(stale, false)
	if !actor.TopicStreamsEnabled() {
		t.Fatal("stale disable mutated replacement")
	}
}

func TestProcessLoopMutationDoesNotWaitForBlockedActor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	emitting := make(chan struct{})
	release := make(chan struct{})
	registry := NewRegistry(ctx, Config{OutboundQueueSize: 1}, Hooks{EmitInbound: func(_ peer.ID, event InboundEvent) bool {
		if event.Kind == InboundRPC {
			close(emitting)
			<-release
		}
		return true
	}})
	actor := registry.For(peer.ID("peer"))
	generation, ok := actor.OpenInboundControl(newRemoteUnsubscribeStream())
	if !ok {
		t.Fatal("open inbound control")
	}
	actor.DeliverInboundControl(generation, &pb.RPC{})
	select {
	case <-emitting:
	case <-time.After(time.Second):
		t.Fatal("actor did not enter inbound delivery")
	}
	done := make(chan struct{})
	go func() {
		actor.RemoteSubscribed(Session{generation: 1}, "topic")
		actor.RemoteUnsubscribed(Session{generation: 1}, "topic")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("process-loop mutation waited for actor")
	}
	close(release)
}

func TestRouteCancellationNeverFallsBackToControl(t *testing.T) {
	registryCtx, stop := context.WithCancel(context.Background())
	defer stop()
	ctx, cancel := context.WithCancel(context.Background())
	registry := NewRegistry(registryCtx, Config{OutboundQueueSize: 1}, Hooks{})
	actor := registry.For(peer.ID("peer"))
	generation, ok := actor.beginOutboundSession()
	if !ok {
		t.Fatal("session")
	}
	cancel()
	rpc := &pb.RPC{Publish: []*pb.Message{{Topic: proto.String("topic")}}}
	result := actor.routeOutbound(ctx, generation, rpc)
	if result.disposition != routeAborted || result.Control != nil {
		t.Fatalf("canceled route manufactured control fallback: %#v", result)
	}
}

func TestPostSubmitCancellationReturnsTerminalRouteDisposition(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	emitting, release := make(chan struct{}), make(chan struct{})
	var drops atomic.Int32
	stream := newRemoteUnsubscribeStream()
	registry := NewRegistry(ctx, Config{
		OutboundQueueSize: 1,
		TopicStreamsHooks: topicstreams.OutboundHooks{Drop: func(peer.ID, topicstreams.Envelope) { drops.Add(1) }},
	}, Hooks{EmitInbound: func(_ peer.ID, event InboundEvent) bool {
		if event.Kind == InboundRPC {
			close(emitting)
			<-release
		}
		return true
	}})
	actor := registry.For(peer.ID("route-peer"))
	generation, ok := actor.beginOutboundSession()
	if !ok {
		t.Fatal("begin outbound session")
	}
	streams := topicstreams.NewOutboundStreams(ctx, remoteUnsubscribeHost{stream: stream}, peer.ID("route-peer"), 1, nil, registry.config.TopicStreamsHooks)
	if !actor.attachOutbound(generation, streams) {
		t.Fatal("attach outbound streams")
	}
	actor.SetTopicStreamsEnabled(Session{generation}, true)

	inbound, ok := actor.OpenInboundControl(stream)
	if !ok {
		t.Fatal("open inbound control")
	}
	actor.DeliverInboundControl(inbound, &pb.RPC{})
	<-emitting

	routeCtx, routeCancel := context.WithCancel(context.Background())
	done := make(chan routeReply, 1)
	topic, subscribe := "unauthorized", true
	go func() {
		done <- actor.routeOutbound(routeCtx, generation, &pb.RPC{
			Publish:       []*pb.Message{{Topic: &topic}},
			Subscriptions: []*pb.RPC_SubOpts{{Topicid: &topic, Subscribe: &subscribe}},
		})
	}()
	time.Sleep(10 * time.Millisecond) // route event is queued behind the blocked actor
	routeCancel()
	close(release)
	result := <-done
	if result.disposition != routeCompleted || result.Dropped != 1 || result.Accepted != 0 {
		t.Fatalf("unexpected terminal route disposition: %#v", result)
	}
	if result.Control == nil || len(result.Control.Subscriptions) != 1 {
		t.Fatalf("control remainder was lost: %#v", result.Control)
	}
	if drops.Load() != 1 {
		t.Fatalf("drop accounting=%d, want 1", drops.Load())
	}
}

func TestOutboundTerminationRetiresActorAndQueue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registry := NewRegistry(ctx, Config{OutboundQueueSize: 1}, Hooks{})
	pid := peer.ID("churn-peer")
	old := registry.For(pid)
	old.Start(0)
	select {
	case <-old.Done():
	case <-time.After(time.Second):
		t.Fatal("actor was not retired after outbound lifetime ended")
	}
	if _, ok := registry.Existing(pid); ok {
		t.Fatal("retired actor remained in registry")
	}
	if err := old.Send(&pb.RPC{}, false); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("retired queue accepted send: %v", err)
	}
	fresh := registry.For(pid)
	if fresh == old {
		t.Fatal("registry reused stale actor")
	}
}
