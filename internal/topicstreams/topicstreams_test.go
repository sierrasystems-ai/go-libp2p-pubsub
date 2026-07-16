package topicstreams

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"google.golang.org/protobuf/proto"
)

func TestRouteRPCSendsTopicPayloadsAndReturnsControlRemainder(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	publishWriter := &writer{topic: "publish", ch: make(chan Envelope, 1), cancel: func() {}, done: make(chan struct{})}
	partialWriter := &writer{topic: "partial", ch: make(chan Envelope, 1), cancel: func() {}, done: make(chan struct{})}
	streams := &OutboundStreams{
		ctx: ctx, cancel: cancel, peer: peer.ID("peer"), queueSize: 1,
		streams: map[string]*writer{"publish": publishWriter, "partial": partialWriter},
	}
	subscribe := true
	rpc := &pb.RPC{
		Publish:       []*pb.Message{{Topic: proto.String("publish"), Data: []byte("message")}},
		Partial:       &pb.PartialMessagesExtension{TopicID: proto.String("partial")},
		Subscriptions: []*pb.RPC_SubOpts{{Topicid: proto.String("ours"), Subscribe: &subscribe}},
		Control:       &pb.ControlMessage{},
	}

	remainder := streams.RouteRPC(rpc)
	if remainder == nil || len(remainder.GetSubscriptions()) != 1 || remainder.GetControl() == nil {
		t.Fatalf("unexpected control remainder: %#v", remainder)
	}
	if len(remainder.GetPublish()) != 0 || remainder.GetPartial() != nil {
		t.Fatalf("topic payload leaked into control remainder: %#v", remainder)
	}
	if got := <-publishWriter.ch; !proto.Equal(got.Publish, rpc.Publish[0]) || got.Publish == rpc.Publish[0] {
		t.Fatal("publish was not cloned and routed to its topic stream")
	}
	if got := <-partialWriter.ch; !proto.Equal(got.Partial, rpc.Partial) || got.Partial == rpc.Partial {
		t.Fatal("partial extension was not cloned and routed to its topic stream")
	}
	if len(rpc.GetPublish()) != 1 || rpc.GetPartial() == nil {
		t.Fatal("routing mutated the caller's RPC")
	}
}

func TestRouteRPCTopicOnlyHasNoControlRemainder(t *testing.T) {
	rpc := &pb.RPC{Publish: []*pb.Message{{}}}
	streams := &OutboundStreams{}
	if got := streams.RouteRPC(rpc); got != nil {
		t.Fatalf("expected nil remainder, got %#v", got)
	}
}

func TestRouteRPCPreservesUnknownFields(t *testing.T) {
	rpc := &pb.RPC{}
	rpc.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
	streams := &OutboundStreams{}
	remainder := streams.RouteRPC(rpc)
	if remainder == nil || string(remainder.ProtoReflect().GetUnknown()) != string(rpc.ProtoReflect().GetUnknown()) {
		t.Fatalf("unknown fields were not preserved: %#v", remainder)
	}
}

type cancellationHost struct {
	host.Host
	stream network.Stream
}

func (h cancellationHost) NewStream(context.Context, peer.ID, ...protocol.ID) (network.Stream, error) {
	return h.stream, nil
}

type cancellationStream struct {
	network.Stream
	writes         atomic.Int32
	payloadStarted chan struct{}
	releasePayload chan struct{}
}

func newCancellationStream() *cancellationStream {
	return &cancellationStream{
		payloadStarted: make(chan struct{}),
		releasePayload: make(chan struct{}),
	}
}

func (s *cancellationStream) Write(p []byte) (int, error) {
	if s.writes.Add(1) == 2 {
		close(s.payloadStarted)
		<-s.releasePayload
	}
	return len(p), nil
}

func (s *cancellationStream) Read([]byte) (int, error)         { return 0, io.EOF }
func (s *cancellationStream) SetWriteDeadline(time.Time) error { return nil }
func (s *cancellationStream) Reset() error                     { return nil }

func TestWriterCancellationDoesNotDrainQueue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	stream := newCancellationStream()
	streams := &OutboundStreams{
		host:   cancellationHost{stream: stream},
		peer:   peer.ID("peer"),
		logger: discardLogger(),
	}
	w := &writer{
		topic: "topic",
		ch:    make(chan Envelope, 2),
		done:  make(chan struct{}),
	}
	w.ch <- Envelope{Topic: "topic", Publish: &pb.Message{Data: []byte("first")}}
	w.ch <- Envelope{Topic: "topic", Publish: &pb.Message{Data: []byte("queued")}}

	go streams.run(ctx, w)
	select {
	case <-stream.payloadStarted:
	case <-time.After(time.Second):
		t.Fatal("writer did not start the first payload")
	}
	cancel()
	close(stream.releasePayload)
	select {
	case <-w.done:
	case <-time.After(time.Second):
		t.Fatal("writer did not return after the in-progress write completed")
	}
	if got := stream.writes.Load(); got != 2 {
		t.Fatalf("cancellation drained queued payload: got %d writes, want header plus first payload", got)
	}
	if got := len(w.ch); got != 1 {
		t.Fatalf("queued payload was consumed after cancellation: %d remain, want 1", got)
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
