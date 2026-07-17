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

	result := streams.RouteRPC(rpc, nil)
	remainder := result.Control
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
	if got := streams.RouteRPC(rpc, nil).Control; got != nil {
		t.Fatalf("expected nil remainder, got %#v", got)
	}
}

func TestRouteRPCPreservesUnknownFields(t *testing.T) {
	rpc := &pb.RPC{}
	rpc.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
	streams := &OutboundStreams{}
	remainder := streams.RouteRPC(rpc, nil).Control
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
	if got := len(w.ch); got != 0 {
		t.Fatalf("queued payload was not finalized after cancellation: %d remain, want 0", got)
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type failingHost struct {
	host.Host
	stream network.Stream
	err    error
}

func (h failingHost) NewStream(context.Context, peer.ID, ...protocol.ID) (network.Stream, error) {
	return h.stream, h.err
}

type failingStream struct {
	network.Stream
	failWrite int32
	writes    atomic.Int32
}

func (s *failingStream) Write(p []byte) (int, error) {
	if s.writes.Add(1) == s.failWrite {
		return 0, io.ErrClosedPipe
	}
	return len(p), nil
}
func (s *failingStream) Read([]byte) (int, error)         { return 0, io.EOF }
func (s *failingStream) SetWriteDeadline(time.Time) error { return nil }
func (s *failingStream) Reset() error                     { return nil }

func TestRouteRPCReportsRejectedPayload(t *testing.T) {
	var drops atomic.Int32
	streams := NewOutboundStreams(context.Background(), nil, peer.ID("peer"), 1, nil, OutboundHooks{
		Drop: func(peer.ID, Envelope) { drops.Add(1) },
	})
	result := streams.RouteRPC(&pb.RPC{Publish: []*pb.Message{{Topic: proto.String("topic")}}}, func(Envelope) bool { return false })
	if result.Accepted != 0 || result.Dropped != 1 || result.Control != nil {
		t.Fatalf("unexpected route result: %#v", result)
	}
	if drops.Load() != 1 {
		t.Fatalf("got %d drops, want 1", drops.Load())
	}
}

func TestWriterFailuresDropAcceptedEnvelopesExactlyOnce(t *testing.T) {
	tests := []struct {
		name      string
		host      host.Host
		envelopes int
	}{
		{name: "open", host: failingHost{err: io.ErrClosedPipe}, envelopes: 2},
		{name: "header", host: failingHost{stream: &failingStream{failWrite: 1}}, envelopes: 2},
		{name: "payload", host: failingHost{stream: &failingStream{failWrite: 2}}, envelopes: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			dropped := make(chan Envelope, tt.envelopes)
			streams := NewOutboundStreams(ctx, tt.host, peer.ID("peer"), tt.envelopes, nil, OutboundHooks{
				Drop: func(_ peer.ID, e Envelope) { dropped <- e },
			})
			for i := 0; i < tt.envelopes; i++ {
				if !streams.Send(Envelope{Topic: "topic", Publish: &pb.Message{Data: []byte{byte(i)}}}) {
					t.Fatalf("envelope %d was not accepted", i)
				}
			}
			seen := make(map[byte]int)
			for i := 0; i < tt.envelopes; i++ {
				select {
				case e := <-dropped:
					seen[e.Publish.Data[0]]++
				case <-time.After(time.Second):
					t.Fatalf("timed out after %d drops", i)
				}
			}
			for i := 0; i < tt.envelopes; i++ {
				if seen[byte(i)] != 1 {
					t.Fatalf("envelope %d dropped %d times", i, seen[byte(i)])
				}
			}
		})
	}
}

func TestDropCallbackCanReenterOutboundStreams(t *testing.T) {
	callbackDone := make(chan struct{})
	var streams *OutboundStreams
	streams = NewOutboundStreams(context.Background(), nil, peer.ID("peer"), 1, nil, OutboundHooks{
		Drop: func(peer.ID, Envelope) {
			streams.CloseTopic("topic")
			close(callbackDone)
		},
	})
	streams.Close()

	sendDone := make(chan bool, 1)
	go func() {
		sendDone <- streams.Send(Envelope{Topic: "topic", Publish: &pb.Message{}})
	}()
	select {
	case accepted := <-sendDone:
		if accepted {
			t.Fatal("closed streams accepted envelope")
		}
	case <-time.After(time.Second):
		t.Fatal("reentrant Drop callback deadlocked Send")
	}
	select {
	case <-callbackDone:
	case <-time.After(time.Second):
		t.Fatal("Drop callback did not complete")
	}
}
