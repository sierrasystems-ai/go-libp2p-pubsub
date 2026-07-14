package pubsub

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"google.golang.org/protobuf/proto"
)

func TestTopicScopedRoundTrip(t *testing.T) {
	topic := "foo"
	orig := &pb.Message{
		From:      []byte("some-from"),
		Data:      []byte("hello world"),
		Seqno:     []byte("123"),
		Topic:     proto.String(topic),
		Signature: []byte("sig"),
		Key:       []byte("key"),
	}

	ts := messageToTopicScoped(orig)

	// The topic name MUST NOT be set on the wire.
	if ts.UnsetTopicName != nil {
		t.Fatalf("expected unset_topic_name to be nil on the wire, got %q", ts.GetUnsetTopicName())
	}

	got := topicScopedToMessage(ts, topic)

	if got.GetTopic() != topic {
		t.Fatalf("topic not restored: got %q want %q", got.GetTopic(), topic)
	}
	if !bytes.Equal(got.From, orig.From) {
		t.Fatalf("from mismatch: got %x want %x", got.From, orig.From)
	}
	if !bytes.Equal(got.Data, orig.Data) {
		t.Fatalf("data mismatch")
	}
	if !bytes.Equal(got.Seqno, orig.Seqno) {
		t.Fatalf("seqno mismatch")
	}
	if !bytes.Equal(got.Signature, orig.Signature) {
		t.Fatalf("signature mismatch")
	}
	if !bytes.Equal(got.Key, orig.Key) {
		t.Fatalf("key mismatch")
	}
}

func TestTopicStreamControlRemainder(t *testing.T) {
	// Publish-only: nothing belongs on the control stream.
	pubOnly := &RPC{RPC: pb.RPC{Publish: []*pb.Message{{Topic: proto.String("t")}}}}
	if rem := topicStreamControlRemainder(pubOnly); rem != nil {
		t.Fatal("publish-only RPC should have no control-stream remainder")
	}

	// Publish + control: the control parts stay, the publish does not.
	withCtrl := &RPC{RPC: pb.RPC{
		Publish: []*pb.Message{{Topic: proto.String("t")}},
		Control: &pb.ControlMessage{},
	}}
	rem := topicStreamControlRemainder(withCtrl)
	if rem == nil || rem.Control == nil {
		t.Fatal("expected control to remain on the control stream")
	}
	if len(rem.Publish) != 0 {
		t.Fatal("publish must not be on the control stream")
	}
}

func TestAcquireInboundTopicStreamLimit(t *testing.T) {
	p := &PubSub{inboundTopicStreams: make(map[peer.ID]map[string]int)}
	pid := peer.ID("peer")
	topic := "t"

	for i := 0; i < maxConcurrentInboundTopicStreamsPerTopic; i++ {
		if !p.acquireInboundTopicStream(pid, topic) {
			t.Fatalf("acquire %d should succeed", i)
		}
	}
	if p.acquireInboundTopicStream(pid, topic) {
		t.Fatal("acquiring beyond the limit should fail")
	}
	// A different topic is independent.
	if !p.acquireInboundTopicStream(pid, "other") {
		t.Fatal("different topic should be allowed")
	}
	// Releasing frees a slot.
	p.releaseInboundTopicStream(pid, topic)
	if !p.acquireInboundTopicStream(pid, topic) {
		t.Fatal("acquire after release should succeed")
	}
}

func TestAcquireInboundTopicStreamPerPeerCap(t *testing.T) {
	p := &PubSub{inboundTopicStreams: make(map[peer.ID]map[string]int)}
	pid := peer.ID("peer")

	for i := 0; i < maxConcurrentInboundTopicStreamsPerPeer; i++ {
		if !p.acquireInboundTopicStream(pid, fmt.Sprintf("t%d", i)) {
			t.Fatalf("acquire %d should succeed", i)
		}
	}
	if p.acquireInboundTopicStream(pid, "one-more") {
		t.Fatal("acquiring beyond the per-peer cap should fail")
	}
	// Another peer is unaffected.
	if !p.acquireInboundTopicStream(peer.ID("other"), "t0") {
		t.Fatal("other peer should be allowed")
	}
	// Releasing a slot frees capacity for the capped peer.
	p.releaseInboundTopicStream(pid, "t0")
	if !p.acquireInboundTopicStream(pid, "one-more") {
		t.Fatal("acquire after release should succeed")
	}
}

func TestInboundControlHelloTimeout(t *testing.T) {
	p := &PubSub{inboundControl: make(map[peer.ID]*inboundControlState)}
	pid := peer.ID("peer")
	st := p.inboundControlStateFor(pid)

	if _, ok := st.waitForHello(context.Background(), 10*time.Millisecond); ok {
		t.Fatal("expected waitForHello to time out")
	}

	// A state never touched by a control stream is dropped, so peers that
	// never open a control stream cannot leak entries.
	p.dropInboundControlStateIfUnused(pid, st)
	p.inboundControlMx.Lock()
	_, exists := p.inboundControl[pid]
	p.inboundControlMx.Unlock()
	if exists {
		t.Fatal("expected unused inbound control state to be dropped")
	}

	// A state the control stream has touched must not be dropped.
	st2 := p.inboundControlStateFor(pid)
	p.controlHelloEnqueued(pid)
	p.dropInboundControlStateIfUnused(pid, st2)
	p.inboundControlMx.Lock()
	_, exists = p.inboundControl[pid]
	p.inboundControlMx.Unlock()
	if !exists {
		t.Fatal("state with hello enqueued must not be dropped")
	}
}

func TestInboundControlGatingDropAfterClose(t *testing.T) {
	p := &PubSub{inboundControl: make(map[peer.ID]*inboundControlState)}
	pid := peer.ID("peer")

	st := p.inboundControlStateFor(pid)
	p.controlStreamClosed(pid)

	dropped, ok := st.waitForHello(context.Background(), time.Minute)
	if !ok {
		t.Fatal("waitForHello should not have been cancelled")
	}
	if !dropped {
		t.Fatal("expected drop after the control stream closed")
	}
}

func TestInboundControlGatingHelloFirst(t *testing.T) {
	p := &PubSub{inboundControl: make(map[peer.ID]*inboundControlState)}
	pid := peer.ID("peer")

	st := p.inboundControlStateFor(pid)

	type result struct {
		dropped bool
		ok      bool
	}
	resCh := make(chan result, 1)
	go func() {
		dropped, ok := st.waitForHello(context.Background(), time.Minute)
		resCh <- result{dropped, ok}
	}()

	// The waiter must still be blocked before the hello is enqueued.
	select {
	case <-resCh:
		t.Fatal("waitForHello returned before the control hello was enqueued")
	case <-time.After(50 * time.Millisecond):
	}

	p.controlHelloEnqueued(pid)

	select {
	case r := <-resCh:
		if !r.ok || r.dropped {
			t.Fatalf("expected proceed after hello, got dropped=%t ok=%t", r.dropped, r.ok)
		}
	case <-time.After(time.Second):
		t.Fatal("waitForHello did not return after the hello was enqueued")
	}
}

func TestTopicStreamsExtensionPlumbing(t *testing.T) {
	pe := PeerExtensions{TopicStreams: true}
	rpc := pe.ExtendRPC(&RPC{})
	if rpc.Control == nil || rpc.Control.Extensions == nil || !rpc.Control.Extensions.GetTopicStreams() {
		t.Fatal("ExtendRPC did not advertise topicStreams")
	}

	got := peerExtensionsFromRPC(rpc)
	if !got.TopicStreams {
		t.Fatal("peerExtensionsFromRPC did not read topicStreams")
	}

	// A peer that does not advertise topicStreams must read as disabled.
	empty := PeerExtensions{}
	emptyHello := empty.ExtendRPC(&RPC{})
	if !hasPeerExtensions(emptyHello) || proto.Size(&emptyHello.RPC) == 0 {
		t.Fatal("disabled extensions must produce an explicit capability hello")
	}
	none := peerExtensionsFromRPC(emptyHello)
	if none.TopicStreams {
		t.Fatal("expected topicStreams to be false when not advertised")
	}
}

// TestTopicScopedSignatureParity ensures a signed Message still verifies after
// being converted to a TopicScopedMessage (topic stripped) and reconstructed
// with the topic from the header. This is the crux of the wire design.
func TestTopicScopedSignatureParity(t *testing.T) {
	for _, tc := range []struct {
		name string
		gen  func() (crypto.PrivKey, error)
	}{
		{"ed25519", func() (crypto.PrivKey, error) {
			priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 0)
			return priv, err
		}},
		{"rsa", func() (crypto.PrivKey, error) {
			priv, _, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
			return priv, err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			privk, err := tc.gen()
			if err != nil {
				t.Fatal(err)
			}
			id, err := peer.IDFromPublicKey(privk.GetPublic())
			if err != nil {
				t.Fatal(err)
			}

			topic := "the-topic"
			msg := &pb.Message{
				Data:  []byte("abc"),
				Topic: proto.String(topic),
				From:  []byte(id),
				Seqno: []byte("123"),
			}
			if err := signMessage(id, privk, msg); err != nil {
				t.Fatal(err)
			}
			// Sanity: the original verifies.
			if err := verifyMessageSignature(msg); err != nil {
				t.Fatalf("original message failed to verify: %v", err)
			}

			// Send over the wire as a topic-scoped message (topic stripped),
			// marshal/unmarshal to emulate the wire, then reconstruct.
			ts := messageToTopicScoped(msg)
			wire, err := proto.Marshal(ts)
			if err != nil {
				t.Fatal(err)
			}
			var decoded pb.TopicScopedMessage
			if err := proto.Unmarshal(wire, &decoded); err != nil {
				t.Fatal(err)
			}

			reconstructed := topicScopedToMessage(&decoded, topic)
			if err := verifyMessageSignature(reconstructed); err != nil {
				t.Fatalf("reconstructed message failed to verify: %v", err)
			}
		})
	}
}
