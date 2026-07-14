package pubsub

import (
	"testing"

	"github.com/libp2p/go-libp2p-pubsub/internal/topicstreams"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"google.golang.org/protobuf/proto"
)

func TestTopicStreamsConfig(t *testing.T) {
	defaults := DefaultTopicStreamsConfig()
	if defaults.MaxInboundStreamsPerTopic != 3 || defaults.MaxInboundStreamsPerPeer != 256 {
		t.Fatalf("unexpected defaults: %#v", defaults)
	}
	newPS := func() *PubSub {
		return &PubSub{rt: &GossipSubRouter{extensions: newExtensionsState(PeerExtensions{}, func(peer.ID) {}, func(peer.ID, *RPC, bool) {})}}
	}
	ps := newPS()
	if err := WithTopicStreams()(ps); err != nil {
		t.Fatal(err)
	}
	if ps.topicStreamsConfig != defaults {
		t.Fatalf("default option config = %#v", ps.topicStreamsConfig)
	}
	cfg := TopicStreamsConfig{MaxInboundStreamsPerTopic: 5, MaxInboundStreamsPerPeer: 42}
	ps = newPS()
	if err := WithTopicStreams(cfg)(ps); err != nil {
		t.Fatal(err)
	}
	if ps.topicStreamsConfig != cfg {
		t.Fatalf("custom config = %#v", ps.topicStreamsConfig)
	}
	for _, invalid := range []TopicStreamsConfig{{}, {MaxInboundStreamsPerTopic: 1}, {MaxInboundStreamsPerPeer: 1}} {
		if err := WithTopicStreams(invalid)(newPS()); err == nil {
			t.Fatalf("expected invalid config %#v to fail", invalid)
		}
	}
	if err := WithTopicStreams(cfg, cfg)(newPS()); err == nil {
		t.Fatal("expected multiple configs to fail")
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
	none := peerExtensionsFromRPC(empty.ExtendRPC(&RPC{}))
	if none.TopicStreams {
		t.Fatal("expected topicStreams to be false when not advertised")
	}
}

// TestTopicScopedSignatureParity verifies that stripping and restoring the topic
// preserves the signed Message representation.
func TestTopicScopedSignatureParity(t *testing.T) {
	for _, tc := range []struct {
		name     string
		generate func() (crypto.PrivKey, error)
	}{
		{"ed25519", func() (crypto.PrivKey, error) {
			key, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 0)
			return key, err
		}},
		{"rsa", func() (crypto.PrivKey, error) {
			key, _, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
			return key, err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key, err := tc.generate()
			if err != nil {
				t.Fatal(err)
			}
			id, err := peer.IDFromPublicKey(key.GetPublic())
			if err != nil {
				t.Fatal(err)
			}
			topic := "the-topic"
			msg := &pb.Message{Data: []byte("abc"), Topic: &topic, From: []byte(id), Seqno: []byte("123")}
			if err := signMessage(id, key, msg); err != nil {
				t.Fatal(err)
			}
			if err := verifyMessageSignature(msg); err != nil {
				t.Fatalf("original signature: %v", err)
			}
			wire, err := proto.Marshal(topicstreams.ScopeMessage(msg))
			if err != nil {
				t.Fatal(err)
			}
			var decoded pb.TopicScopedMessage
			if err := proto.Unmarshal(wire, &decoded); err != nil {
				t.Fatal(err)
			}
			if err := verifyMessageSignature(topicstreams.RestoreMessage(&decoded, topic)); err != nil {
				t.Fatalf("restored signature: %v", err)
			}
		})
	}
}
