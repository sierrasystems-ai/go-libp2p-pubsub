package pubsub

import (
	"bytes"
	"testing"

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
