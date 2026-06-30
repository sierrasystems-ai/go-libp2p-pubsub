package pubsub

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

func hasStreamProto(h host.Host, pid peer.ID, proto protocol.ID) bool {
	for _, c := range h.Network().ConnsToPeer(pid) {
		for _, s := range c.GetStreams() {
			if s.Protocol() == proto {
				return true
			}
		}
	}
	return false
}

// TestTopicStreamsDelivery verifies that two nodes with the Topic Streams
// extension deliver a published message over a /gsts/v0beta stream.
func TestTopicStreamsDelivery(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		hosts := getDefaultHosts(t, 2)
		psubs := getGossipsubs(ctx, hosts, WithTopicStreams())

		var subs []*Subscription
		for _, ps := range psubs {
			sub, err := ps.Subscribe("foobar")
			if err != nil {
				t.Fatal(err)
			}
			subs = append(subs, sub)
		}

		connect(t, hosts[0], hosts[1])

		// Build the mesh and complete extension negotiation.
		time.Sleep(2 * time.Second)

		msg := []byte("hello over topic streams")
		if err := psubs[0].Publish("foobar", msg); err != nil {
			t.Fatal(err)
		}

		for i, sub := range subs {
			got, err := sub.Next(ctx)
			if err != nil {
				t.Fatalf("sub %d: %v", i, err)
			}
			if !bytes.Equal(got.Data, msg) {
				t.Fatalf("sub %d: wrong message: %q", i, got.Data)
			}
			if got.GetTopic() != "foobar" {
				t.Fatalf("sub %d: topic not populated: %q", i, got.GetTopic())
			}
		}

		// The publisher should have opened a topic stream to the receiver.
		if !hasStreamProto(hosts[0], hosts[1].ID(), TopicStreamsProtocolID) {
			t.Fatal("expected a /gsts/v0beta stream from publisher to receiver")
		}
	})
}
