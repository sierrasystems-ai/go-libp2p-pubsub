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

func countStreamProto(h host.Host, pid peer.ID, proto protocol.ID) int {
	n := 0
	for _, c := range h.Network().ConnsToPeer(pid) {
		for _, s := range c.GetStreams() {
			if s.Protocol() == proto {
				n++
			}
		}
	}
	return n
}

func hasStreamProto(h host.Host, pid peer.ID, proto protocol.ID) bool {
	return countStreamProto(h, pid, proto) > 0
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

// TestTopicStreamsMultipleTopics verifies that publishing on several topics
// opens an independent topic stream per topic and delivers all messages.
func TestTopicStreamsMultipleTopics(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		hosts := getDefaultHosts(t, 2)
		psubs := getGossipsubs(ctx, hosts, WithTopicStreams())

		topics := []string{"alpha", "beta", "gamma"}
		recvSubs := make(map[string]*Subscription)
		for _, topic := range topics {
			for i, ps := range psubs {
				sub, err := ps.Subscribe(topic)
				if err != nil {
					t.Fatal(err)
				}
				if i == 1 {
					recvSubs[topic] = sub
				}
			}
		}

		connect(t, hosts[0], hosts[1])
		time.Sleep(2 * time.Second)

		for _, topic := range topics {
			if err := psubs[0].Publish(topic, []byte("msg-"+topic)); err != nil {
				t.Fatal(err)
			}
		}

		for _, topic := range topics {
			got, err := recvSubs[topic].Next(ctx)
			if err != nil {
				t.Fatalf("topic %s: %v", topic, err)
			}
			if string(got.Data) != "msg-"+topic {
				t.Fatalf("topic %s: wrong message %q", topic, got.Data)
			}
		}

		// One independent topic stream per topic.
		if n := countStreamProto(hosts[0], hosts[1].ID(), TopicStreamsProtocolID); n < len(topics) {
			t.Fatalf("expected at least %d topic streams, got %d", len(topics), n)
		}
	})
}

// TestTopicStreamsBackwardCompat verifies that when only one side enables the
// extension, messages are still delivered over the control stream and no topic
// streams are opened.
func TestTopicStreamsBackwardCompat(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		hosts := getDefaultHosts(t, 2)
		// Only host[0] enables the Topic Streams extension.
		psub0 := getGossipsub(ctx, hosts[0], WithTopicStreams())
		psub1 := getGossipsub(ctx, hosts[1])
		psubs := []*PubSub{psub0, psub1}

		var subs []*Subscription
		for _, ps := range psubs {
			sub, err := ps.Subscribe("foobar")
			if err != nil {
				t.Fatal(err)
			}
			subs = append(subs, sub)
		}

		connect(t, hosts[0], hosts[1])
		time.Sleep(2 * time.Second)

		// Publish from each side.
		if err := psubs[0].Publish("foobar", []byte("from-0")); err != nil {
			t.Fatal(err)
		}
		if err := psubs[1].Publish("foobar", []byte("from-1")); err != nil {
			t.Fatal(err)
		}

		// Both messages should be delivered to both subscribers.
		seen := map[string]int{}
		for range subs {
			for j := 0; j < 2; j++ {
				got, err := subs[0].Next(ctx)
				if err != nil {
					t.Fatal(err)
				}
				seen[string(got.Data)]++
				_ = j
			}
			break
		}
		// Drain receiver too.
		for j := 0; j < 2; j++ {
			got, err := subs[1].Next(ctx)
			if err != nil {
				t.Fatal(err)
			}
			seen[string(got.Data)]++
		}
		if seen["from-0"] == 0 || seen["from-1"] == 0 {
			t.Fatalf("expected both messages delivered, saw %v", seen)
		}

		// No topic streams should be opened since negotiation did not succeed.
		if hasStreamProto(hosts[0], hosts[1].ID(), TopicStreamsProtocolID) {
			t.Fatal("did not expect any /gsts/v0beta stream without mutual negotiation")
		}
	})
}
