package pubsub

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-pubsub/partialmessages"
	"github.com/libp2p/go-libp2p-pubsub/partialmessages/bitmap"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"google.golang.org/protobuf/proto"
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

func waitForDisconnected(t *testing.T, h host.Host, pid peer.ID) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if h.Network().Connectedness(pid) == network.NotConnected {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("peer %s remained connected after protocol violation", pid)
}

func waitForTopicStreamsEnabled(t *testing.T, ps *PubSub, pid peer.ID) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		enabled := make(chan bool, 1)
		select {
		case ps.eval <- func() {
			transport := ps.peerTransports[pid]
			enabled <- transport != nil && transport.topicStreamsEnabled.Load()
		}:
		case <-ps.ctx.Done():
			t.Fatal(ps.ctx.Err())
		}
		if <-enabled {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("topic streams were not negotiated with peer %s", pid)
}

func TestTopicStreamProtocolViolationsCloseConnection(t *testing.T) {
	tests := []struct {
		name string
		send func(*PubSub, network.Stream) error
	}{
		{
			name: "missing topic",
			send: func(ps *PubSub, s network.Stream) error {
				return ps.writeProtoFrame(s, &pb.TopicRPCHeader{})
			},
		},
		{
			name: "empty RPC",
			send: func(ps *PubSub, s network.Stream) error {
				if err := ps.writeProtoFrame(s, &pb.TopicRPCHeader{Topic: proto.String("topic")}); err != nil {
					return err
				}
				return ps.writeProtoFrame(s, &pb.TopicRPC{})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			hosts := getDefaultHosts(t, 2)
			psubs := getGossipsubs(ctx, hosts, WithTopicStreams())
			connect(t, hosts[0], hosts[1])

			s, err := hosts[0].NewStream(ctx, hosts[1].ID(), TopicStreamsProtocolID)
			if err != nil {
				t.Fatal(err)
			}
			if err := tc.send(psubs[0], s); err != nil {
				t.Fatal(err)
			}

			waitForDisconnected(t, hosts[0], hosts[1].ID())
		})
	}
}

func TestTopicStreamsRejectControlStreamPayloads(t *testing.T) {
	tests := []struct {
		name    string
		payload func(*pb.RPC)
	}{
		{
			name: "publish",
			payload: func(rpc *pb.RPC) {
				rpc.Publish = []*pb.Message{{Topic: proto.String("topic"), Data: []byte("payload")}}
			},
		},
		{
			name: "partial",
			payload: func(rpc *pb.RPC) {
				rpc.Partial = &pb.PartialMessagesExtension{TopicID: proto.String("topic")}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			hosts := getDefaultHosts(t, 2)
			psubs := getGossipsubs(ctx, hosts, WithTopicStreams())
			connect(t, hosts[0], hosts[1])
			waitForTopicStreamsEnabled(t, psubs[1], hosts[0].ID())

			s, err := hosts[0].NewStream(ctx, hosts[1].ID(), GossipSubID_v13)
			if err != nil {
				t.Fatal(err)
			}
			rpc := &pb.RPC{
				Control: &pb.ControlMessage{
					Extensions: &pb.ControlExtensions{TopicStreams: proto.Bool(true)},
				},
			}
			tc.payload(rpc)
			if err := psubs[0].writeProtoFrame(s, rpc); err != nil {
				t.Fatal(err)
			}

			waitForDisconnected(t, hosts[0], hosts[1].ID())
		})
	}
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

// TestPartialMessagesOverTopicStreams verifies that when both the Partial
// Messages and Topic Streams extensions are negotiated, partial messages are
// exchanged over topic streams and still reconstruct fully.
func TestPartialMessagesOverTopicStreams(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		topic := "test-topic"
		const hostCount = 2
		hosts := getDefaultHosts(t, hostCount)
		psubs := make([]*PubSub, 0, len(hosts))

		gossipsubCtx, closeGossipsub := context.WithCancel(context.Background())
		go func() {
			<-gossipsubCtx.Done()
			for _, h := range hosts {
				h.Close()
			}
		}()

		partialExt := make([]*partialmessages.PartialMessagesExtension[peerState], hostCount)
		logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

		partialMessageStore := make([]map[string]*minimalTestPartialMessage, hostCount)
		for i := range hostCount {
			partialMessageStore[i] = make(map[string]*minimalTestPartialMessage)
		}

		for i := range partialExt {
			partialExt[i] = &partialmessages.PartialMessagesExtension[peerState]{
				Logger: logger.With("id", i),
				OnEmitGossip: func(topic string, groupID []byte, gossipPeers []peer.ID, peerStates map[peer.ID]peerState) {
					pm := partialMessageStore[i][topic+string(groupID)]
					if pm == nil {
						return
					}
					partialExt[i].PublishPartial(topic, groupID, pm.publishActions)
				},
				OnIncomingRPC: func(from peer.ID, peerStates map[peer.ID]peerState, rpc *pb.PartialMessagesExtension) error {
					peerState := peerStates[from]
					groupID := rpc.GroupID
					pm, ok := partialMessageStore[i][topic+string(groupID)]
					if !ok {
						pm = &minimalTestPartialMessage{Group: groupID}
						partialMessageStore[i][topic+string(groupID)] = pm
					}
					if rpc.PartsMetadata != nil {
						peerState.recvd = bitmap.Merge(peerState.recvd, rpc.PartsMetadata)
					}
					prevMeta := slices.Clone(pm.PartsMetadata())
					shouldRepublish := pm.onIncomingRPC(from, rpc)
					if !bytes.Equal(prevMeta, pm.PartsMetadata()) {
						peerState.sent = bitmap.Merge(peerState.sent, pm.PartsMetadata())
					}
					peerStates[from] = peerState
					if shouldRepublish {
						go PublishPartial(psubs[i], topic, pm.GroupID(), pm.publishActions)
					}
					return nil
				},
			}
		}

		for i, h := range hosts {
			psub := getGossipsub(gossipsubCtx, h,
				WithPartialMessagesExtension(partialExt[i]),
				WithTopicStreams(),
			)
			tp, err := psub.Join(topic, RequestPartialMessages())
			if err != nil {
				t.Fatal(err)
			}
			if _, err = tp.Subscribe(); err != nil {
				t.Fatal(err)
			}
			psubs = append(psubs, psub)
		}

		connect(t, hosts[0], hosts[1])
		time.Sleep(2 * time.Second)

		group := []byte("test-group")
		msg1 := &minimalTestPartialMessage{
			Group: group,
			Parts: [2][]byte{[]byte("Hello"), []byte("World")},
		}
		partialMessageStore[0][topic+string(group)] = msg1
		if err := PublishPartial(psubs[0], topic, msg1.GroupID(), msg1.publishActions); err != nil {
			t.Fatal(err)
		}

		time.Sleep(2 * time.Second)
		closeGossipsub()
		time.Sleep(time.Second)

		for i, msgStore := range partialMessageStore {
			if len(msgStore) == 0 {
				t.Errorf("Host %d is missing the partial message", i)
			}
			for _, pm := range msgStore {
				if !pm.complete() {
					t.Errorf("host %d: expected complete message, but %v is incomplete", i, pm)
				}
			}
		}
	})
}

// TestTopicStreamsCloseOnUnsubscribe verifies that a publisher closes its
// outbound topic stream when it unsubscribes from the topic, per the spec's
// stream lifecycle.
func TestTopicStreamsCloseOnUnsubscribe(t *testing.T) {
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
		time.Sleep(2 * time.Second)

		if err := psubs[0].Publish("foobar", []byte("hello")); err != nil {
			t.Fatal(err)
		}
		for i, sub := range subs {
			if _, err := sub.Next(ctx); err != nil {
				t.Fatalf("sub %d: %v", i, err)
			}
		}

		if !hasStreamProto(hosts[0], hosts[1].ID(), TopicStreamsProtocolID) {
			t.Fatal("expected a topic stream after publishing")
		}

		// Unsubscribing must close the publisher's topic stream.
		subs[0].Cancel()
		time.Sleep(2 * time.Second)

		if hasStreamProto(hosts[0], hosts[1].ID(), TopicStreamsProtocolID) {
			t.Fatal("expected the topic stream to be closed after unsubscribe")
		}
	})
}
