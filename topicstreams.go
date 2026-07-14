package pubsub

import (
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"google.golang.org/protobuf/proto"
)

// TopicStreamsProtocolID is the protocol ID used for topic streams. See the
// Topic Streams extension spec (topic-streams.md).
const TopicStreamsProtocolID = protocol.ID("/gsts/v0beta")

const topicStreamsProtocolViolationError = network.ConnErrorCode(0xd52505)

const (
	// maxConcurrentInboundTopicStreamsPerTopic is the number of concurrent
	// inbound topic streams a peer may have open for a single topic before we
	// downscore and reject further ones (topic-streams.md).
	maxConcurrentInboundTopicStreamsPerTopic = 3

	// maxConcurrentInboundTopicStreamsPerPeer bounds the total number of
	// concurrent inbound topic streams a single peer may have open across all
	// topics. The per-topic limit alone would let a peer hold unbounded
	// streams (and reader goroutines) by inventing topic names.
	maxConcurrentInboundTopicStreamsPerPeer = 256

	// topicStreamHeaderTimeout is how long we wait for the initiator's
	// TopicRPCHeader before resetting the stream. The spec suggests one second
	// as a reasonable timeout.
	topicStreamHeaderTimeout = time.Second

	// topicStreamHelloTimeout bounds how long an inbound topic-stream reader
	// waits for the peer's control-stream hello before giving up and resetting
	// the stream. An honest peer only opens topic streams after extension
	// negotiation, so its hello is already in flight; the generous bound
	// tolerates processLoop backpressure while preventing readers from being
	// parked forever by peers that never open a control stream.
	topicStreamHelloTimeout = 30 * time.Second

	// recentlyUnsubscribedTTL is how long we remember a topic we unsubscribed
	// from, during which we do not penalize peers for sending us messages on it.
	recentlyUnsubscribedTTL = 10 * time.Second

	// unsubscribedTopicPenalty is the behavior penalty applied to a peer that
	// sends us a message for a topic we are not subscribed to.
	unsubscribedTopicPenalty = 1

	// tooManyTopicStreamsPenalty is the behavior penalty applied to a peer that
	// opens more than the allowed number of concurrent topic streams per topic.
	tooManyTopicStreamsPenalty = 1
)

func (p *PubSub) abortTopicStreamsConnection(conn network.Conn, reason string) {
	p.logger.Warn("topic streams protocol violation; closing connection", "peer", conn.RemotePeer(), "reason", reason)
	if err := conn.CloseWithError(topicStreamsProtocolViolationError); err != nil {
		p.logger.Debug("failed to close connection after topic streams protocol violation", "peer", conn.RemotePeer(), "err", err)
	}
}

// penalizePeer applies a behavior penalty to a peer via the gossipsub scoring
// subsystem. It is a no-op when the router is not gossipsub or scoring is
// disabled. Safe to call from any goroutine.
func (p *PubSub) penalizePeer(pid peer.ID, count int) {
	if gs, ok := p.rt.(*GossipSubRouter); ok {
		gs.score.AddPenalty(pid, count)
	}
}

// recentlyUnsubscribedFrom reports whether we unsubscribed from topic within
// the grace window. Only called from the processLoop goroutine.
func (p *PubSub) recentlyUnsubscribedFrom(topic string) bool {
	t, ok := p.recentlyUnsubscribed[topic]
	if !ok {
		return false
	}
	if time.Since(t) > recentlyUnsubscribedTTL {
		delete(p.recentlyUnsubscribed, topic)
		return false
	}
	return true
}

// acquireInboundTopicStream reserves a slot for a new inbound topic stream from
// pid on topic, returning false if the per-topic or per-peer concurrency limit
// is exceeded.
func (p *PubSub) acquireInboundTopicStream(pid peer.ID, topic string) bool {
	p.topicStreamCountMx.Lock()
	defer p.topicStreamCountMx.Unlock()
	if p.inboundTopicStreams == nil {
		return true
	}
	m := p.inboundTopicStreams[pid]
	if m == nil {
		m = make(map[string]int)
		p.inboundTopicStreams[pid] = m
	}
	if m[topic] >= maxConcurrentInboundTopicStreamsPerTopic {
		return false
	}
	total := 0
	for _, n := range m {
		total += n
	}
	if total >= maxConcurrentInboundTopicStreamsPerPeer {
		return false
	}
	m[topic]++
	return true
}

// releaseInboundTopicStream releases a slot acquired by acquireInboundTopicStream.
func (p *PubSub) releaseInboundTopicStream(pid peer.ID, topic string) {
	p.topicStreamCountMx.Lock()
	defer p.topicStreamCountMx.Unlock()
	if p.inboundTopicStreams == nil {
		return
	}
	m := p.inboundTopicStreams[pid]
	if m == nil {
		return
	}
	m[topic]--
	if m[topic] <= 0 {
		delete(m, topic)
		if len(m) == 0 {
			delete(p.inboundTopicStreams, pid)
		}
	}
}

// messageToTopicScoped converts a full Message into a TopicScopedMessage for
// sending on a topic stream. The topic is carried once in the TopicRPCHeader,
// so it is omitted here (the unset_topic_name field is left unset on the wire).
func messageToTopicScoped(msg *pb.Message) *pb.TopicScopedMessage {
	if msg == nil {
		return nil
	}
	ts := &pb.TopicScopedMessage{
		Data:      msg.Data,
		Seqno:     msg.Seqno,
		Signature: msg.Signature,
		Key:       msg.Key,
	}
	if msg.From != nil {
		// Message.from is bytes; TopicScopedMessage.from is string. They share
		// the same protobuf wire encoding, so this preserves the bytes used to
		// compute the signature.
		ts.From = proto.String(string(msg.From))
	}
	return ts
}

// topicScopedToMessage reconstructs the full Message from a TopicScopedMessage
// received on a topic stream, setting the topic from the stream's
// TopicRPCHeader so the message ID can be computed and the signature verified.
func topicScopedToMessage(ts *pb.TopicScopedMessage, topic string) *pb.Message {
	if ts == nil {
		return nil
	}
	msg := &pb.Message{
		Data:      ts.Data,
		Seqno:     ts.Seqno,
		Signature: ts.Signature,
		Key:       ts.Key,
		Topic:     proto.String(topic),
	}
	if ts.From != nil {
		msg.From = []byte(ts.GetFrom())
	}
	return msg
}
