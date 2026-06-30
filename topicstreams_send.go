package pubsub

import (
	"context"
	"encoding/binary"
	"time"

	pool "github.com/libp2p/go-buffer-pool"
	"github.com/multiformats/go-varint"
	"google.golang.org/protobuf/proto"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

// topicStreamSet owns the outbound topic streams for a single peer. It is
// created and used exclusively by that peer's writer goroutine
// (handleSendingMessages), so it needs no locking. Each topic gets its own
// stream and writer goroutine, so a slow/large message on one topic does not
// head-of-line block other topics.
type topicStreamSet struct {
	p       *PubSub
	pid     peer.ID
	ctx     context.Context
	cancel  context.CancelFunc
	streams map[string]*outboundTopicStream
}

func newTopicStreamSet(p *PubSub, ctx context.Context, pid peer.ID) *topicStreamSet {
	ctx, cancel := context.WithCancel(ctx)
	return &topicStreamSet{
		p:       p,
		pid:     pid,
		ctx:     ctx,
		cancel:  cancel,
		streams: make(map[string]*outboundTopicStream),
	}
}

// send routes a TopicRPC to the topic's stream, opening it lazily. The first
// frame on a freshly opened stream is the TopicRPCHeader, written by the
// stream's own goroutine.
func (tss *topicStreamSet) send(topic string, tr *pb.TopicRPC) {
	ots, ok := tss.streams[topic]
	if !ok {
		ots = tss.p.newOutboundTopicStream(tss.ctx, tss.pid, topic)
		tss.streams[topic] = ots
	}
	ots.enqueue(tr)
}

// closeTopic closes a single topic stream (e.g. on unsubscribe / leave).
func (tss *topicStreamSet) closeTopic(topic string) {
	if ots, ok := tss.streams[topic]; ok {
		ots.cancel()
		delete(tss.streams, topic)
	}
}

// close tears down every topic stream for the peer.
func (tss *topicStreamSet) close() {
	tss.cancel()
	tss.streams = nil
}

type outboundTopicStream struct {
	topic  string
	ch     chan *pb.TopicRPC
	cancel context.CancelFunc
}

func (ots *outboundTopicStream) enqueue(tr *pb.TopicRPC) {
	// Non-blocking: dropping on a full per-topic queue mirrors the drop-on-full
	// behavior of the main rpcQueue and avoids head-of-line blocking the
	// dispatcher.
	select {
	case ots.ch <- tr:
	default:
	}
}

func (p *PubSub) newOutboundTopicStream(ctx context.Context, pid peer.ID, topic string) *outboundTopicStream {
	streamCtx, cancel := context.WithCancel(ctx)
	ots := &outboundTopicStream{
		topic:  topic,
		ch:     make(chan *pb.TopicRPC, p.peerOutboundQueueSize),
		cancel: cancel,
	}
	go p.runOutboundTopicStream(streamCtx, pid, topic, ots.ch)
	return ots
}

func (p *PubSub) runOutboundTopicStream(ctx context.Context, pid peer.ID, topic string, ch chan *pb.TopicRPC) {
	s, err := p.host.NewStream(ctx, pid, TopicStreamsProtocolID)
	if err != nil {
		p.logger.Debug("failed to open topic stream", "peer", pid, "topic", topic, "err", err)
		return
	}
	defer s.Reset()

	// The topic stream is treated as unidirectional: the responder MUST NOT
	// write. Watch for any data from the peer and treat it as a protocol
	// violation.
	go p.guardTopicStreamResponder(s)

	// The initiator MUST send a single TopicRPCHeader before any TopicRPC.
	if err := p.writeProtoFrame(s, &pb.TopicRPCHeader{Topic: &topic}); err != nil {
		p.logger.Debug("failed to write topic stream header", "peer", pid, "topic", topic, "err", err)
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case tr := <-ch:
			if err := p.writeProtoFrame(s, tr); err != nil {
				p.logger.Debug("failed to write topic rpc", "peer", pid, "topic", topic, "err", err)
				return
			}
		}
	}
}

// guardTopicStreamResponder enforces the rule that the responder of a topic
// stream MUST NOT write. Any byte read is a protocol violation; we reset the
// stream.
func (p *PubSub) guardTopicStreamResponder(s network.Stream) {
	var b [1]byte
	_, err := s.Read(b[:])
	if err == nil {
		p.logger.Warn("peer wrote on a topic stream it should only read; resetting", "peer", s.Conn().RemotePeer())
	}
	s.Reset()
}

// writeProtoFrame length-prefixes and writes a single protobuf message to the
// stream with a write deadline, mirroring the control-stream writer.
func (p *PubSub) writeProtoFrame(s network.Stream, m proto.Message) error {
	size := uint64(proto.Size(m))

	buf := pool.Get(varint.UvarintSize(size) + int(size))
	defer pool.Put(buf)

	n := binary.PutUvarint(buf, size)
	out, err := proto.MarshalOptions{}.MarshalAppend(buf[:n], m)
	if err != nil {
		return err
	}

	if err := s.SetWriteDeadline(time.Now().Add(time.Second * 30)); err != nil {
		return err
	}

	_, err = s.Write(out)
	return err
}

// sendRPCOverTopicStreams splits the topic-scoped parts of an RPC (publish
// messages and partial-message extension data) onto the appropriate per-topic
// streams. The non-topic parts (subscriptions, control) are handled separately
// by the caller on the control stream.
func (p *PubSub) sendRPCOverTopicStreams(rpc *RPC, tss *topicStreamSet) {
	for _, msg := range rpc.GetPublish() {
		tss.send(msg.GetTopic(), &pb.TopicRPC{Publish: messageToTopicScoped(msg)})
	}
	if rpc.Partial != nil {
		// The topicID is carried by the TopicRPCHeader and MUST be omitted on
		// the wire here.
		partial := proto.CloneOf(rpc.Partial)
		topic := partial.GetTopicID()
		partial.TopicID = nil
		tss.send(topic, &pb.TopicRPC{Partial: partial})
	}
}

// topicStreamControlRemainder returns an RPC containing only the parts of rpc
// that belong on the control stream (everything except topic-scoped publish and
// partial data), or nil if there is nothing to send.
func topicStreamControlRemainder(rpc *RPC) *RPC {
	if len(rpc.Subscriptions) == 0 && rpc.Control == nil && rpc.TestExtension == nil {
		return nil
	}
	return &RPC{RPC: pb.RPC{
		Subscriptions: rpc.Subscriptions,
		Control:       rpc.Control,
		TestExtension: rpc.TestExtension,
	}}
}
