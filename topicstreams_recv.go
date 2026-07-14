package pubsub

import (
	"context"
	"sync"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-msgio"
	"google.golang.org/protobuf/proto"
)

// inboundControlState coordinates a peer's inbound topic-stream readers with
// its inbound control-stream reader. Topic messages are delivered as ordinary
// incomingKindRPC, which is only correct if the peer's control-stream hello
// (the extensions control message) is enqueued onto p.incoming first. This
// type provides that ordering, and signals when the control stream has closed
// so topic messages are dropped.
type inboundControlState struct {
	mu sync.Mutex
	// helloCh is closed once the control hello has been enqueued OR the control
	// stream has closed.
	helloCh       chan struct{}
	helloEnqueued bool
	closed        bool
}

func (p *PubSub) inboundControlStateFor(pid peer.ID) *inboundControlState {
	p.inboundControlMx.Lock()
	defer p.inboundControlMx.Unlock()
	st, ok := p.inboundControl[pid]
	if !ok {
		st = &inboundControlState{helloCh: make(chan struct{})}
		p.inboundControl[pid] = st
	}
	return st
}

// controlHelloEnqueued is called by the control-stream reader right after it
// enqueues the peer's first control RPC (the extensions hello).
func (p *PubSub) controlHelloEnqueued(pid peer.ID) {
	st := p.inboundControlStateFor(pid)
	st.mu.Lock()
	if !st.helloEnqueued {
		st.helloEnqueued = true
		close(st.helloCh)
	}
	st.mu.Unlock()
}

// controlStreamClosed is called when the peer's control-stream reader exits.
// In-flight topic readers that already hold a reference observe closed and drop.
func (p *PubSub) controlStreamClosed(pid peer.ID) {
	p.inboundControlMx.Lock()
	st, ok := p.inboundControl[pid]
	if ok {
		delete(p.inboundControl, pid)
	}
	p.inboundControlMx.Unlock()
	if !ok {
		return
	}
	st.mu.Lock()
	st.closed = true
	if !st.helloEnqueued {
		st.helloEnqueued = true
		close(st.helloCh)
	}
	st.mu.Unlock()
}

// waitForHello blocks until the control hello has been enqueued or the control
// stream has closed (or ctx is done / timeout elapses). dropped is true when
// the message should be dropped (the control stream is closed). ok is false
// when ctx was done or the timeout elapsed before the hello arrived.
func (st *inboundControlState) waitForHello(ctx context.Context, timeout time.Duration) (dropped bool, ok bool) {
	st.mu.Lock()
	if st.helloEnqueued || st.closed {
		dropped = st.closed
		st.mu.Unlock()
		return dropped, true
	}
	ch := st.helloCh
	st.mu.Unlock()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-ch:
		st.mu.Lock()
		dropped = st.closed
		st.mu.Unlock()
		return dropped, true
	case <-timer.C:
		return false, false
	case <-ctx.Done():
		return false, false
	}
}

// dropInboundControlStateIfUnused removes pid's inboundControlState if it is
// still st and no control stream ever touched it (no hello enqueued, not
// closed). Called by topic-stream readers that gave up waiting for the hello,
// so peers that never open a control stream cannot leak state entries (only
// controlStreamClosed deletes entries otherwise).
func (p *PubSub) dropInboundControlStateIfUnused(pid peer.ID, st *inboundControlState) {
	p.inboundControlMx.Lock()
	defer p.inboundControlMx.Unlock()
	st.mu.Lock()
	unused := !st.helloEnqueued && !st.closed
	st.mu.Unlock()
	if unused && p.inboundControl[pid] == st {
		delete(p.inboundControl, pid)
	}
}

func (st *inboundControlState) isClosed() bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.closed
}

// handleNewTopicStream reads an inbound topic stream (TopicStreamsProtocolID).
// It reads the TopicRPCHeader, then each TopicRPC, reconstructs an ordinary RPC
// (full Message with topic / Partial with topicID), and pushes it onto
// p.incoming as an incomingKindRPC. The control stream's hello is guaranteed to
// be enqueued first (see inboundControlState).
//
// This handler deliberately does NOT emit incomingKindNewStream /
// incomingKindClosedStream: those carry control-stream semantics (they would
// clear the peer's subscription/extension state). Topic-stream lifecycle is
// handled entirely here.
func (p *PubSub) handleNewTopicStream(s network.Stream) {
	pid := s.Conn().RemotePeer()
	defer s.Reset()

	r := msgio.NewVarintReaderSize(s, p.maxMessageSize)

	// The initiator MUST send a single TopicRPCHeader first. Bound the wait so
	// header-less streams (which are not yet counted against any limit) cannot
	// pin a goroutine and stream indefinitely.
	_ = s.SetReadDeadline(time.Now().Add(topicStreamHeaderTimeout))
	hdrBytes, err := r.ReadMsg()
	if err != nil {
		r.ReleaseMsg(hdrBytes)
		return
	}
	_ = s.SetReadDeadline(time.Time{})
	var hdr pb.TopicRPCHeader
	err = proto.Unmarshal(hdrBytes, &hdr)
	r.ReleaseMsg(hdrBytes)
	if err != nil {
		p.logger.Debug("bogus topic stream header", "peer", pid, "err", err)
		p.abortTopicStreamsConnection(s.Conn(), "malformed topic stream header")
		return
	}
	topic := hdr.GetTopic()
	if topic == "" {
		p.abortTopicStreamsConnection(s.Conn(), "topic stream header omitted topic")
		return
	}

	// Enforce the per-(peer, topic) concurrency limit.
	if !p.acquireInboundTopicStream(pid, topic) {
		p.logger.Debug("too many concurrent topic streams for topic; resetting", "peer", pid, "topic", topic)
		p.penalizePeer(pid, tooManyTopicStreamsPenalty)
		return
	}
	defer p.releaseInboundTopicStream(pid, topic)

	ctrl := p.inboundControlStateFor(pid)
	helloSeen := false

	for {
		msgbytes, err := r.ReadMsg()
		if err != nil {
			r.ReleaseMsg(msgbytes)
			return
		}
		if len(msgbytes) == 0 {
			r.ReleaseMsg(msgbytes)
			p.abortTopicStreamsConnection(s.Conn(), "empty topic RPC")
			return
		}
		var tr pb.TopicRPC
		err = proto.Unmarshal(msgbytes, &tr)
		r.ReleaseMsg(msgbytes)
		if err != nil {
			p.logger.Debug("bogus topic rpc", "peer", pid, "err", err)
			p.abortTopicStreamsConnection(s.Conn(), "malformed topic RPC")
			return
		}

		rpc := topicRPCToRPC(&tr, topic, pid)
		if rpc == nil {
			p.abortTopicStreamsConnection(s.Conn(), "topic RPC omitted payload")
			return
		}

		// Ordering gate: hold this one frame until the control hello has been
		// enqueued (buffering at most one frame, since we do not read the next
		// frame while blocked here). Drop everything once the control stream is
		// closed.
		if !helloSeen {
			dropped, ok := ctrl.waitForHello(p.ctx, topicStreamHelloTimeout)
			if !ok {
				// Timed out (or shutting down) without ever seeing a control
				// stream from this peer; drop the coordination state we may
				// have created so it cannot accumulate.
				p.dropInboundControlStateIfUnused(pid, ctrl)
				return
			}
			if dropped {
				return
			}
			helloSeen = true
		} else if ctrl.isClosed() {
			return
		}

		select {
		case p.incoming <- incomingUnion{kind: incomingKindRPC, rpc: rpc}:
		case <-p.ctx.Done():
			return
		}
	}
}

// topicRPCToRPC reconstructs an ordinary RPC from a TopicRPC received on a topic
// stream scoped to topic. Returns nil if there is nothing to deliver.
func topicRPCToRPC(tr *pb.TopicRPC, topic string, from peer.ID) *RPC {
	rpc := &RPC{}
	rpc.from = from
	rpc.viaTopicStream = true
	if pub := tr.GetPublish(); pub != nil {
		rpc.Publish = []*pb.Message{topicScopedToMessage(pub, topic)}
	}
	if partial := tr.GetPartial(); partial != nil {
		// The topicID is omitted on the wire; repopulate it from the header.
		partial.TopicID = proto.String(topic)
		rpc.Partial = partial
	}
	if rpc.Publish == nil && rpc.Partial == nil {
		return nil
	}
	return rpc
}
