// Package topicstreams owns all Topic Streams wire I/O and protobuf framing.
package topicstreams

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"sync"
	"time"

	pool "github.com/libp2p/go-buffer-pool"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-msgio"
	"github.com/multiformats/go-varint"
	"google.golang.org/protobuf/proto"
)

const ProtocolID = protocol.ID("/gsts/v0beta")
const ProtocolViolationError = network.ConnErrorCode(0xd52505)
const headerTimeout = time.Second
const writeTimeout = 30 * time.Second

type ViolationError struct{ Reason string }

func (e *ViolationError) Error() string  { return "topic streams protocol violation: " + e.Reason }
func IsProtocolViolation(err error) bool { var e *ViolationError; return errors.As(err, &e) }

// Envelope is the package-owned topic-scoped payload passed across Hooks.
type Envelope struct {
	Topic   string
	Publish *pb.Message
	Partial *pb.PartialMessagesExtension
}

// InboundHooks adapt decoded envelopes without exposing wire framing.
type InboundHooks struct {
	Admit     func(network.Stream, string) bool
	Deliver   func(Envelope) bool
	Close     func()
	Logger    *slog.Logger
	Violation func(network.Conn, string)
}

func HandleInbound(s network.Stream, maxMessageSize int, h InboundHooks) {
	if h.Logger == nil {
		h.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	pid := s.Conn().RemotePeer()
	defer s.Reset()
	r := msgio.NewVarintReaderSize(s, maxMessageSize)
	_ = s.SetReadDeadline(time.Now().Add(headerTimeout))
	b, err := r.ReadMsg()
	if err != nil {
		r.ReleaseMsg(b)
		return
	}
	_ = s.SetReadDeadline(time.Time{})
	var hdr pb.TopicRPCHeader
	err = proto.Unmarshal(b, &hdr)
	r.ReleaseMsg(b)
	if err != nil {
		h.Logger.Debug("bogus topic stream header", "peer", pid, "err", err)
		h.violate(s.Conn(), "malformed topic stream header")
		return
	}
	topic := hdr.GetTopic()
	if topic == "" {
		h.violate(s.Conn(), "topic stream header omitted topic")
		return
	}
	if h.Admit == nil || h.Deliver == nil {
		return
	}
	if !h.Admit(s, topic) {
		h.Logger.Debug("too many concurrent topic streams; resetting", "peer", pid, "topic", topic)
		return
	}
	if h.Close != nil {
		defer h.Close()
	}
	for {
		b, err = r.ReadMsg()
		if err != nil {
			r.ReleaseMsg(b)
			return
		}
		if len(b) == 0 {
			r.ReleaseMsg(b)
			h.violate(s.Conn(), "empty topic RPC")
			return
		}
		var tr pb.TopicRPC
		err = proto.Unmarshal(b, &tr)
		r.ReleaseMsg(b)
		if err != nil {
			h.Logger.Debug("bogus topic rpc", "peer", pid, "topic", topic, "err", err)
			h.violate(s.Conn(), "malformed topic RPC")
			return
		}
		env, ok := RestoreEnvelope(&tr, topic)
		if !ok {
			h.violate(s.Conn(), "topic RPC omitted payload")
			return
		}
		if !h.Deliver(env) {
			return
		}
	}
}

func (h InboundHooks) violate(c network.Conn, reason string) {
	if h.Violation != nil {
		h.Violation(c, reason)
	} else if c != nil {
		_ = c.CloseWithError(ProtocolViolationError)
	}
}

// ScopeMessage removes the topic carried by the stream header.
func ScopeMessage(msg *pb.Message) *pb.TopicScopedMessage {
	if msg == nil {
		return nil
	}
	return &pb.TopicScopedMessage{Data: msg.Data, Seqno: msg.Seqno, Signature: msg.Signature, Key: msg.Key, From: msg.From}
}

// RestoreMessage restores the topic carried by the stream header.
func RestoreMessage(x *pb.TopicScopedMessage, topic string) *pb.Message {
	if x == nil {
		return nil
	}
	return &pb.Message{Data: x.Data, Seqno: x.Seqno, Signature: x.Signature, Key: x.Key, From: x.From, Topic: proto.String(topic)}
}

func RestoreEnvelope(tr *pb.TopicRPC, topic string) (Envelope, bool) {
	if x := tr.GetPublish(); x != nil {
		return Envelope{Topic: topic, Publish: RestoreMessage(x, topic)}, true
	}
	if x := tr.GetPartial(); x != nil {
		x.TopicID = proto.String(topic)
		return Envelope{Topic: topic, Partial: x}, true
	}
	return Envelope{}, false
}

func PublishEnvelope(msg *pb.Message) Envelope { return Envelope{Topic: msg.GetTopic(), Publish: msg} }
func PartialEnvelope(x *pb.PartialMessagesExtension) Envelope {
	return Envelope{Topic: x.GetTopicID(), Partial: x}
}

// RouteResult describes the disposition of one routed RPC. Every topic
// payload is either accepted by a writer queue or synchronously reported as a
// drop; Control contains the control-stream remainder.
type RouteResult struct {
	Control  *pb.RPC
	Accepted int
	Dropped  int
}

// RouteRPC sends authorized topic-scoped payloads and returns the control
// stream remainder. authorize is evaluated before Send and may reject stale
// subscription generations. Clone once because queued envelopes outlive this
// call and callers may retain rpc.
func (s *OutboundStreams) RouteRPC(rpc *pb.RPC, authorize func(Envelope) bool) RouteResult {
	var result RouteResult
	if rpc == nil {
		return result
	}
	remainder := proto.CloneOf(rpc)
	route := func(e Envelope) {
		if authorize != nil && !authorize(e) {
			s.drop(e)
			result.Dropped++
			return
		}
		if s.Send(e) {
			result.Accepted++
		} else {
			result.Dropped++
		}
	}
	for _, msg := range remainder.GetPublish() {
		route(PublishEnvelope(msg))
	}
	if partial := remainder.GetPartial(); partial != nil {
		route(PartialEnvelope(partial))
	}
	remainder.Publish = nil
	remainder.Partial = nil
	if proto.Size(remainder) != 0 {
		result.Control = remainder
	}
	return result
}

func (e Envelope) wire() *pb.TopicRPC {
	if e.Publish != nil {
		return &pb.TopicRPC{Payload: &pb.TopicRPC_Publish{Publish: ScopeMessage(e.Publish)}}
	}
	if e.Partial != nil {
		x := proto.CloneOf(e.Partial)
		x.TopicID = nil
		return &pb.TopicRPC{Payload: &pb.TopicRPC_Partial{Partial: x}}
	}
	return nil
}

// OutboundHooks contain policy seams owned by the embedding package.
type OutboundHooks struct {
	Drop      func(peer.ID, Envelope)
	Violation func(network.Conn, string)
}

type OutboundStreams struct {
	ctx       context.Context
	cancel    context.CancelFunc
	host      host.Host
	logger    *slog.Logger
	peer      peer.ID
	queueSize int
	hooks     OutboundHooks
	mu        sync.Mutex
	streams   map[string]*writer
	closed    bool
}

type writer struct {
	topic  string
	ch     chan Envelope
	cancel context.CancelFunc
	done   chan struct{}
}

func NewOutboundStreams(ctx context.Context, h host.Host, p peer.ID, queueSize int, logger *slog.Logger, hooks OutboundHooks) *OutboundStreams {
	ctx, cancel := context.WithCancel(ctx)
	if queueSize < 1 {
		queueSize = 1
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &OutboundStreams{ctx: ctx, cancel: cancel, host: h, logger: logger, peer: p, queueSize: queueSize, hooks: hooks, streams: make(map[string]*writer)}
}

func (s *OutboundStreams) Send(e Envelope) bool {
	s.mu.Lock()
	if s.closed || e.Topic == "" || e.wire() == nil {
		s.mu.Unlock()
		s.drop(e)
		return false
	}
	w, ok := s.streams[e.Topic]
	if ok {
		select {
		case <-w.done:
			w.cancel()
			delete(s.streams, e.Topic)
			ok = false
		default:
		}
	}
	if !ok {
		ctx, cancel := context.WithCancel(s.ctx)
		w = &writer{topic: e.Topic, ch: make(chan Envelope, s.queueSize), cancel: cancel, done: make(chan struct{})}
		s.streams[e.Topic] = w
		go s.run(ctx, w)
	}
	select {
	case w.ch <- e:
		s.mu.Unlock()
		return true
	default:
		s.mu.Unlock()
		s.drop(e)
		return false
	}
}

// CloseTopic cancels and forgets a topic writer without waiting for I/O shutdown.
func (s *OutboundStreams) CloseTopic(topic string) {
	s.mu.Lock()
	w, ok := s.streams[topic]
	if ok {
		w.cancel()
		delete(s.streams, topic)
	}
	s.mu.Unlock()
}

func (s *OutboundStreams) CloseTopicAndWait(ctx context.Context, topic string) bool {
	s.mu.Lock()
	w, ok := s.streams[topic]
	if ok {
		w.cancel()
		delete(s.streams, topic)
	}
	s.mu.Unlock()
	if !ok {
		return true
	}
	select {
	case <-w.done:
		return true
	case <-ctx.Done():
		return false
	}
}

func (s *OutboundStreams) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.cancel()
	s.streams = nil
	s.mu.Unlock()
}

func (s *OutboundStreams) run(ctx context.Context, w *writer) {
	var inFlight *Envelope
	defer func() { s.finishWriter(w, inFlight) }()
	if s.host == nil {
		s.logger.Debug("topic stream host is not configured", "peer", s.peer, "topic", w.topic)
		return
	}
	st, err := s.host.NewStream(ctx, s.peer, ProtocolID)
	if err != nil || st == nil {
		s.logger.Debug("failed to open topic stream", "peer", s.peer, "topic", w.topic, "err", err)
		return
	}
	defer st.Reset()
	go guardResponder(st, s.hooks.Violation)
	if err := WriteFrame(st, &pb.TopicRPCHeader{Topic: &w.topic}); err != nil {
		s.logger.Debug("failed to write topic stream header", "peer", s.peer, "topic", w.topic, "err", err)
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case e := <-w.ch:
			inFlight = &e
			if ctx.Err() != nil {
				return
			}
			if err := WriteFrame(st, e.wire()); err != nil {
				s.logger.Debug("failed to write topic rpc", "peer", s.peer, "topic", w.topic, "err", err)
				return
			}
			inFlight = nil
		}
	}
}

func (s *OutboundStreams) finishWriter(w *writer, inFlight *Envelope) {
	s.mu.Lock()
	if current := s.streams[w.topic]; current == w {
		delete(s.streams, w.topic)
	}
	dropped := make([]Envelope, 0, len(w.ch)+1)
	if inFlight != nil {
		dropped = append(dropped, *inFlight)
	}
	for {
		select {
		case e := <-w.ch:
			dropped = append(dropped, e)
		default:
			// done closes after every accepted envelope has a final disposition,
			// but callbacks run unlocked so they may safely reenter this object.
			s.mu.Unlock()
			for _, e := range dropped {
				s.drop(e)
			}
			close(w.done)
			return
		}
	}
}

func (s *OutboundStreams) drop(e Envelope) {
	if s.hooks.Drop != nil {
		s.hooks.Drop(s.peer, e)
	}
}

func guardResponder(s network.Stream, violation func(network.Conn, string)) {
	var b [1]byte
	n, err := s.Read(b[:])
	if n > 0 {
		if violation != nil {
			violation(s.Conn(), "topic stream responder wrote data")
		} else {
			_ = s.Conn().CloseWithError(ProtocolViolationError)
		}
	} else if err != nil && err != io.EOF {
		return
	}
}

func WriteFrame(s network.Stream, m proto.Message) error {
	size := uint64(proto.Size(m))
	buf := pool.Get(varint.UvarintSize(size) + int(size))
	defer pool.Put(buf)
	n := binary.PutUvarint(buf, size)
	out, err := proto.MarshalOptions{}.MarshalAppend(buf[:n], m)
	if err != nil {
		return err
	}
	if err = s.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
		return err
	}
	_, err = s.Write(out)
	return err
}
