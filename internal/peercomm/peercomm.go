package peercomm

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"

	"sync"
	"time"

	"github.com/libp2p/go-libp2p-pubsub/internal/topicstreams"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-msgio"

	pool "github.com/libp2p/go-buffer-pool"
	"github.com/multiformats/go-varint"
	"google.golang.org/protobuf/proto"
)

const (
	mailboxSize = 64
	stopTimeout = time.Second
)

// Generation is an opaque token identifying one actor-owned stream generation.
type Generation struct{ value uint64 }

// Session is an opaque token identifying one outbound control-stream lifetime.
type Session struct{ generation uint64 }

// Admission is an opaque token for an admitted inbound topic stream.
type Admission struct {
	id, generation uint64
	accepted       bool
}

func (a Admission) Accepted() bool { return a.accepted }

// InboundEventKind describes serialized inbound communication events.
type InboundEventKind uint8

const (
	InboundRPC InboundEventKind = iota
	InboundTopicRPC
	InboundControlOpened
	InboundControlClosed
)

// InboundEvent is emitted in actor order.
type InboundEvent struct {
	Kind    InboundEventKind
	RPC     *pb.RPC
	Stream  network.Stream
	Session Session
}

// Hooks are callbacks into the embedding PubSub service.
type Hooks struct {
	Protocols            func() []protocol.ID
	PrepareHello         func(context.Context, peer.ID, protocol.ID, Session) (*pb.RPC, bool)
	OutboundOpenFailed   func(peer.ID)
	OutboundDead         func(peer.ID)
	EmitInbound          func(peer.ID, InboundEvent) bool
	PenalizeInboundLimit func(peer.ID)
}

type Config struct {
	Host                      host.Host
	OutboundQueueSize         int
	MaxMessageSize            int
	MaxControlMessageSize     int
	MaxInboundStreamsPerTopic int
	MaxInboundStreamsPerPeer  int
	FirstControlTimeout       time.Duration
	Logger                    *slog.Logger
	TopicStreamsHooks         topicstreams.OutboundHooks
	TopicStreamViolation      func(network.Conn, string)
}

type Registry struct {
	ctx     context.Context
	config  Config
	hooks   Hooks
	mu      sync.Mutex
	peers   map[peer.ID]*Actor
	stopped bool
}

func NewRegistry(ctx context.Context, config Config, hooks Hooks) *Registry {
	if config.Logger == nil {
		config.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Registry{ctx: ctx, config: config, hooks: hooks, peers: make(map[peer.ID]*Actor)}
}

// For returns the peer actor, or nil after registry shutdown.
func (r *Registry) For(pid peer.ID) *Actor {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return nil
	}
	if a := r.peers[pid]; a != nil {
		return a
	}
	ctx, cancel := context.WithCancel(r.ctx)
	a := &Actor{registry: r, peer: pid, events: make(chan actorEvent, mailboxSize), done: make(chan struct{}), ctx: ctx, cancel: cancel, queue: newRPCQueue(r.config.OutboundQueueSize)}
	a.delivery = newInboundDelivery(ctx, pid, r.hooks.EmitInbound, cancel)
	r.peers[pid] = a
	go a.run()
	return a
}
func (r *Registry) Existing(pid peer.ID) (*Actor, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	a, ok := r.peers[pid]
	return a, ok
}

// Len returns the number of live peer actors registered.
func (r *Registry) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.peers)
}
func (r *Registry) remove(pid peer.ID, a *Actor) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.peers[pid] != a {
		return false
	}
	delete(r.peers, pid)
	return true
}
func (r *Registry) Stop() {
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return
	}
	r.stopped = true
	peers := r.peers
	r.peers = make(map[peer.ID]*Actor)
	r.mu.Unlock()
	for _, a := range peers {
		a.cancel()
	}
	t := time.NewTimer(stopTimeout)
	defer t.Stop()
	for _, a := range peers {
		select {
		case <-a.done:
		case <-t.C:
			return
		}
	}
}

type Actor struct {
	registry *Registry
	peer     peer.ID
	events   chan actorEvent
	done     chan struct{}
	ctx      context.Context
	cancel   context.CancelFunc
	queue    *rpcQueue
	delivery *inboundDelivery
	submitMu sync.Mutex
	pending  int
	retiring bool
}
type actorEvent interface{ actorEvent() }

type inboundControlOpen struct {
	stream network.Stream
	reply  chan Generation
}
type inboundControlRPC struct {
	generation Generation
	rpc        *pb.RPC
	reserved   bool
}
type inboundControlClose struct {
	generation Generation
	stream     network.Stream
}
type inboundTopicOpen struct {
	stream network.Stream
	topic  string
	reply  chan Admission
}
type inboundTopicRPC struct {
	admission Admission
	rpc       *pb.RPC
	reply     chan bool
	deadline  time.Time
}
type inboundTopicClose struct{ admission Admission }

type cancelPendingInboundTopicRPC struct {
	admission Admission
	reply     chan bool
	ack       chan struct{}
}
type stop struct{ done chan struct{} }
type startOutbound struct{ backoff time.Duration }
type outboundLifecycleEnded struct{ opened bool }

func (inboundControlOpen) actorEvent()           {}
func (inboundControlRPC) actorEvent()            {}
func (inboundControlClose) actorEvent()          {}
func (inboundTopicOpen) actorEvent()             {}
func (inboundTopicRPC) actorEvent()              {}
func (inboundTopicClose) actorEvent()            {}
func (cancelPendingInboundTopicRPC) actorEvent() {}
func (stop) actorEvent()                         {}
func (startOutbound) actorEvent()                {}
func (outboundLifecycleEnded) actorEvent()       {}

type inboundTopic struct {
	stream     network.Stream
	topic      string
	generation uint64
	pending    *inboundTopicRPC
}

func (a *Actor) submit(ev actorEvent) bool {
	a.submitMu.Lock()
	if a.retiring {
		a.submitMu.Unlock()
		return false
	}
	// Reserve this submission before releasing the barrier lock. Retirement
	// cannot cross the barrier while either a sender or an accepted event exists.
	a.pending++
	a.submitMu.Unlock()
	select {
	case a.events <- ev:
		return true
	case <-a.done:
		a.consumed()
		return false
	case <-a.ctx.Done():
		a.consumed()
		return false
	}
}
func (a *Actor) OpenInboundControl(s network.Stream) (Generation, bool) {
	ch := make(chan Generation, 1)
	if !a.submit(inboundControlOpen{s, ch}) {
		return Generation{}, false
	}
	select {
	case g := <-ch:
		return g, true
	case <-a.done:
		return Generation{}, false
	case <-a.ctx.Done():
		return Generation{}, false
	}
}
func (a *Actor) DeliverInboundControl(g Generation, r *pb.RPC) {
	if !a.delivery.reserve() {
		return
	}
	if !a.submit(inboundControlRPC{generation: g, rpc: r, reserved: true}) {
		a.delivery.release()
	}
}
func (a *Actor) CloseInboundControl(g Generation, s network.Stream) {
	a.submit(inboundControlClose{g, s})
}
func (a *Actor) OpenInboundTopic(s network.Stream, topic string) Admission {
	ch := make(chan Admission, 1)
	if !a.submit(inboundTopicOpen{s, topic, ch}) {
		return Admission{}
	}
	select {
	case x := <-ch:
		return x
	case <-a.done:
		return Admission{}
	case <-a.ctx.Done():
		return Admission{}
	}
}
func (a *Actor) DeliverInboundTopic(ad Admission, r *pb.RPC) bool {
	ch := make(chan bool, 1)
	timeout := a.registry.config.FirstControlTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	if !a.submit(inboundTopicRPC{ad, r, ch, time.Now().Add(timeout)}) {
		return false
	}
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case ok := <-ch:
		return ok
	case <-t.C:
		ack := make(chan struct{})
		if a.submit(cancelPendingInboundTopicRPC{ad, ch, ack}) {
			select {
			case <-ack:
			case <-a.done:
			case <-a.ctx.Done():
			}
		}
		return false
	case <-a.done:
		return false
	case <-a.ctx.Done():
		return false
	}
}
func (a *Actor) CloseInboundTopic(ad Admission) { a.submit(inboundTopicClose{ad}) }
func (a *Actor) Terminate() {
	a.submitMu.Lock()
	a.retiring = true
	a.submitMu.Unlock()
	a.registry.remove(a.peer, a)
	a.queue.close()
	a.cancel()
}
func (a *Actor) SetTopicStreamsEnabled(session Session, enabled bool) {
	a.submit(setOutboundEnabled{generation: session.generation, enabled: enabled})
}

// TopicStreamsEnabled reports the selected session's negotiated routing state.
func (a *Actor) CurrentSession() Session {
	reply := make(chan uint64, 1)
	if !a.submit(queryOutboundSession{reply: reply}) {
		return Session{}
	}
	return Session{<-reply}
}

func (a *Actor) TopicStreamsEnabled() bool {
	reply := make(chan bool, 1)
	if !a.submit(queryOutboundEnabled{reply: reply}) {
		return false
	}
	return <-reply
}

// RemoteSubscribed asynchronously authorizes a topic for the event's session.
func (a *Actor) RemoteSubscribed(session Session, topic string) {
	a.submit(setTopicAuthorization{generation: session.generation, topic: topic, authorized: true})
}

// RemoteUnsubscribed asynchronously revokes a topic for the event's session.
func (a *Actor) RemoteUnsubscribed(session Session, topic string) {
	a.submit(setTopicAuthorization{generation: session.generation, topic: topic})
}

func (a *Actor) Stop() bool {
	ack := make(chan struct{})
	t := time.NewTimer(stopTimeout)
	defer t.Stop()
	if !a.submit(stop{ack}) {
		return true
	}
	select {
	case <-ack:
		return true
	case <-a.done:
		return true
	case <-t.C:
		return false
	}
}
func (a *Actor) Done() <-chan struct{} { return a.done }

func (a *Actor) run() {
	defer close(a.done)
	defer a.cancel()
	defer a.queue.close()
	outbound := outboundSession{generation: 1, authorizations: make(map[string]struct{})}
	nextOutboundGeneration := uint64(1)
	var control network.Stream
	var controlSet bool
	var controlGen, nextTopicID uint64
	firstControl := false
	topics := make(map[uint64]*inboundTopic)
	counts := make(map[string]int)
	total := 0
	outboundActive := false
	retirementPending := false
	emit := func(ev InboundEvent, reply chan bool) bool {
		return a.delivery.submit(ev, reply, false)
	}
	closeTopics := func() {
		for _, t := range topics {
			t.stream.Reset()
			if t.pending != nil {
				t.pending.reply <- false
			}
		}
		topics = make(map[uint64]*inboundTopic)
		counts = make(map[string]int)
		total = 0
	}
	closeOutbound := func() { outbound.retire() }
	retire := func() bool {
		if controlSet || len(topics) > 0 || outboundActive {
			return false
		}
		return a.registry.beginRetirement(a)
	}
	shutdown := func() {
		if controlSet {
			control.Reset()
		}
		closeTopics()
		closeOutbound()
	}
	for {
		select {
		case raw := <-a.events:
			a.consumed()
			switch ev := raw.(type) {
			case startOutbound:
				if outboundActive {
					continue
				}
				outboundActive = true
				go a.runOutbound(ev.backoff)
			case beginOutboundSession:
				outbound.retire()
				nextOutboundGeneration++
				outbound = outboundSession{generation: nextOutboundGeneration, authorizations: make(map[string]struct{})}
				ev.reply <- outbound.generation
			case attachOutboundStreams:
				if ev.generation != outbound.generation || ev.generation == 0 {
					ev.streams.Close()
					ev.reply <- false
					continue
				}
				if outbound.streams != nil {
					outbound.streams.Close()
				}
				outbound.streams = ev.streams
				ev.reply <- true
			case endOutboundSession:
				if ev.generation == outbound.generation {
					outbound.retire()
				}
				close(ev.done)
			case outboundLifecycleEnded:
				outboundActive = false
				// Cross the registry/submission barrier before invoking external
				// lifecycle code. A callback reconnect therefore gets a fresh actor.
				retired := retire()
				retirementPending = !retired
				h := a.registry.hooks
				if ev.opened {
					if h.OutboundDead != nil {
						h.OutboundDead(a.peer)
					}
				} else if h.OutboundOpenFailed != nil {
					h.OutboundOpenFailed(a.peer)
				}
				if retired {
					return
				}
			case setOutboundEnabled:
				if ev.generation != outbound.generation {
					continue
				}
				if !ev.enabled {
					outbound.retire()
				} else if outbound.generation != 0 {
					outbound.enabled = true
				}
			case setTopicAuthorization:
				if ev.generation == outbound.generation && outbound.generation != 0 {
					if ev.authorized {
						outbound.authorizations[ev.topic] = struct{}{}
					} else {
						delete(outbound.authorizations, ev.topic)
						if outbound.streams != nil {
							outbound.streams.CloseTopic(ev.topic)
						}
					}
				}
			case routeOutboundRPC:
				if ev.generation != outbound.generation {
					ev.reply <- routeReply{disposition: routeStale, RouteResult: topicstreams.RouteResult{Control: ev.rpc}}
				} else {
					ev.reply <- routeReply{disposition: routeCompleted, RouteResult: outbound.route(ev.rpc)}
				}
			case queryOutboundSession:
				ev.reply <- outbound.generation
			case queryOutboundEnabled:
				ev.reply <- (ev.generation == 0 || ev.generation == outbound.generation) && outbound.enabled
			case inboundControlOpen:
				if controlSet && control != ev.stream {
					control.Reset()
					closeTopics()
					emit(InboundEvent{Kind: InboundControlClosed, Stream: control, Session: Session{outbound.generation}}, nil)
				}
				controlGen++
				control = ev.stream
				controlSet = true
				firstControl = false
				for _, t := range topics {
					if t.generation == 0 {
						t.generation = controlGen
					}
				}
				emit(InboundEvent{Kind: InboundControlOpened, Stream: ev.stream, Session: Session{outbound.generation}}, nil)
				ev.reply <- Generation{controlGen}
			case inboundControlRPC:
				if ev.generation.value != controlGen || !controlSet {
					if ev.reserved {
						a.delivery.release()
					}
					continue
				}
				if !a.delivery.submit(InboundEvent{Kind: InboundRPC, RPC: ev.rpc, Stream: control, Session: Session{outbound.generation}}, nil, ev.reserved) {
					shutdown()
					return
				}
				if !firstControl {
					firstControl = true
					for _, t := range topics {
						if t.generation == controlGen && t.pending != nil {
							ok := false
							if time.Now().Before(t.pending.deadline) {
								ok = emit(InboundEvent{Kind: InboundTopicRPC, RPC: t.pending.rpc, Stream: t.stream, Session: Session{outbound.generation}}, t.pending.reply)
							}
							if !ok {
								t.pending.reply <- false
							}
							t.pending = nil
						}
					}
				}
			case inboundControlClose:
				if ev.generation.value != controlGen || !controlSet || control != ev.stream {
					continue
				}
				closeTopics()
				emit(InboundEvent{Kind: InboundControlClosed, Stream: ev.stream, Session: Session{outbound.generation}}, nil)
				controlSet = false
				firstControl = false
				if retire() {
					return
				}
			case inboundTopicOpen:
				if counts[ev.topic] >= a.registry.config.MaxInboundStreamsPerTopic || total >= a.registry.config.MaxInboundStreamsPerPeer {
					ev.reply <- Admission{}
					if a.registry.hooks.PenalizeInboundLimit != nil {
						a.registry.hooks.PenalizeInboundLimit(a.peer)
					}
					continue
				}
				nextTopicID++
				g := controlGen
				if !controlSet {
					g = 0
				}
				topics[nextTopicID] = &inboundTopic{stream: ev.stream, topic: ev.topic, generation: g}
				counts[ev.topic]++
				total++
				ev.reply <- Admission{id: nextTopicID, generation: g, accepted: true}
			case inboundTopicRPC:
				t := topics[ev.admission.id]
				if t == nil || (ev.admission.generation != 0 && t.generation != ev.admission.generation) {
					ev.reply <- false
					continue
				}
				if !controlSet {
					if t.pending != nil {
						ev.reply <- false
					} else {
						t.pending = &ev
					}
					continue
				}
				if t.generation == 0 {
					t.generation = controlGen
				}
				if t.generation != controlGen {
					ev.reply <- false
					continue
				}
				if !firstControl {
					if t.pending != nil {
						ev.reply <- false
					} else {
						t.pending = &ev
					}
					continue
				}
				if time.Now().Before(ev.deadline) {
					if !emit(InboundEvent{Kind: InboundTopicRPC, RPC: ev.rpc, Stream: t.stream, Session: Session{outbound.generation}}, ev.reply) {
						ev.reply <- false
					}
				} else {
					ev.reply <- false
				}
			case cancelPendingInboundTopicRPC:
				t := topics[ev.admission.id]
				if t != nil && (ev.admission.generation == 0 || t.generation == ev.admission.generation) && t.pending != nil && t.pending.reply == ev.reply {
					t.pending = nil
				}
				close(ev.ack)
			case inboundTopicClose:
				t := topics[ev.admission.id]
				if t == nil || (ev.admission.generation != 0 && t.generation != ev.admission.generation) {
					continue
				}
				if t.pending != nil {
					t.pending.reply <- false
				}
				delete(topics, ev.admission.id)
				counts[t.topic]--
				total--
				if retire() {
					return
				}
			case stop:
				shutdown()
				close(ev.done)
				return
			}
			// Retirement crosses the explicit submission barrier only after every
			// event accepted in the current epoch has been consumed.
			if retirementPending && retire() {
				return
			}
		case <-a.ctx.Done():
			shutdown()
			return
		}
	}
}

var (
	ErrQueueClosed = errors.New("peer communication queue closed")
	ErrQueueFull   = errors.New("peer communication queue full")
)

type rpcQueue struct {
	mu             sync.Mutex
	ready          *sync.Cond
	space          *sync.Cond
	normal, urgent []*pb.RPC
	max            int
	closed         bool
}

func newRPCQueue(max int) *rpcQueue {
	if max <= 0 {
		max = 1
	}
	q := &rpcQueue{max: max}
	q.ready = sync.NewCond(&q.mu)
	q.space = sync.NewCond(&q.mu)
	return q
}
func (q *rpcQueue) push(r *pb.RPC, urgent, block bool) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrQueueClosed
	}
	for len(q.normal)+len(q.urgent) == q.max {
		if !block {
			return ErrQueueFull
		}
		q.space.Wait()
		if q.closed {
			return ErrQueueClosed
		}
	}
	if urgent {
		q.urgent = append(q.urgent, r)
	} else {
		q.normal = append(q.normal, r)
	}
	q.ready.Signal()
	return nil
}
func (q *rpcQueue) pop(ctx context.Context) (*pb.RPC, error) {
	stop := context.AfterFunc(ctx, func() { q.mu.Lock(); q.ready.Broadcast(); q.mu.Unlock() })
	defer stop()
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.normal)+len(q.urgent) == 0 {
		if q.closed {
			return nil, ErrQueueClosed
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		q.ready.Wait()
	}
	var r *pb.RPC
	if len(q.urgent) > 0 {
		r = q.urgent[0]
		q.urgent[0] = nil
		q.urgent = q.urgent[1:]
	} else {
		r = q.normal[0]
		q.normal[0] = nil
		q.normal = q.normal[1:]
	}
	q.space.Signal()
	return r, nil
}
func (q *rpcQueue) close() {
	q.mu.Lock()
	q.closed = true
	q.ready.Broadcast()
	q.space.Broadcast()
	q.mu.Unlock()
}

// Send enqueues an RPC without blocking. Urgent RPCs are drained before normal RPCs.
func (a *Actor) Send(rpc *pb.RPC, urgent bool) error { return a.queue.push(rpc, urgent, false) }

// Start asks the actor to open and own an outbound control stream after backoff.
func (a *Actor) Start(backoff time.Duration) {
	a.submit(startOutbound{backoff: backoff})
}

// HandleInboundTopicStream owns the complete inbound topic-stream lifecycle.
func (a *Actor) HandleInboundTopicStream(s network.Stream) {
	var admission Admission
	topicstreams.HandleInbound(s, a.registry.config.MaxMessageSize, topicstreams.InboundHooks{
		Admit: func(s network.Stream, topic string) bool {
			admission = a.OpenInboundTopic(s, topic)
			return admission.Accepted()
		},
		Deliver: func(envelope topicstreams.Envelope) bool {
			rpc := new(pb.RPC)
			if envelope.Publish != nil {
				rpc.Publish = []*pb.Message{envelope.Publish}
			}
			rpc.Partial = envelope.Partial
			return a.DeliverInboundTopic(admission, rpc)
		},
		Close:     func() { a.CloseInboundTopic(admission) },
		Logger:    a.registry.config.Logger,
		Violation: a.registry.config.TopicStreamViolation,
	})
}

// HandleInboundControl owns the complete inbound control-stream read lifecycle.
func (a *Actor) HandleInboundControl(s network.Stream) {
	g, ok := a.OpenInboundControl(s)
	if !ok {
		_ = s.Reset()
		return
	}
	defer a.CloseInboundControl(g, s)
	r := msgio.NewVarintReaderSize(s, a.registry.config.MaxMessageSize)
	for {
		_, _ = r.NextMsgLen()
		start := time.Now()
		b, err := r.ReadMsg()
		if err != nil {
			r.ReleaseMsg(b)
			if err != io.EOF {
				_ = s.Reset()
				a.registry.config.Logger.Log(a.ctx, slog.LevelDebug, "error reading rpc", "peer", a.peer, "err", err)
			} else {
				_ = s.Close()
			}
			return
		}
		if len(b) == 0 {
			continue
		}
		if err = pb.ValidateRawRPCControlMessageSize(b, a.registry.config.MaxControlMessageSize); err != nil {
			r.ReleaseMsg(b)
			_ = s.Reset()
			a.registry.config.Logger.Log(a.ctx, slog.LevelWarn, "RPC control message too large", "peer", a.peer, "err", err)
			return
		}
		rpc := new(pb.RPC)
		err = proto.Unmarshal(b, rpc)
		r.ReleaseMsg(b)
		if err != nil {
			_ = s.Reset()
			a.registry.config.Logger.Log(a.ctx, slog.LevelWarn, "bogus rpc", "peer", a.peer, "err", err)
			return
		}

		a.registry.config.Logger.Log(a.ctx, slog.LevelDebug, "received rpc", "peer", a.peer, "duration", time.Since(start), "rpc", rpc)
		a.DeliverInboundControl(g, rpc)
	}
}

func (a *Actor) runOutbound(backoff time.Duration) {
	h := a.registry.hooks
	opened := false
	defer func() { a.submit(outboundLifecycleEnded{opened: opened}) }()
	if backoff > 0 {
		timer := time.NewTimer(backoff)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-a.ctx.Done():
			return
		}
	}
	generation, ok := a.beginOutboundSession()
	if !ok {
		return
	}
	defer a.endOutbound(generation)
	if a.registry.config.Host == nil || h.Protocols == nil {
		return
	}
	s, err := a.registry.config.Host.NewStream(a.ctx, a.peer, h.Protocols()...)
	if err != nil {
		a.registry.config.Logger.Log(a.ctx, slog.LevelDebug, "error opening stream", "peer", a.peer, "err", err)
		return
	}
	opened = true
	ctx, cancel := context.WithCancel(a.ctx)
	defer cancel()
	defer s.Close()
	hello, ok := h.PrepareHello(ctx, a.peer, s.Protocol(), Session{generation})
	if !ok {
		_ = s.Reset()
		return
	}
	if hello != nil && proto.Size(hello) > 0 {
		if err = a.writeRPC(s, hello); err != nil {
			_ = s.Reset()
			return
		}
	}
	topics := topicstreams.NewOutboundStreams(ctx, a.registry.config.Host, a.peer, a.registry.config.OutboundQueueSize, a.registry.config.Logger, a.registry.config.TopicStreamsHooks)
	if !a.attachOutbound(generation, topics) {
		return
	}
	go func() {
		_, e := s.Read([]byte{0})
		if e == nil {
			a.registry.config.Logger.Log(a.ctx, slog.LevelDebug, "unexpected message on outbound stream", "peer", a.peer)
		}
		_ = s.Reset()
		cancel()
	}()
	for {
		rpc, e := a.queue.pop(ctx)
		if e != nil {
			return
		}
		routed := a.routeOutbound(ctx, generation, rpc)
		if routed.disposition == routeAborted {
			continue
		}
		rpc = routed.Control
		if rpc == nil {
			continue
		}
		if e = a.writeRPC(s, rpc); e != nil {
			_ = s.Reset()
			break
		}
	}
}

func (a *Actor) writeRPC(s network.Stream, rpc *pb.RPC) error {
	size := uint64(proto.Size(rpc))
	buf := pool.Get(varint.UvarintSize(size) + int(size))
	defer pool.Put(buf)
	n := binary.PutUvarint(buf, size)
	out, err := proto.MarshalOptions{}.MarshalAppend(buf[:n], rpc)
	if err != nil {
		return err
	}
	if err = s.SetWriteDeadline(time.Now().Add(30 * time.Second)); err != nil {
		return err
	}
	_, err = s.Write(out)
	if err != nil {
		a.registry.config.Logger.Log(a.ctx, slog.LevelDebug, "failed to send rpc", "peer", a.peer, "err", err)
		return err
	}
	if publish := rpc.GetPublish(); len(publish) > 0 {
		a.registry.config.Logger.Log(a.ctx, slog.LevelDebug, "sent rpc", "peer", a.peer, "topic", publish[0].GetTopic())
	} else {
		a.registry.config.Logger.Log(a.ctx, slog.LevelDebug, "sent rpc", "peer", a.peer)
	}
	return nil
}
