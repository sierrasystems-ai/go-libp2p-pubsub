package peercomm

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"

	"sync"
	"sync/atomic"
	"time"

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

// InboundEventKind describes serialized inbound communication events.
type InboundEventKind uint8

const (
	InboundRPC InboundEventKind = iota
	InboundControlOpened
	InboundControlClosed
)

// InboundEvent is emitted in actor order.
type InboundEvent struct {
	Kind   InboundEventKind
	RPC    *pb.RPC
	Stream network.Stream
}

// Hooks are callbacks into the embedding PubSub service.
type Hooks struct {
	Protocols          func() []protocol.ID
	PrepareHello       func(context.Context, peer.ID, protocol.ID) (*pb.RPC, bool)
	OutboundOpenFailed func(peer.ID)
	OutboundDead       func(peer.ID)
	EmitInbound        func(peer.ID, InboundEvent) bool
}

type Config struct {
	Host                  host.Host
	OutboundQueueSize     int
	MaxMessageSize        int
	MaxControlMessageSize int
	Logger                *slog.Logger
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
	a := &Actor{registry: r, peer: pid, events: make(chan any, mailboxSize), done: make(chan struct{}), ctx: ctx, cancel: cancel, queue: newRPCQueue(r.config.OutboundQueueSize)}
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
	events   chan any
	done     chan struct{}
	ctx      context.Context
	cancel   context.CancelFunc
	queue    *rpcQueue
	started  atomic.Bool
}
type inboundControlOpen struct {
	stream network.Stream
	reply  chan Generation
}
type inboundControlRPC struct {
	generation Generation
	rpc        *pb.RPC
}
type inboundControlClose struct {
	generation Generation
	stream     network.Stream
}
type stop struct{ done chan struct{} }

func (a *Actor) submit(ev any) bool {
	select {
	case <-a.done:
		return false
	default:
	}
	select {
	case a.events <- ev:
		return true
	case <-a.done:
		return false
	case <-a.ctx.Done():
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
	a.submit(inboundControlRPC{g, r})
}
func (a *Actor) CloseInboundControl(g Generation, s network.Stream) {
	a.submit(inboundControlClose{g, s})
}
func (a *Actor) Terminate() {
	a.registry.remove(a.peer, a)
	a.queue.close()
	a.cancel()
}
func (a *Actor) Stop() bool {
	ack := make(chan struct{})
	t := time.NewTimer(stopTimeout)
	defer t.Stop()
	select {
	case a.events <- stop{ack}:
	case <-a.done:
		return true
	case <-t.C:
		return false
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
	var control network.Stream
	var controlSet bool
	var controlGen uint64
	emit := func(ev InboundEvent) bool {
		if a.registry.hooks.EmitInbound == nil {
			return true
		}
		return a.registry.hooks.EmitInbound(a.peer, ev)
	}
	retire := func() bool {
		if controlSet || a.started.Load() {
			return false
		}
		return a.registry.remove(a.peer, a)
	}
	shutdown := func() {
		if controlSet {
			_ = control.Reset()
		}
	}
	for {
		select {
		case raw := <-a.events:
			switch ev := raw.(type) {
			case inboundControlOpen:
				if controlSet && control != ev.stream {
					_ = control.Reset()
					emit(InboundEvent{Kind: InboundControlClosed, Stream: control})
				}
				controlGen++
				control = ev.stream
				controlSet = true
				emit(InboundEvent{Kind: InboundControlOpened, Stream: ev.stream})
				ev.reply <- Generation{controlGen}
			case inboundControlRPC:
				if ev.generation.value != controlGen || !controlSet {
					continue
				}
				if !emit(InboundEvent{Kind: InboundRPC, RPC: ev.rpc, Stream: control}) {
					shutdown()
					return
				}
			case inboundControlClose:
				if ev.generation.value != controlGen || !controlSet || control != ev.stream {
					continue
				}
				emit(InboundEvent{Kind: InboundControlClosed, Stream: ev.stream})
				controlSet = false
				if retire() {
					return
				}
			case stop:
				shutdown()
				close(ev.done)
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

// Start opens and owns the outbound control stream after the requested backoff.
func (a *Actor) Start(backoff time.Duration) {
	if !a.started.CompareAndSwap(false, true) {
		return
	}
	go func() {
		if backoff > 0 {
			timer := time.NewTimer(backoff)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-a.ctx.Done():
				a.started.Store(false)
				return
			}
		}
		a.runOutbound()
	}()
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

func (a *Actor) runOutbound() {
	h := a.registry.hooks
	opened := false
	defer func() { a.outboundTerminated(opened) }()
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
	defer func() { _ = s.Close() }()
	hello, ok := h.PrepareHello(ctx, a.peer, s.Protocol())
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

func (a *Actor) outboundTerminated(opened bool) {
	// Publish the idle state before notifying PubSub so a callback-triggered
	// restart cannot be rejected by the previous session's start guard.
	a.started.Store(false)
	h := a.registry.hooks
	if opened {
		if h.OutboundDead != nil {
			h.OutboundDead(a.peer)
		}
	} else if h.OutboundOpenFailed != nil {
		h.OutboundOpenFailed(a.peer)
	}
}
