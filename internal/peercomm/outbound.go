package peercomm

import (
	"context"

	"github.com/libp2p/go-libp2p-pubsub/internal/topicstreams"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

// outboundSession is owned exclusively by Actor.run. Its generation binds
// negotiation, remote subscriptions, routing, topic writers, and teardown to
// one outbound control-stream lifetime. A new generation always starts empty;
// traffic queued across a transition stays on the control stream until the new
// session negotiates and authorizes it explicitly.
type outboundSession struct {
	generation     uint64
	enabled        bool
	authorizations map[string]struct{}
	streams        *topicstreams.OutboundStreams
}

func (s *outboundSession) retire() {
	if s.streams != nil {
		s.streams.Close()
	}
	s.enabled = false
	s.authorizations = nil
	s.streams = nil
}

func (s *outboundSession) route(rpc *pb.RPC) topicstreams.RouteResult {
	if s.streams != nil {
		for _, subscription := range rpc.GetSubscriptions() {
			if !subscription.GetSubscribe() {
				s.streams.CloseTopic(subscription.GetTopicid())
			}
		}
	}
	if !s.enabled || s.streams == nil {
		return topicstreams.RouteResult{Control: rpc}
	}
	return s.streams.RouteRPC(rpc, func(envelope topicstreams.Envelope) bool {
		_, ok := s.authorizations[envelope.Topic]
		return ok
	})
}

type beginOutboundSession struct{ reply chan uint64 }
type attachOutboundStreams struct {
	generation uint64
	streams    *topicstreams.OutboundStreams
	reply      chan bool
}
type endOutboundSession struct {
	generation uint64
	done       chan struct{}
}
type setOutboundEnabled struct {
	generation uint64
	enabled    bool
}
type setTopicAuthorization struct {
	generation uint64
	topic      string
	authorized bool
}
type routeDisposition uint8

const (
	routeAborted routeDisposition = iota
	routeStale
	routeCompleted
)

type routeReply struct {
	topicstreams.RouteResult
	disposition routeDisposition
}

type routeOutboundRPC struct {
	generation uint64
	rpc        *pb.RPC
	reply      chan routeReply
}
type queryOutboundSession struct{ reply chan uint64 }
type queryOutboundEnabled struct {
	generation uint64
	reply      chan bool
}

type outboundActorEvent interface {
	actorEvent
	outboundActorEvent()
}

func (beginOutboundSession) actorEvent()          {}
func (attachOutboundStreams) actorEvent()         {}
func (endOutboundSession) actorEvent()            {}
func (setOutboundEnabled) actorEvent()            {}
func (setTopicAuthorization) actorEvent()         {}
func (routeOutboundRPC) actorEvent()              {}
func (queryOutboundSession) actorEvent()          {}
func (queryOutboundEnabled) actorEvent()          {}
func (beginOutboundSession) outboundActorEvent()  {}
func (attachOutboundStreams) outboundActorEvent() {}
func (endOutboundSession) outboundActorEvent()    {}
func (setOutboundEnabled) outboundActorEvent()    {}
func (setTopicAuthorization) outboundActorEvent() {}
func (routeOutboundRPC) outboundActorEvent()      {}
func (queryOutboundSession) outboundActorEvent()  {}
func (queryOutboundEnabled) outboundActorEvent()  {}

func (a *Actor) beginOutboundSession() (uint64, bool) {
	reply := make(chan uint64, 1)
	if !a.submit(beginOutboundSession{reply: reply}) {
		return 0, false
	}
	select {
	case generation := <-reply:
		return generation, true
	case <-a.done:
		return 0, false
	case <-a.ctx.Done():
		return 0, false
	}
}

func (a *Actor) attachOutbound(generation uint64, streams *topicstreams.OutboundStreams) bool {
	reply := make(chan bool, 1)
	if !a.submit(attachOutboundStreams{generation: generation, streams: streams, reply: reply}) {
		streams.Close()
		return false
	}
	select {
	case attached := <-reply:
		return attached
	case <-a.done:
		streams.Close()
		return false
	case <-a.ctx.Done():
		streams.Close()
		return false
	}
}

func (a *Actor) endOutbound(generation uint64) {
	done := make(chan struct{})
	if a.submit(endOutboundSession{generation: generation, done: done}) {
		select {
		case <-done:
		case <-a.done:
		case <-a.ctx.Done():
		}
	}
}

func (a *Actor) routeOutbound(ctx context.Context, generation uint64, rpc *pb.RPC) routeReply {
	// Cancellation observed before submission has no actor side effects.
	select {
	case <-ctx.Done():
		return routeReply{disposition: routeAborted}
	default:
	}
	reply := make(chan routeReply, 1)
	if !a.submit(routeOutboundRPC{generation: generation, rpc: rpc, reply: reply}) {
		return routeReply{disposition: routeAborted}
	}
	// Cancellation only aborts before submission. Once the actor accepted the
	// event, wait for its one terminal disposition so a routed payload and its
	// control remainder can never be abandoned with unknown status.
	select {
	case result := <-reply:
		return result
	case <-a.done:
		select {
		case result := <-reply:
			return result
		default:
			return routeReply{disposition: routeAborted}
		}
	}
}
