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
	enabled bool
	done    chan struct{}
}
type setTopicAuthorization struct {
	topic      string
	authorized bool
	done       chan struct{}
}
type closeOutboundTopic struct {
	topic string
	done  chan struct{}
}
type routeOutboundRPC struct {
	generation uint64
	rpc        *pb.RPC
	reply      chan topicstreams.RouteResult
}
type queryOutboundEnabled struct{ reply chan bool }

type outboundActorEvent interface {
	actorEvent
	outboundActorEvent()
}

func (beginOutboundSession) actorEvent()          {}
func (attachOutboundStreams) actorEvent()         {}
func (endOutboundSession) actorEvent()            {}
func (setOutboundEnabled) actorEvent()            {}
func (setTopicAuthorization) actorEvent()         {}
func (closeOutboundTopic) actorEvent()            {}
func (routeOutboundRPC) actorEvent()              {}
func (queryOutboundEnabled) actorEvent()          {}
func (beginOutboundSession) outboundActorEvent()  {}
func (attachOutboundStreams) outboundActorEvent() {}
func (endOutboundSession) outboundActorEvent()    {}
func (setOutboundEnabled) outboundActorEvent()    {}
func (setTopicAuthorization) outboundActorEvent() {}
func (closeOutboundTopic) outboundActorEvent()    {}
func (routeOutboundRPC) outboundActorEvent()      {}
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

func (a *Actor) routeOutbound(ctx context.Context, generation uint64, rpc *pb.RPC) topicstreams.RouteResult {
	reply := make(chan topicstreams.RouteResult, 1)
	if !a.submit(routeOutboundRPC{generation: generation, rpc: rpc, reply: reply}) {
		return topicstreams.RouteResult{Control: rpc}
	}
	select {
	case result := <-reply:
		return result
	case <-ctx.Done():
		return topicstreams.RouteResult{Control: rpc}
	case <-a.done:
		return topicstreams.RouteResult{Control: rpc}
	}
}
