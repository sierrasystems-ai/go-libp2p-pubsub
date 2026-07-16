package peercomm

import (
	"context"

	"github.com/libp2p/go-libp2p-pubsub/internal/topicstreams"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

type topicAuthorization struct {
	generation uint64
	authorized bool
}

type outboundCommand interface{ outboundCommand() }

type setOutboundEnabled struct{ enabled bool }
type setTopicAuthorization struct {
	topic      string
	authorized bool
}
type attachOutboundStreams struct{ streams *topicstreams.OutboundStreams }
type detachOutboundStreams struct{}
type closeOutboundTopic struct{ topic string }
type routeOutboundRPC struct {
	rpc   *pb.RPC
	reply chan topicstreams.RouteResult
}
type queryOutboundEnabled struct{ reply chan bool }
type closeOutboundState struct{ done chan struct{} }

func (setOutboundEnabled) outboundCommand()    {}
func (setTopicAuthorization) outboundCommand() {}
func (attachOutboundStreams) outboundCommand() {}
func (detachOutboundStreams) outboundCommand() {}
func (closeOutboundTopic) outboundCommand()    {}
func (routeOutboundRPC) outboundCommand()      {}
func (queryOutboundEnabled) outboundCommand()  {}
func (closeOutboundState) outboundCommand()    {}

type outboundState struct {
	ctx      context.Context
	commands chan outboundCommand
	done     chan struct{}
}

func newOutboundState(ctx context.Context) *outboundState {
	s := &outboundState{ctx: ctx, commands: make(chan outboundCommand), done: make(chan struct{})}
	go s.run()
	return s
}

func (s *outboundState) run() {
	defer close(s.done)
	var streams *topicstreams.OutboundStreams
	var enabled bool
	authorizations := make(map[string]topicAuthorization)
	closeStreams := func() {
		if streams != nil {
			streams.Close()
			streams = nil
		}
	}
	for {
		select {
		case raw := <-s.commands:
			switch command := raw.(type) {
			case setOutboundEnabled:
				enabled = command.enabled
				if !enabled {
					closeStreams()
				}
			case setTopicAuthorization:
				state := authorizations[command.topic]
				state.generation++
				state.authorized = command.authorized
				authorizations[command.topic] = state
				if !command.authorized && streams != nil {
					streams.CloseTopic(command.topic)
				}
			case attachOutboundStreams:
				closeStreams()
				streams = command.streams
			case detachOutboundStreams:
				closeStreams()
			case closeOutboundTopic:
				if streams != nil {
					streams.CloseTopic(command.topic)
				}
			case routeOutboundRPC:
				if !enabled || streams == nil {
					command.reply <- topicstreams.RouteResult{Control: command.rpc}
					continue
				}
				command.reply <- streams.RouteRPC(command.rpc, func(envelope topicstreams.Envelope) bool {
					state := authorizations[envelope.Topic]
					return state.authorized && state.generation != 0
				})
			case queryOutboundEnabled:
				command.reply <- enabled
			case closeOutboundState:
				closeStreams()
				close(command.done)
				return
			}
		case <-s.ctx.Done():
			closeStreams()
			return
		}
	}
}

func (s *outboundState) send(command outboundCommand) bool {
	select {
	case s.commands <- command:
		return true
	case <-s.done:
		return false
	case <-s.ctx.Done():
		return false
	}
}

func (s *outboundState) setEnabled(enabled bool) { s.send(setOutboundEnabled{enabled}) }
func (s *outboundState) setAuthorized(topic string, authorized bool) {
	s.send(setTopicAuthorization{topic: topic, authorized: authorized})
}
func (s *outboundState) attach(streams *topicstreams.OutboundStreams) {
	if !s.send(attachOutboundStreams{streams}) {
		streams.Close()
	}
}
func (s *outboundState) detach()                 { s.send(detachOutboundStreams{}) }
func (s *outboundState) closeTopic(topic string) { s.send(closeOutboundTopic{topic: topic}) }
func (s *outboundState) route(rpc *pb.RPC) topicstreams.RouteResult {
	reply := make(chan topicstreams.RouteResult, 1)
	if !s.send(routeOutboundRPC{rpc: rpc, reply: reply}) {
		return topicstreams.RouteResult{Control: rpc}
	}
	return <-reply
}
func (s *outboundState) enabled() bool {
	reply := make(chan bool, 1)
	if !s.send(queryOutboundEnabled{reply}) {
		return false
	}
	return <-reply
}
func (s *outboundState) close() {
	done := make(chan struct{})
	if s.send(closeOutboundState{done}) {
		<-done
	}
}
