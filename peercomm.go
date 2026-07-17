package pubsub

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/libp2p/go-libp2p-pubsub/internal/peercomm"
	"github.com/libp2p/go-libp2p-pubsub/internal/topicstreams"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

type peerComm = peercomm.Actor
type peerCommRegistry = peercomm.Registry

func (p *PubSub) initPeerComms() {
	p.peerComms = peercomm.NewRegistry(p.ctx, peercomm.Config{
		Host:                      p.host,
		OutboundQueueSize:         p.peerOutboundQueueSize,
		MaxMessageSize:            p.maxMessageSize,
		MaxControlMessageSize:     p.maxControlMessageSize,
		MaxInboundStreamsPerTopic: p.topicStreamsConfig.MaxInboundStreamsPerTopic,
		MaxInboundStreamsPerPeer:  p.topicStreamsConfig.MaxInboundStreamsPerPeer,
		FirstControlTimeout:       topicStreamFirstControlTimeout,
		Logger:                    p.rpcLogger,
		TopicStreamsHooks: topicstreams.OutboundHooks{
			Drop: func(pid peer.ID, e topicstreams.Envelope) {
				if p.tracer == nil {
					return
				}
				if e.Publish != nil {
					p.tracer.DropRPC(&RPC{RPC: pb.RPC{Publish: []*pb.Message{e.Publish}}}, pid)
				}
				if e.Partial != nil {
					p.tracer.DropRPC(&RPC{RPC: pb.RPC{Partial: e.Partial}}, pid)
				}
			},
			Violation: p.abortTopicStreamsConnection,
		},
		TopicStreamViolation: p.abortTopicStreamsConnection,
	}, peercomm.Hooks{
		Protocols: p.rt.Protocols,
		PrepareHello: func(ctx context.Context, pid peer.ID, proto protocol.ID, session peercomm.Session) (*pb.RPC, bool) {
			response := make(chan prepareOutboundResponse, 1)
			request := prepareOutboundRequest{peer: pid, protocol: proto, session: session, response: response}
			select {
			case p.prepareOutbound <- request:
			case <-ctx.Done():
				return nil, false
			}
			select {
			case result := <-response:
				return result.rpc, result.ok
			case <-ctx.Done():
				return nil, false
			}
		},
		OutboundOpenFailed: func(pid peer.ID) {
			select {
			case p.newPeerError <- pid:
			case <-p.ctx.Done():
			}
		},
		OutboundDead: p.notifyPeerDead,
		EmitInbound: func(pid peer.ID, ev peercomm.InboundEvent) bool {
			in := incomingUnion{}
			switch ev.Kind {
			case peercomm.InboundRPC, peercomm.InboundTopicRPC:
				in.kind = incomingKindRPC
				in.rpc = wrapPeerRPC(ev.RPC, pid, ev.Kind == peercomm.InboundTopicRPC)
				in.rpc.session = ev.Session
				if ev.Stream != nil {
					in.rpc.conn = ev.Stream.Conn()
				}
			case peercomm.InboundControlOpened:
				in.kind, in.s = incomingKindNewStream, ev.Stream
			case peercomm.InboundControlClosed:
				in.kind, in.s = incomingKindClosedStream, ev.Stream
			}
			select {
			case p.incoming <- in:
				return true
			case <-p.ctx.Done():
				return false
			}
		},
		PenalizeInboundLimit: func(pid peer.ID) { _ = p.PeerFeedback("", pid, PeerFeedbackBehaviorPenalty) },
	})
}

func (p *PubSub) existingPeerComm(pid peer.ID) (*peerComm, bool) {
	if p.peerComms == nil {
		return nil, false
	}
	return p.peerComms.Existing(pid)
}

func wrapPeerRPC(raw *pb.RPC, from peer.ID, viaTopicStream bool) *RPC {
	rpc := &RPC{from: from, viaTopicStream: viaTopicStream}
	proto.Merge(&rpc.RPC, raw)
	return rpc
}
