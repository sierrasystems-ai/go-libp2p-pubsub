package pubsub

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/libp2p/go-libp2p-pubsub/internal/peercomm"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

type peerComm = peercomm.Actor
type peerCommRegistry = peercomm.Registry

func (p *PubSub) initPeerComms() {
	p.peerComms = peercomm.NewRegistry(p.ctx, peercomm.Config{
		Host:                  p.host,
		OutboundQueueSize:     p.peerOutboundQueueSize,
		MaxMessageSize:        p.maxMessageSize,
		MaxControlMessageSize: p.maxControlMessageSize,
		Logger:                p.rpcLogger,
	}, peercomm.Hooks{
		Protocols: p.rt.Protocols,
		PrepareHello: func(ctx context.Context, pid peer.ID, proto protocol.ID) (*pb.RPC, bool) {
			response := make(chan prepareOutboundResponse, 1)
			request := prepareOutboundRequest{peer: pid, protocol: proto, response: response}
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
			case peercomm.InboundRPC:
				in.kind = incomingKindRPC
				in.rpc = wrapPeerRPC(ev.RPC, pid)
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
	})
}

func (p *PubSub) existingPeerComm(pid peer.ID) (*peerComm, bool) {
	if p.peerComms == nil {
		return nil, false
	}
	return p.peerComms.Existing(pid)
}

func wrapPeerRPC(raw *pb.RPC, from peer.ID) *RPC {
	rpc := &RPC{from: from}
	proto.Merge(&rpc.RPC, raw)
	return rpc
}
