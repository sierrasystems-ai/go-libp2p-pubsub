package pubsub

import (
	"context"
	"testing"

	"github.com/libp2p/go-libp2p-pubsub/internal/peercomm"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

func TestTopicStreamsNegotiationUpdatesExistingPeerComm(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registry := peercomm.NewRegistry(ctx, peercomm.Config{OutboundQueueSize: 1}, peercomm.Hooks{})
	p := &PubSub{peerComms: registry}
	extensions := newExtensionsState(PeerExtensions{TopicStreams: true}, func(peer.ID) {}, func(peer.ID, *RPC, bool) {})
	wireTopicStreamsPeerComm(extensions, p)

	pid := peer.ID("negotiated-peer")
	actor := registry.For(pid)
	extensions.OnNewOutboundStream(pid, &RPC{})
	remoteSupportsTopicStreams := true
	if err := extensions.HandleRPC(&RPC{RPC: pb.RPC{Control: &pb.ControlMessage{Extensions: &pb.ControlExtensions{TopicStreams: &remoteSupportsTopicStreams}}}, from: pid}); err != nil {
		t.Fatal(err)
	}
	if !actor.TopicStreamsEnabled() {
		t.Fatal("mutual negotiation did not enable actor routing")
	}
	extensions.OnClosedOutboundStream(pid)
	if actor.TopicStreamsEnabled() {
		t.Fatal("negotiation teardown did not disable actor routing")
	}

	unknown := peer.ID("unknown-peer")
	extensions.onTopicStreamsDisabled(unknown)
	if _, ok := registry.Existing(unknown); ok {
		t.Fatal("disable event created a peer actor")
	}
}
