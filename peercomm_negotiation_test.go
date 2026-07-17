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
	extensions.OnNewOutboundStream(pid, &RPC{session: actor.CurrentSession()})
	remoteSupportsTopicStreams := true
	rpc := &RPC{session: actor.CurrentSession(), RPC: pb.RPC{Control: &pb.ControlMessage{Extensions: &pb.ControlExtensions{TopicStreams: &remoteSupportsTopicStreams}}}, from: pid}
	candidate, disposition := extensions.validateInboundRPC(rpc)
	if disposition != inboundRPCAccept {
		t.Fatalf("first negotiation RPC was rejected: %v", disposition)
	}
	extensions.commitInboundRPC(candidate)
	if err := extensions.HandleRPC(rpc); err != nil {
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
	extensions.onTopicStreamsDisabled(unknown, peercomm.Session{})
	if _, ok := registry.Existing(unknown); ok {
		t.Fatal("disable event created a peer actor")
	}
}

func TestRejectedInitialAdvertisementDoesNotCommitExtensions(t *testing.T) {
	pid := peer.ID("rejected-peer")
	extensions := newExtensionsState(PeerExtensions{TopicStreams: true}, func(peer.ID) {}, func(peer.ID, *RPC, bool) {})
	extensions.OnNewOutboundStream(pid, &RPC{})
	remoteSupportsTopicStreams := true
	rpc := &RPC{RPC: pb.RPC{Control: &pb.ControlMessage{Extensions: &pb.ControlExtensions{TopicStreams: &remoteSupportsTopicStreams}}}, from: pid}

	rejected, disposition := extensions.validateInboundRPC(rpc)
	if disposition != inboundRPCAccept {
		t.Fatalf("candidate validation failed: %v", disposition)
	}
	// Model a later admission gate rejecting the RPC by deliberately not committing.
	if extensions.topicStreamsNegotiated(pid) {
		t.Fatal("validation alone enabled Topic Streams")
	}

	accepted, disposition := extensions.validateInboundRPC(rpc)
	if disposition != inboundRPCAccept || !accepted.first || !rejected.first {
		t.Fatalf("valid retry was not preserved as the initial advertisement: %#v", accepted)
	}
	extensions.commitInboundRPC(accepted)
	if !extensions.topicStreamsNegotiated(pid) {
		t.Fatal("accepted initial advertisement did not enable Topic Streams")
	}
}
