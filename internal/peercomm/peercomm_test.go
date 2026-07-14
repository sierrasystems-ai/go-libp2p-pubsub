package peercomm

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

func TestRPCQueuePriorityAndCapacity(t *testing.T) {
	q := newRPCQueue(2)
	normal := &pb.RPC{}
	urgent := &pb.RPC{}
	if err := q.push(normal, false, false); err != nil {
		t.Fatal(err)
	}
	if err := q.push(urgent, true, false); err != nil {
		t.Fatal(err)
	}
	if err := q.push(&pb.RPC{}, false, false); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("expected queue full, got %v", err)
	}
	got, err := q.pop(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got != urgent {
		t.Fatal("urgent RPC was not drained first")
	}
	got, err = q.pop(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got != normal {
		t.Fatal("normal RPC was not preserved")
	}
}

func TestRPCQueueCloseRejectsSend(t *testing.T) {
	q := newRPCQueue(1)
	q.close()
	if err := q.push(&pb.RPC{}, false, false); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("expected queue closed, got %v", err)
	}
}

func TestOutboundTerminalCallbackCanRestart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var callbacks atomic.Int32
	var actor *Actor
	registry := NewRegistry(ctx, Config{OutboundQueueSize: 1}, Hooks{
		OutboundDead: func(peer.ID) {
			if callbacks.Add(1) == 1 {
				actor.Start(time.Hour)
			}
		},
	})
	actor = registry.For(peer.ID("restart-peer"))
	actor.started.Store(true)
	actor.outboundTerminated(true)
	if callbacks.Load() != 1 {
		t.Fatalf("expected one terminal callback, got %d", callbacks.Load())
	}
	if !actor.started.Load() {
		t.Fatal("restart requested by terminal callback was lost")
	}
}

func TestRPCQueuePopCancellationReturnsPromptly(t *testing.T) {
	q := newRPCQueue(1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := q.pop(ctx); done <- err }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected cancellation, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("queue pop remained blocked after cancellation")
	}
}

func TestRegistryExistingReportsPresence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registry := NewRegistry(ctx, Config{OutboundQueueSize: 1}, Hooks{})
	pid := peer.ID("existing-peer")
	if actor, ok := registry.Existing(pid); ok || actor != nil {
		t.Fatal("missing actor reported as existing")
	}
	created := registry.For(pid)
	if actor, ok := registry.Existing(pid); !ok || actor != created {
		t.Fatal("existing actor was not returned with ok=true")
	}
}

func TestClosedControlActorRejectsSend(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registry := NewRegistry(ctx, Config{OutboundQueueSize: 1}, Hooks{})
	actor := registry.For(peer.ID("retiring-peer"))
	generation, ok := actor.OpenInboundControl(nil)
	if !ok {
		t.Fatal("control stream was not accepted")
	}
	actor.CloseInboundControl(generation, nil)
	select {
	case <-actor.Done():
	case <-time.After(time.Second):
		t.Fatal("actor did not retire after its control stream closed")
	}
	if err := actor.Send(&pb.RPC{}, false); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("retired actor Send returned %v, want %v", err, ErrQueueClosed)
	}
}
