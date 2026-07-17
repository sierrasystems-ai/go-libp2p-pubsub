package peercomm

import (
	"context"

	"github.com/libp2p/go-libp2p/core/peer"
)

// inboundDelivery serializes external PubSub callbacks outside the peer actor.
// Its bounded mailbox makes overload explicit; cancellation settles queued work
// without waiting for a callback that does not honor cancellation.
type inboundDelivery struct {
	ctx       context.Context
	peer      peer.ID
	hook      func(peer.ID, InboundEvent) bool
	onFailure func()
	queue     chan deliveryRequest
	slots     chan struct{}
}

type deliveryRequest struct {
	event    InboundEvent
	reply    chan bool
	reserved bool
}

func newInboundDelivery(ctx context.Context, pid peer.ID, hook func(peer.ID, InboundEvent) bool, onFailure func()) *inboundDelivery {
	d := &inboundDelivery{ctx: ctx, peer: pid, hook: hook, onFailure: onFailure, queue: make(chan deliveryRequest, mailboxSize), slots: make(chan struct{}, mailboxSize)}
	go d.run()
	return d
}

func (d *inboundDelivery) reserve() bool {
	select {
	case d.slots <- struct{}{}:
		return true
	case <-d.ctx.Done():
		return false
	}
}

func (d *inboundDelivery) release() { <-d.slots }

func (d *inboundDelivery) submit(event InboundEvent, reply chan bool, reserved bool) bool {
	if d.hook == nil {
		if reserved {
			d.release()
		}
		if reply != nil {
			reply <- true
		}
		return true
	}
	select {
	case d.queue <- deliveryRequest{event: event, reply: reply, reserved: reserved}:
		return true
	case <-d.ctx.Done():
		if reserved {
			d.release()
		}
		if reply != nil {
			reply <- false
		}
		return false
	default:
		if reserved {
			d.release()
		}
		if reply != nil {
			reply <- false
		}
		return false
	}
}

func (d *inboundDelivery) run() {
	for {
		select {
		case request := <-d.queue:
			completed := make(chan bool, 1)
			go func() { completed <- d.hook(d.peer, request.event) }()
			select {
			case ok := <-completed:
				if request.reserved {
					d.release()
				}
				if request.reply != nil {
					request.reply <- ok
				}
				if !ok && d.onFailure != nil {
					d.onFailure()
				}
			case <-d.ctx.Done():
				if request.reserved {
					d.release()
				}
				if request.reply != nil {
					request.reply <- false
				}
				return
			}
		case <-d.ctx.Done():
			return
		}
	}
}
