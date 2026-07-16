package pubsub

import (
	"time"

	"github.com/libp2p/go-libp2p-pubsub/internal/topicstreams"
	"github.com/libp2p/go-libp2p/core/network"
)

const TopicStreamsProtocolID = topicstreams.ProtocolID
const topicStreamsProtocolViolationError = topicstreams.ProtocolViolationError
const (
	topicStreamFirstControlTimeout = 30 * time.Second
	recentlyUnsubscribedTTL        = 10 * time.Second
)

func (p *PubSub) abortTopicStreamsConnection(conn network.Conn, reason string) {
	p.logger.Warn("topic streams protocol violation; closing connection", "peer", conn.RemotePeer(), "reason", reason)
	if err := conn.CloseWithError(topicStreamsProtocolViolationError); err != nil {
		p.logger.Debug("failed to close connection after topic streams protocol violation", "peer", conn.RemotePeer(), "err", err)
	}
}

func (p *PubSub) handleNewTopicStream(s network.Stream) {
	if actor := p.peerComms.For(s.Conn().RemotePeer()); actor != nil {
		actor.HandleInboundTopicStream(s)
	} else {
		_ = s.Reset()
	}
}
