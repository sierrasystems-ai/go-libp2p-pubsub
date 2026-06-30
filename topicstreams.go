package pubsub

import (
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/protocol"
	"google.golang.org/protobuf/proto"
)

// TopicStreamsProtocolID is the protocol ID used for topic streams. See the
// Topic Streams extension spec (topic-streams.md).
const TopicStreamsProtocolID = protocol.ID("/gsts/v0beta")

// messageToTopicScoped converts a full Message into a TopicScopedMessage for
// sending on a topic stream. The topic is carried once in the TopicRPCHeader,
// so it is omitted here (the unset_topic_name field is left unset on the wire).
func messageToTopicScoped(msg *pb.Message) *pb.TopicScopedMessage {
	if msg == nil {
		return nil
	}
	ts := &pb.TopicScopedMessage{
		Data:      msg.Data,
		Seqno:     msg.Seqno,
		Signature: msg.Signature,
		Key:       msg.Key,
	}
	if msg.From != nil {
		// Message.from is bytes; TopicScopedMessage.from is string. They share
		// the same protobuf wire encoding, so this preserves the bytes used to
		// compute the signature.
		ts.From = proto.String(string(msg.From))
	}
	return ts
}

// topicScopedToMessage reconstructs the full Message from a TopicScopedMessage
// received on a topic stream, setting the topic from the stream's
// TopicRPCHeader so the message ID can be computed and the signature verified.
func topicScopedToMessage(ts *pb.TopicScopedMessage, topic string) *pb.Message {
	if ts == nil {
		return nil
	}
	msg := &pb.Message{
		Data:      ts.Data,
		Seqno:     ts.Seqno,
		Signature: ts.Signature,
		Key:       ts.Key,
		Topic:     proto.String(topic),
	}
	if ts.From != nil {
		msg.From = []byte(ts.GetFrom())
	}
	return msg
}
