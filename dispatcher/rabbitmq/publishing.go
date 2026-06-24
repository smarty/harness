package rabbitmq

import (
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/smarty/harness/v2/contracts"
)

// toPublishing maps a *contracts.Message to a persistent amqp.Publishing,
// ported from messaging/v3's publish mapping and narrowed to the fields harness
// populates. The message is published to the exchange named by its Type (see
// Dispatcher.Dispatch); the Type field is carried on the publishing as well.
// amqp091 copies Body into the wire frame during PublishWithContext, so the
// pooled Content buffer is never retained past the publish call.
//
// Content is normally populated by the serialization stage before broadcast; a
// nil Content (only reachable by direct misuse of the public type) maps to a nil
// Body rather than panicking, which amqp091 treats as a valid empty payload.
func toPublishing(message *contracts.Message) amqp.Publishing {
	var body []byte
	if message.Content != nil {
		body = message.Content.Bytes()
	}
	return amqp.Publishing{
		Type:         message.Type,
		ContentType:  message.ContentType,
		Body:         body,
		DeliveryMode: amqp.Persistent,
	}
}
