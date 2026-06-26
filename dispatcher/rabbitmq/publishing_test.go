package rabbitmq

import (
	"bytes"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/should"
	"github.com/smarty/harness/v2/contracts"
)

func TestPublishingFixture(t *testing.T) {
	gunit.Run(new(PublishingFixture), t)
}

type PublishingFixture struct {
	*gunit.Fixture
}

func (this *PublishingFixture) TestMessageMapsToPersistentPublishing() {
	message := &contracts.Message{
		Type:        "subscription:renewed-v2",
		ContentType: "application/json; charset=utf-8",
		Content:     bytes.NewBufferString(`{"hello":"world"}`),
	}

	result := toPublishing(message)

	this.So(result.Type, should.Equal, message.Type)
	this.So(result.ContentType, should.Equal, message.ContentType)
	this.So(result.Body, should.Equal, message.Content.Bytes())
	this.So(result.DeliveryMode, should.Equal, uint8(amqp.Persistent))
}

func (this *PublishingFixture) TestNilContentMapsToNilBody() {
	message := &contracts.Message{
		Type:        "subscription:renewed-v2",
		ContentType: "application/json",
		Content:     nil,
	}

	result := toPublishing(message)

	this.So(result.Body, should.BeNil)
	this.So(result.Type, should.Equal, message.Type)
	this.So(result.ContentType, should.Equal, message.ContentType)
	this.So(result.DeliveryMode, should.Equal, uint8(amqp.Persistent))
}
