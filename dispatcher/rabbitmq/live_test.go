package rabbitmq

import (
	"bytes"
	"context"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/should"
	"github.com/smarty/harness/v2/contracts"
)

// Integration tests in this package require a local RabbitMQ broker
// (see doc/docker-compose.yml). They are excluded by `-short`.
const liveAddress = "amqp://guest:guest@127.0.0.1:5672/"

func TestLiveDispatcherFixture(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping long-running broker integration tests.")
	}
	ensureBrokerReadiness(t)
	gunit.Run(new(LiveDispatcherFixture), t, gunit.Options.IntegrationTests())
}

func ensureBrokerReadiness(t *testing.T) {
	connection, err := amqp.Dial(liveAddress)
	if err != nil {
		t.Fatal("Broker not available (is rabbitmq running?):", err)
	}
	_ = connection.Close()
}

type LiveDispatcherFixture struct {
	*gunit.Fixture
	ctx      context.Context
	control  *amqp.Connection
	channel  *amqp.Channel
	exchange string
	queue    string
	subject  *Dispatcher
}

func (this *LiveDispatcherFixture) Setup() {
	this.ctx = context.Background()
	this.exchange = "harness-live-test-exchange"

	connection, err := amqp.Dial(liveAddress)
	this.So(err, should.BeNil)
	this.control = connection
	channel, err := connection.Channel()
	this.So(err, should.BeNil)
	this.channel = channel

	// Declare the fanout exchange the dispatcher will publish to, plus an
	// exclusive server-named queue bound to it to capture deliveries.
	err = channel.ExchangeDeclare(this.exchange, "fanout", true, false, false, false, nil)
	this.So(err, should.BeNil)
	queue, err := channel.QueueDeclare("", true, false, true, false, nil)
	this.So(err, should.BeNil)
	this.queue = queue.Name
	err = channel.QueueBind(this.queue, "", this.exchange, false, nil)
	this.So(err, should.BeNil)

	this.subject = NewDispatcher(liveAddress)
}

func (this *LiveDispatcherFixture) Teardown() {
	_ = this.subject.Close()
	_, _ = this.channel.QueueDelete(this.queue, false, false, false)
	_ = this.channel.ExchangeDelete(this.exchange, false, false)
	_ = this.channel.Close()
	_ = this.control.Close()
}

func (this *LiveDispatcherFixture) TestPublishRoundTrip() {
	body := `{"event":"renewed"}`

	err := this.subject.Dispatch(this.ctx, &contracts.Message{
		ID:          1,
		Type:        this.exchange,
		ContentType: "application/json",
		Content:     bytes.NewBufferString(body),
	})

	this.So(err, should.BeNil)

	deliveries, err := this.channel.Consume(this.queue, "", true, false, false, false, nil)
	this.So(err, should.BeNil)
	select {
	case delivery := <-deliveries:
		this.So(string(delivery.Body), should.Equal, body)
		this.So(delivery.Type, should.Equal, this.exchange)
		this.So(delivery.ContentType, should.Equal, "application/json")
		this.So(delivery.DeliveryMode, should.Equal, uint8(amqp.Persistent))
	case <-time.After(3 * time.Second):
		this.So(false, should.BeTrue) // timed out waiting for the published delivery
	}
}
