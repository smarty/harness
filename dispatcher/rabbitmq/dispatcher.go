// Package rabbitmq provides the bundled default contracts.Dispatcher for
// harness, publishing messages to RabbitMQ over AMQP. It is the dispatcher
// analog of storage/mysql: a thin, direct implementation over the
// github.com/rabbitmq/amqp091-go driver rather than a messaging framework.
//
// NewDispatcher promotes ?username=&password= query credentials into the URL
// userinfo (see credentials.go), where amqp091 reads them, so a single
// messaging/v3-style address works for both the consumer and the dispatcher.
package rabbitmq

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/smarty/harness/v2/contracts"
)

// Dispatcher publishes batches of messages to RabbitMQ. It owns a lazily
// established, long-lived transaction-mode channel and reconnects on the next
// Dispatch after any failure. The broadcast stage drives it from a single
// goroutine, so the cached channel and reused buffer need no locking.
type Dispatcher struct {
	transport transport
	channel   channel
	buffer    []amqp.Publishing
}

// Dispatch publishes every message as a persistent delivery to the exchange
// named by its Type, then commits the transaction. It returns nil only after a
// clean commit (a durable broker ack); on any publish or commit failure it
// resets the connection and returns the error, so the next call reconnects.
func (this *Dispatcher) Dispatch(ctx context.Context, messages ...*contracts.Message) error {
	if len(messages) == 0 {
		return nil
	}
	current, err := this.ensureChannel(ctx)
	if err != nil {
		return err
	}
	clear(this.buffer)
	this.buffer = this.buffer[:0]
	for _, message := range messages {
		this.buffer = append(this.buffer, toPublishing(message))
	}
	for i, publishing := range this.buffer {
		if err := current.publish(ctx, messages[i].Type, publishing); err != nil {
			this.reset()
			return err
		}
	}
	if err := current.commit(); err != nil {
		this.reset()
		return err
	}
	return nil
}

// Close releases the cached channel/connection. It is an io.Closer for
// dominoes-managed shutdown and is a safe no-op when nothing is cached.
func (this *Dispatcher) Close() error {
	if this.channel == nil {
		return nil
	}
	err := this.channel.close()
	this.channel = nil
	return err
}

func (this *Dispatcher) ensureChannel(ctx context.Context) (channel, error) {
	if this.channel != nil {
		return this.channel, nil
	}
	current, err := this.transport.connect(ctx)
	if err != nil {
		return nil, err
	}
	this.channel = current
	return current, nil
}

func (this *Dispatcher) reset() {
	if this.channel == nil {
		return
	}
	_ = this.channel.close()
	this.channel = nil
}

// transport is the seam the Dispatcher depends on for connectivity. The default
// implementation (amqp.go) wraps amqp091-go; tests substitute a fake so the unit
// suite needs no live broker.
type transport interface {
	// connect dials the broker, opens a connection and channel, and puts the
	// channel into transaction mode (Tx), returning it ready to publish.
	connect(ctx context.Context) (channel, error)
}

// channel is a transaction-mode AMQP channel the Dispatcher publishes a batch
// through and then commits.
type channel interface {
	publish(ctx context.Context, exchange string, msg amqp.Publishing) error
	commit() error
	close() error
}
