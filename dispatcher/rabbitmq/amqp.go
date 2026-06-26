package rabbitmq

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// handshakeTimeout bounds the AMQP protocol handshake when the caller's context
// carries no deadline. It mirrors amqp091's DefaultDial default, which our custom
// Dial closure bypasses; the deadline is lifted by amqp091 once connected.
const handshakeTimeout = 30 * time.Second

// ContextDialer establishes the underlying TCP connection for the AMQP dial.
// *net.Dialer satisfies it, and it is the default; callers override it through
// Options.Dialer (e.g. to honor a cancellable pipeline context during dialing).
type ContextDialer interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

// amqpTransport is the default transport: it dials a RabbitMQ broker over
// amqp091-go and hands back a transaction-mode channel. PLAIN SASL credentials
// and the vhost are taken from the address URL by amqp.DialConfig.
type amqpTransport struct {
	address string
	tls     *tls.Config
	dialer  ContextDialer
}

func (this *amqpTransport) connect(ctx context.Context) (channel, error) {
	config := amqp.Config{
		TLSClientConfig: this.tls,
		Dial: func(network, address string) (net.Conn, error) {
			connection, err := this.dialer.DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}
			// amqp.Open is not context-aware: once the socket is up, ctx
			// cancellation can't interrupt the handshake read. Arm a read
			// deadline (honoring the caller's deadline, else handshakeTimeout)
			// so the single broadcast goroutine stays interruptible. amqp091
			// clears it in openComplete once connected.
			deadline, ok := ctx.Deadline()
			if !ok {
				deadline = time.Now().Add(handshakeTimeout)
			}
			if err := connection.SetReadDeadline(deadline); err != nil {
				_ = connection.Close()
				return nil, err
			}
			return connection, nil
		},
	}
	connection, err := amqp.DialConfig(this.address, config)
	if err != nil {
		return nil, err
	}
	transacted, err := connection.Channel()
	if err != nil {
		_ = connection.Close()
		return nil, err
	}
	if err := transacted.Tx(); err != nil {
		_ = transacted.Close()
		_ = connection.Close()
		return nil, err
	}
	return &amqpChannel{connection: connection, channel: transacted}, nil
}

// amqpChannel is a transaction-mode channel and the connection it rode in on,
// kept together so close() tears down both.
type amqpChannel struct {
	connection *amqp.Connection
	channel    *amqp.Channel
}

func (this *amqpChannel) publish(ctx context.Context, exchange string, msg amqp.Publishing) error {
	return this.channel.PublishWithContext(ctx, exchange, "", false, false, msg)
}
func (this *amqpChannel) commit() error {
	return this.channel.TxCommit()
}
func (this *amqpChannel) close() error {
	return errors.Join(this.channel.Close(), this.connection.Close())
}
