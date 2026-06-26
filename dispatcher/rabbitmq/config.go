package rabbitmq

import (
	"crypto/tls"
	"net"
)

// NewDispatcher builds a Dispatcher that publishes to the RabbitMQ broker at the
// given address (an amqp:// or amqps:// URL carrying credentials and vhost). It
// mirrors mysql.NewMapper's shape: a required handle/address followed by the
// repo-wide Options-singleton functional options. The package owns the connection
// lifecycle (unlike database/sql), so it takes coordinates rather than a live handle.
func NewDispatcher(address string, options ...option) *Dispatcher {
	transport := &amqpTransport{address: promoteCredentials(address)}
	for _, option := range append(Options.defaults(), options...) {
		option(transport)
	}
	return &Dispatcher{transport: transport}
}

type option func(*amqpTransport)

var Options singleton

type singleton struct{}

func (singleton) TLS(config *tls.Config) option {
	return func(transport *amqpTransport) { transport.tls = config }
}
func (singleton) Dialer(dialer ContextDialer) option {
	return func(transport *amqpTransport) { transport.dialer = dialer }
}
func (singleton) defaults() []option {
	return []option{
		Options.Dialer(new(net.Dialer)),
	}
}
