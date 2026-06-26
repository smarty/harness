package rabbitmq

import (
	"crypto/tls"
	"net"
	"testing"

	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/should"
)

func TestConfigFixture(t *testing.T) {
	gunit.Run(new(ConfigFixture), t)
}

type ConfigFixture struct {
	*gunit.Fixture
}

func (this *ConfigFixture) transportOf(dispatcher *Dispatcher) *amqpTransport {
	result, ok := dispatcher.transport.(*amqpTransport)
	this.So(ok, should.BeTrue)
	return result
}

func (this *ConfigFixture) TestDefaults_AddressSetTLSNilDefaultDialer() {
	transport := this.transportOf(NewDispatcher("amqp://localhost/vhost"))

	this.So(transport.address, should.Equal, "amqp://localhost/vhost")
	this.So(transport.tls, should.BeNil)
	_, isNetDialer := transport.dialer.(*net.Dialer)
	this.So(isNetDialer, should.BeTrue)
}

func (this *ConfigFixture) TestTLSOption_SetsTLSConfig() {
	config := &tls.Config{ServerName: "broker"}

	transport := this.transportOf(NewDispatcher("amqps://localhost", Options.TLS(config)))

	this.So(transport.tls == config, should.BeTrue)
}

func (this *ConfigFixture) TestDialerOption_SetsDialer() {
	dialer := &net.Dialer{}

	transport := this.transportOf(NewDispatcher("amqp://localhost", Options.Dialer(dialer)))

	this.So(transport.dialer == dialer, should.BeTrue)
}

func (this *ConfigFixture) TestAddress_PromotesQueryCredentialsToUserinfo() {
	transport := this.transportOf(NewDispatcher("amqp://host/?username=bob&password=secret"))

	this.So(transport.address, should.Equal, "amqp://bob:secret@host/")
}
