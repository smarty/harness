package rabbitmq

import (
	"testing"

	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/should"
)

func TestCredentialsFixture(t *testing.T) {
	gunit.Run(new(CredentialsFixture), t)
}

type CredentialsFixture struct {
	*gunit.Fixture
}

func (this *CredentialsFixture) assertPromoted(input, expected string) {
	this.So(promoteCredentials(input), should.Equal, expected)
}

func (this *CredentialsFixture) TestPromotesQueryCredentialsToUserinfo() {
	this.assertPromoted(
		"amqp://rabbit.service.consul/?username=bob&password=secret",
		"amqp://bob:secret@rabbit.service.consul/",
	)
}
func (this *CredentialsFixture) TestPromotesQueryCredentialsKeepingOtherParams() {
	this.assertPromoted(
		"amqp://rabbit.service.consul/?username=bob&password=secret&server-name=rabbit",
		"amqp://bob:secret@rabbit.service.consul/?server-name=rabbit",
	)
}
func (this *CredentialsFixture) TestPercentEncodesCredentials() {
	this.assertPromoted(
		"amqp://rabbit.service.consul/?username=bob&password=p@ss/word",
		"amqp://bob:p%40ss%2Fword@rabbit.service.consul/",
	)
}
func (this *CredentialsFixture) TestUserinfoTakesPrecedenceOverQuery() {
	this.assertPromoted(
		"amqp://alice:realpass@rabbit.service.consul/?username=bob&password=secret",
		"amqp://alice:realpass@rabbit.service.consul/",
	)
}
func (this *CredentialsFixture) TestFillsMissingFieldFromQuery() {
	this.assertPromoted(
		"amqp://alice@rabbit.service.consul/?username=bob&password=secret",
		"amqp://alice:secret@rabbit.service.consul/",
	)
}
func (this *CredentialsFixture) TestUnchangedWhenNoQueryCredentials() {
	this.assertPromoted(
		"amqp://alice:realpass@rabbit.service.consul/",
		"amqp://alice:realpass@rabbit.service.consul/",
	)
}
func (this *CredentialsFixture) TestUnchangedWhenNoCredentialsAtAll() {
	this.assertPromoted(
		"amqp://rabbit.service.consul/",
		"amqp://rabbit.service.consul/",
	)
}
func (this *CredentialsFixture) TestUnchangedWhenUnparseable() {
	this.assertPromoted("://nonsense", "://nonsense")
}
