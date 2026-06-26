package rabbitmq

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/should"
)

func TestTransportFixture(t *testing.T) {
	gunit.Run(new(TransportFixture), t)
}

type TransportFixture struct {
	*gunit.Fixture
	dialer    *recordingDialer
	transport *amqpTransport
}

func (this *TransportFixture) Setup() {
	this.dialer = &recordingDialer{}
	this.transport = &amqpTransport{
		address: "amqp://guest:guest@localhost:5672/",
		dialer:  this.dialer,
	}
}

func (this *TransportFixture) TestHandshakeDeadline_FromContext() {
	deadline := time.Now().Add(2 * time.Minute)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	// connect dials the fake conn, arms the read deadline, then fails the AMQP
	// handshake (the fake conn returns io.EOF on Read) — that error is expected.
	_, err := this.transport.connect(ctx)

	this.So(err, should.NOT.BeNil)
	this.So(this.dialer.conn.deadlineSet, should.BeTrue)
	this.So(this.dialer.conn.readDeadline, should.HappenOn, deadline)
}

func (this *TransportFixture) TestHandshakeDeadline_DefaultWhenNoContextDeadline() {
	approxExpected := time.Now().Add(handshakeTimeout)

	_, err := this.transport.connect(context.Background())

	this.So(err, should.NOT.BeNil)
	this.So(this.dialer.conn.deadlineSet, should.BeTrue)
	this.So(this.dialer.conn.readDeadline, should.HappenWithin, 5*time.Second, approxExpected)
}

// recordingDialer hands back a single recordingConn so a test can inspect the
// read deadline the transport armed before the AMQP handshake.
type recordingDialer struct {
	conn *recordingConn
}

func (this *recordingDialer) DialContext(_ context.Context, _, _ string) (net.Conn, error) {
	this.conn = &recordingConn{}
	return this.conn, nil
}

// recordingConn is a net.Conn that records the read deadline set on it and fails
// the AMQP handshake fast (Read returns io.EOF) so amqp.DialConfig needs no broker.
// SetReadDeadline is only ever called by connect's Dial closure (before amqp.Open
// launches its reader), so the recorded fields are written before any concurrent
// Read and need no locking.
type recordingConn struct {
	deadlineSet  bool
	readDeadline time.Time
}

func (this *recordingConn) Read([]byte) (int, error)    { return 0, io.EOF }
func (this *recordingConn) Write(p []byte) (int, error) { return len(p), nil }
func (this *recordingConn) Close() error                { return nil }
func (this *recordingConn) LocalAddr() net.Addr         { return fakeAddr{} }
func (this *recordingConn) RemoteAddr() net.Addr        { return fakeAddr{} }
func (this *recordingConn) SetDeadline(time.Time) error { return nil }
func (this *recordingConn) SetReadDeadline(deadline time.Time) error {
	this.deadlineSet = true
	this.readDeadline = deadline
	return nil
}
func (this *recordingConn) SetWriteDeadline(time.Time) error { return nil }

type fakeAddr struct{}

func (fakeAddr) Network() string { return "tcp" }
func (fakeAddr) String() string  { return "fake" }
