package sqladapter

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/should"
	"github.com/smarty/harness/v2/internal/contracts"
)

func TestRecoveryFixture(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping long-running database tests.")
	}
	ensureDatabaseReadiness(t)
	gunit.Run(new(RecoveryFixture), t, gunit.Options.IntegrationTests())
}

type RecoveryFixture struct {
	*gunit.Fixture
	ctx     context.Context
	handle  *sql.DB
	output  chan *contracts.Message
	waits   []time.Duration
	waitErr error
	tracked []any
	subject *Recovery
}

func (this *RecoveryFixture) Setup() {
	this.ctx = context.WithValue(this.Context(), "testing", this.Name())
	this.output = make(chan *contracts.Message, 64)
	this.waits = nil
	this.waitErr = nil
	this.tracked = nil
	handle, err := openTestDatabase()
	this.So(err, should.BeNil)
	this.handle = handle
	_, err = handle.Exec(`TRUNCATE TABLE Messages;`)
	this.So(err, should.BeNil)
	this.subject = NewRecovery(this.ctx, handle, this.output, this.wait, this)
}

func (this *RecoveryFixture) Teardown() {
	_ = this.handle.Close()
}

func (this *RecoveryFixture) wait(_ context.Context, d time.Duration) error {
	this.waits = append(this.waits, d)
	return this.waitErr
}

func (this *RecoveryFixture) Track(observation any) {
	this.tracked = append(this.tracked, observation)
}

func (this *RecoveryFixture) seedUndispatched(typeName string, payload string) uint64 {
	result, err := this.handle.Exec(`INSERT INTO Messages (type, payload) VALUES (?, ?)`, typeName, payload)
	this.So(err, should.BeNil)
	id, err := result.LastInsertId()
	this.So(err, should.BeNil)
	return uint64(id)
}

func (this *RecoveryFixture) seedDispatched(typeName string, payload string) uint64 {
	result, err := this.handle.Exec(`INSERT INTO Messages (type, payload, dispatched) VALUES (?, ?, NOW(3))`, typeName, payload)
	this.So(err, should.BeNil)
	id, err := result.LastInsertId()
	this.So(err, should.BeNil)
	return uint64(id)
}

func (this *RecoveryFixture) receivedMessages() (results []*contracts.Message) {
	for {
		select {
		case message := <-this.output:
			results = append(results, message)
		default:
			return results
		}
	}
}

func (this *RecoveryFixture) TestListen_NoOrphans_NoOp() {
	this.subject.Listen()

	this.So(this.receivedMessages(), should.BeEmpty)
	this.So(this.waits, should.BeEmpty)
	this.So(this.tracked, should.BeEmpty)
}

func (this *RecoveryFixture) TestListen_FeedsUndispatchedRowsToChannelInIDOrder() {
	id1 := this.seedUndispatched("order-received", `{"order":1}`)
	id2 := this.seedUndispatched("order-approved", `{"order":2}`)
	_ = this.seedDispatched("order-received", `{"order":3}`) // already dispatched, must be skipped

	this.subject.Listen()

	messages := this.receivedMessages()
	this.So(len(messages), should.Equal, 2)
	this.So(messages[0].ID, should.Equal, id1)
	this.So(messages[1].ID, should.Equal, id2)
	this.So(this.tracked, should.BeEmpty)
}

func (this *RecoveryFixture) TestListen_PassesPayloadAndTypeIntoMessage() {
	id := this.seedUndispatched("order-received", `{"order":1}`)

	this.subject.Listen()

	messages := this.receivedMessages()
	this.So(len(messages), should.Equal, 1)
	message := messages[0]
	this.So(message.ID, should.Equal, id)
	this.So(message.Type, should.Equal, "order-received")
	this.So(message.ContentType, should.Equal, "application/json")
	this.So(message.Content.Bytes(), should.Equal, []byte(`{"order":1}`))
	this.So(message.Value, should.BeNil)
}

func (this *RecoveryFixture) TestListen_QueryError_TracksErrorAndRetries() {
	_ = this.handle.Close() // force query errors
	calls := 0
	this.subject = NewRecovery(this.ctx, this.handle, this.output, func(ctx context.Context, d time.Duration) error {
		this.waits = append(this.waits, d)
		calls++
		if calls == 2 {
			return context.Canceled // stop the retry loop
		}
		return nil
	}, this)

	this.subject.Listen()

	this.So(this.receivedMessages(), should.BeEmpty)
	this.So(this.waits, should.Equal, []time.Duration{time.Second, time.Second})
	this.So(len(this.tracked), should.Equal, 1)
	tracked, ok := this.tracked[0].(contracts.RecoveryError)
	this.So(ok, should.BeTrue)
	this.So(tracked.Attempts, should.Equal, 1)
	this.So(tracked.Error, should.NOT.BeNil)
}

func (this *RecoveryFixture) TestListen_QueryError_ContextCanceledDuringWait_Abandons() {
	_ = this.handle.Close() // force query errors
	this.waitErr = context.Canceled

	this.subject.Listen()

	this.So(this.receivedMessages(), should.BeEmpty)
	this.So(this.waits, should.Equal, []time.Duration{time.Second})
	this.So(this.tracked, should.BeEmpty) // wait failed before the error was tracked
}
