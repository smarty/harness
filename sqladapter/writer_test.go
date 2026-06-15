package sqladapter

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/should"
	"github.com/smarty/harness/v2/contracts"
)

// Local test-only message types mirror the shape the source used.
type (
	orderReceived struct {
		AccountID uint64
		OrderID   uint64
		Timestamp time.Time
	}
	orderApproved struct {
		AccountID uint64
		OrderID   uint64
		Timestamp time.Time
	}
)

func TestWriterFixture(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping long-running database tests.")
	}
	ensureDatabaseReadiness(t)
	gunit.Run(new(WriterFixture), t, gunit.Options.IntegrationTests())
}

type WriterFixture struct {
	*gunit.Fixture
	ctx     context.Context
	handle  *sql.DB
	subject *Writer

	legacyWriteCalls [][]any
	legacyWritePanic any
	testStride       uint64
}

func (this *WriterFixture) Setup() {
	this.ctx = context.WithValue(this.Context(), "testing", this.Name())
	handle, err := openTestDatabase()
	this.So(err, should.BeNil)
	this.handle = handle
	this.legacyWriteCalls = nil
	this.legacyWritePanic = nil
	this.testStride = 7
	this.subject = NewWriter(handle, this.testStride, this.fakeLegacyWrite)
	this.truncateTables()
}

func (this *WriterFixture) fakeLegacyWrite(ctx context.Context, _ *sql.Tx, messages ...any) {
	this.So(ctx.Value("testing"), should.Equal, this.Name())
	this.legacyWriteCalls = append(this.legacyWriteCalls, messages)
	if this.legacyWritePanic != nil {
		panic(this.legacyWritePanic)
	}
}

func (this *WriterFixture) Teardown() {
	_ = this.handle.Close()
}

func (this *WriterFixture) truncateTables() {
	_, err := this.handle.Exec(`TRUNCATE TABLE Messages;`)
	this.So(err, should.BeNil)
}

func (this *WriterFixture) TestWrite_InsertsMessageRowAndInvokesLegacyWrite() {
	event := orderReceived{AccountID: 1, OrderID: 2, Timestamp: time.Now().UTC().Round(time.Second)}
	message := serializedMessage(event, "")

	err := this.subject.Write(this.ctx, message)

	this.So(err, should.BeNil)
	this.So(this.countMessages(), should.Equal, 1)
	this.So(this.legacyWriteCalls, should.Equal, [][]any{{event}})
}

func (this *WriterFixture) TestWrite_LegacyWritePanic_RollsBackInsertedMessage() {
	this.legacyWritePanic = "boom"
	event := orderReceived{AccountID: 1, OrderID: 2, Timestamp: time.Now().UTC()}

	err := this.subject.Write(this.ctx, serializedMessage(event, ""))

	this.So(err, should.NOT.BeNil)
	this.So(this.countMessages(), should.Equal, 0)
}

func (this *WriterFixture) TestWrite_PreservesPreSetMessageType() {
	event := orderReceived{AccountID: 1, OrderID: 2, Timestamp: time.Now().UTC()}
	message := serializedMessage(event, "explicit.override")

	err := this.subject.Write(this.ctx, message)

	this.So(err, should.BeNil)
	this.So(this.firstMessageType(), should.Equal, "explicit.override")
}

func (this *WriterFixture) TestWrite_PopulatesMessageIDFromAutoincrement() {
	event1 := orderReceived{AccountID: 1, OrderID: 2, Timestamp: time.Now().UTC()}
	event2 := orderReceived{AccountID: 1, OrderID: 3, Timestamp: time.Now().UTC()}
	m1 := serializedMessage(event1, "")
	m2 := serializedMessage(event2, "")

	err := this.subject.Write(this.ctx, m1, m2)

	this.So(err, should.BeNil)
	this.So(m1.ID, should.NOT.Equal, uint64(0))
	this.So(m2.ID, should.Equal, m1.ID+this.testStride)
}

func (this *WriterFixture) TestWrite_NoMessages_NoOp() {
	err := this.subject.Write(this.ctx)
	this.So(err, should.BeNil)
	this.So(this.countMessages(), should.Equal, 0)
}

func (this *WriterFixture) TestWrite_ReuseBookkeeping() {
	event := orderReceived{AccountID: 1, OrderID: 2, Timestamp: time.Now().UTC().Round(time.Second)}
	message := serializedMessage(event, "")

	_ = this.subject.Write(this.ctx, message)
	_ = this.subject.Write(this.ctx, message) // multiple calls reset internally re-used values (statement, args, etc...)

	this.So(this.countMessages(), should.Equal, 2)
}

// TestConcurrentWriters_AssignedIDsMatchActualRows exercises the central claim
// documented on assignIDs: a single multi-row "simple insert" reserves a block
// of consecutive auto-increment values spaced by stride, and this holds even
// when several connections insert into the Messages table at once (MySQL's
// innodb_autoinc_lock_mode = 2 reserves a contiguous block per statement). Each
// writer stamps a globally unique payload onto every message; after the storm,
// the assigned IDs must form a partition of the table with no collisions and
// each ID must point at the exact row that writer wrote. If concurrent inserts
// ever interleaved within a single statement's reserved block, an assigned ID
// would land on another writer's row and the payload comparison would catch it.
func (this *WriterFixture) TestConcurrentWriters_AssignedIDsMatchActualRows() {
	const (
		writers          = 8
		batchesPerWriter = 20
		messagesPerBatch = 5
	)
	stride := this.autoIncrementIncrement()

	records := make([][]writeRecord, writers)
	errs := make([]error, writers)
	var waiter sync.WaitGroup
	for w := range writers {
		waiter.Add(1)
		go func() {
			defer waiter.Done()
			records[w], errs[w] = this.runWriter(w, stride, batchesPerWriter, messagesPerBatch)
		}()
	}
	waiter.Wait()

	expected := make(map[uint64]string)
	for w := range writers {
		this.So(errs[w], should.BeNil)
		for _, record := range records[w] {
			_, duplicate := expected[record.id]
			this.So(duplicate, should.BeFalse) // assigned IDs must never collide across writers
			expected[record.id] = record.payload
		}
	}

	this.So(this.allRows(), should.Equal, expected)
}

func (this *WriterFixture) runWriter(writerID int, stride uint64, batches, perBatch int) (results []writeRecord, err error) {
	writer := NewWriter(this.handle, stride, func(context.Context, *sql.Tx, ...any) {})
	for batch := range batches {
		messages := make([]*contracts.Message, 0, perBatch)
		for m := range perBatch {
			event := orderReceived{AccountID: uint64(writerID), OrderID: uint64(batch*perBatch + m)}
			messages = append(messages, serializedMessage(event, ""))
		}
		if err := writer.Write(this.ctx, messages...); err != nil {
			return nil, err
		}
		for _, message := range messages {
			results = append(results, writeRecord{id: message.ID, payload: string(message.Content.Bytes())})
		}
	}
	return results, nil
}

type writeRecord struct {
	id      uint64
	payload string
}

func (this *WriterFixture) autoIncrementIncrement() uint64 {
	var result uint64
	err := this.handle.QueryRow(`SELECT @@auto_increment_increment`).Scan(&result)
	this.So(err, should.BeNil)
	return result
}

func (this *WriterFixture) allRows() map[uint64]string {
	rows, err := this.handle.Query(`SELECT id, payload FROM Messages`)
	this.So(err, should.BeNil)
	defer func() { _ = rows.Close() }()
	results := make(map[uint64]string)
	for rows.Next() {
		var id uint64
		var payload []byte
		this.So(rows.Scan(&id, &payload), should.BeNil)
		results[id] = string(payload)
	}
	this.So(rows.Err(), should.BeNil)
	return results
}

func TestAssignIDsFixture(t *testing.T) {
	gunit.Run(new(AssignIDsFixture), t)
}

// AssignIDsFixture exercises the pure ID-derivation logic without a database,
// covering the stride arithmetic and the defensive guards that a live MySQL
// server would never naturally trip.
type AssignIDsFixture struct {
	*gunit.Fixture
}

func (this *AssignIDsFixture) writer(stride uint64) *Writer {
	return NewWriter(nil, stride, nil)
}
func (this *AssignIDsFixture) messages(count int) (results []*contracts.Message) {
	for range count {
		results = append(results, &contracts.Message{})
	}
	return results
}

func (this *AssignIDsFixture) TestStridedIDsDerivedFromFirstIdentity() {
	messages := this.messages(3)

	err := this.writer(7).assignIDs(messages, 3, 100)

	this.So(err, should.BeNil)
	this.So(messages[0].ID, should.Equal, uint64(100))
	this.So(messages[1].ID, should.Equal, uint64(107))
	this.So(messages[2].ID, should.Equal, uint64(114))
}

func (this *AssignIDsFixture) TestZeroStrideDefaultsToOne() {
	messages := this.messages(3)

	err := this.writer(0).assignIDs(messages, 3, 5)

	this.So(err, should.BeNil)
	this.So(messages[0].ID, should.Equal, uint64(5))
	this.So(messages[1].ID, should.Equal, uint64(6))
	this.So(messages[2].ID, should.Equal, uint64(7))
}

func (this *AssignIDsFixture) TestSingleMessage() {
	messages := this.messages(1)

	err := this.writer(7).assignIDs(messages, 1, 42)

	this.So(err, should.BeNil)
	this.So(messages[0].ID, should.Equal, uint64(42))
}

func (this *AssignIDsFixture) TestRowsAffectedMismatchReturnsErrorAndLeavesIDsUntouched() {
	messages := this.messages(3)

	err := this.writer(7).assignIDs(messages, 2, 100)

	this.So(err, should.Equal, errRowsAffected)
	this.So(messages[0].ID, should.Equal, uint64(0))
	this.So(messages[1].ID, should.Equal, uint64(0))
	this.So(messages[2].ID, should.Equal, uint64(0))
}

func (this *AssignIDsFixture) TestZeroIdentityReturnsErrorAndLeavesIDsUntouched() {
	messages := this.messages(2)

	err := this.writer(7).assignIDs(messages, 2, 0)

	this.So(err, should.Equal, errIdentityFailure)
	this.So(messages[0].ID, should.Equal, uint64(0))
	this.So(messages[1].ID, should.Equal, uint64(0))
}

func (this *AssignIDsFixture) TestNegativeIdentityReturnsError() {
	messages := this.messages(1)

	err := this.writer(7).assignIDs(messages, 1, -1)

	this.So(err, should.Equal, errIdentityFailure)
	this.So(messages[0].ID, should.Equal, uint64(0))
}

func (this *WriterFixture) countMessages() int {
	var count int
	err := this.handle.QueryRow(`SELECT COUNT(*) FROM Messages`).Scan(&count)
	this.So(err, should.BeNil)
	return count
}
func (this *WriterFixture) firstMessageType() string {
	var t string
	err := this.handle.QueryRow(`SELECT type FROM Messages ORDER BY id LIMIT 1`).Scan(&t)
	this.So(err, should.BeNil)
	return t
}

func serializedMessage(value any, typeOverride string) *contracts.Message {
	payload, _ := json.Marshal(value)
	return &contracts.Message{
		Value:       value,
		Type:        typeOverride,
		Content:     bytes.NewBuffer(payload),
		ContentType: "application/json",
	}
}
