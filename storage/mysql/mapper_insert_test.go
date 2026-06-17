package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"sync"

	"github.com/smarty/gunit/v2/assert/should"
	"github.com/smarty/harness/v2/contracts"
	"github.com/smarty/harness/v2/internal/storage"
)

type insertEvent struct {
	Mapper int
	Order  int
}

func insertMessage(value any, typeName string) *contracts.Message {
	payload, _ := json.Marshal(value)
	return &contracts.Message{
		Value:       value,
		Type:        typeName,
		Content:     bytes.NewBuffer(payload),
		ContentType: "application/json",
	}
}

func (this *MapperFixture) insert(messages ...*contracts.Message) error {
	return this.subject.Exec(this.ctx, &storage.InsertMessages{Messages: messages})
}

func (this *MapperFixture) TestInsert_PersistsRowAndAssignsID() {
	message := insertMessage(insertEvent{Order: 1}, "order-received")

	err := this.insert(message)

	this.So(err, should.BeNil)
	this.So(message.ID, should.NOT.Equal, uint64(0))
	this.So(this.countMessages(), should.Equal, 1)
	this.So(this.rowType(message.ID), should.Equal, "order-received")
}

func (this *MapperFixture) TestInsert_MultipleRows_AssignsStridedIDsMatchingRows() {
	first := insertMessage(insertEvent{Order: 1}, "order-received")
	second := insertMessage(insertEvent{Order: 2}, "order-approved")

	err := this.insert(first, second)

	this.So(err, should.BeNil)
	this.So(this.countMessages(), should.Equal, 2)
	this.So(second.ID, should.Equal, first.ID+this.stride)
	this.So(this.rowType(first.ID), should.Equal, "order-received")
	this.So(this.rowType(second.ID), should.Equal, "order-approved")
}

func (this *MapperFixture) TestInsert_InvalidMessagesTableName_Rejected() {
	mapper := NewMapper(this.handle, this.stride, "Snapshots", "Msg; DROP")

	err := mapper.Exec(this.ctx, &storage.InsertMessages{
		Messages: []*contracts.Message{insertMessage(insertEvent{Order: 1}, "order-received")},
	})

	this.So(err, should.NOT.BeNil)
	this.So(this.countMessages(), should.Equal, 0)
}

func (this *MapperFixture) TestInsert_NoMessages_NoOp() {
	err := this.insert()

	this.So(err, should.BeNil)
	this.So(this.countMessages(), should.Equal, 0)
}

func (this *MapperFixture) TestInsert_LegacyWrite_ReceivesValuesAndCommitsInSameTransaction() {
	var gotValues []any
	event := insertEvent{Order: 7}
	mapper := NewMapper(this.handle, this.stride, "Snapshots", "Messages").WithLegacyWrite(
		func(ctx context.Context, tx *sql.Tx, values ...any) {
			gotValues = values
			_, err := tx.ExecContext(ctx, `UPDATE Messages SET type = 'touched-by-legacy'`)
			this.So(err, should.BeNil)
		})
	message := insertMessage(event, "order-received")

	err := mapper.Exec(this.ctx, &storage.InsertMessages{Messages: []*contracts.Message{message}})

	this.So(err, should.BeNil)
	this.So(gotValues, should.Equal, []any{event})
	this.So(this.countMessages(), should.Equal, 1)
	this.So(this.rowType(message.ID), should.Equal, "touched-by-legacy") // legacy UPDATE committed atomically
}

func (this *MapperFixture) TestInsert_LegacyWritePanic_RollsBackInsert() {
	mapper := NewMapper(this.handle, this.stride, "Snapshots", "Messages").WithLegacyWrite(
		func(context.Context, *sql.Tx, ...any) { panic("boom") })
	message := insertMessage(insertEvent{Order: 1}, "order-received")

	err := mapper.Exec(this.ctx, &storage.InsertMessages{Messages: []*contracts.Message{message}})

	this.So(err, should.NOT.BeNil)
	this.So(this.countMessages(), should.Equal, 0)
}

func (this *MapperFixture) TestInsert_BeginTransactionError_ReturnsError() {
	_ = this.handle.Close() // force BeginTx to fail

	err := this.insert(insertMessage(insertEvent{Order: 1}, "order-received"))

	this.So(err, should.NOT.BeNil)
}

func (this *MapperFixture) TestInsert_ExecError_RollsBack() {
	oversizedType := strings.Repeat("x", 300) // exceeds Messages.type varchar(256)

	err := this.insert(insertMessage(insertEvent{Order: 1}, oversizedType))

	this.So(err, should.NOT.BeNil)
	this.So(this.countMessages(), should.Equal, 0) // the failed INSERT was rolled back
}

type writeRecord struct {
	id      uint64
	payload string
}

// TestConcurrentInserts_AssignedIDsMatchActualRows exercises the central claim
// documented on assignIDs: a single multi-row "simple insert" reserves a block
// of consecutive auto-increment values spaced by stride, and this holds even
// when several connections insert at once. Each mapper stamps a globally unique
// payload onto every message; after the storm, the assigned IDs must partition
// the table with no collisions, each pointing at the exact row it wrote.
func (this *MapperFixture) TestConcurrentInserts_AssignedIDsMatchActualRows() {
	const (
		mappers          = 8
		batchesPerMapper = 20
		messagesPerBatch = 5
	)
	records := make([][]writeRecord, mappers)
	errs := make([]error, mappers)
	var waiter sync.WaitGroup
	for w := range mappers {
		waiter.Add(1)
		go func() {
			defer waiter.Done()
			records[w], errs[w] = this.runMapper(w, batchesPerMapper, messagesPerBatch)
		}()
	}
	waiter.Wait()

	expected := make(map[uint64]string)
	for w := range mappers {
		this.So(errs[w], should.BeNil)
		for _, record := range records[w] {
			_, duplicate := expected[record.id]
			this.So(duplicate, should.BeFalse) // assigned IDs must never collide across mappers
			expected[record.id] = record.payload
		}
	}
	this.So(this.allRows(), should.Equal, expected)
}

func (this *MapperFixture) runMapper(mapperID, batches, perBatch int) (results []writeRecord, err error) {
	mapper := NewMapper(this.handle, this.stride, "Snapshots", "Messages")
	for batch := range batches {
		messages := make([]*contracts.Message, 0, perBatch)
		for m := range perBatch {
			event := insertEvent{Mapper: mapperID, Order: batch*perBatch + m}
			messages = append(messages, insertMessage(event, "order-received"))
		}
		if err := mapper.Exec(this.ctx, &storage.InsertMessages{Messages: messages}); err != nil {
			return nil, err
		}
		for _, message := range messages {
			results = append(results, writeRecord{id: message.ID, payload: string(message.Content.Bytes())})
		}
	}
	return results, nil
}

func (this *MapperFixture) countMessages() int {
	var count int
	err := this.handle.QueryRow(`SELECT COUNT(*) FROM Messages`).Scan(&count)
	this.So(err, should.BeNil)
	return count
}

func (this *MapperFixture) rowType(id uint64) string {
	var result string
	err := this.handle.QueryRow(`SELECT type FROM Messages WHERE id = ?`, id).Scan(&result)
	this.So(err, should.BeNil)
	return result
}

func (this *MapperFixture) allRows() map[uint64]string {
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
