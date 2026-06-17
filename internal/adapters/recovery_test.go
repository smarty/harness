package adapters

import (
	"context"
	"errors"
	"testing"

	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/should"
	"github.com/smarty/harness/v2/contracts"
	"github.com/smarty/harness/v2/internal/storage"
)

func TestRecoveryFixture(t *testing.T) {
	gunit.Run(new(RecoveryFixture), t)
}

// RecoveryFixture exercises the Recovery cursor logic in isolation against a
// fake contracts.Storage; no real database is needed. The two SQL queries themselves
// (bounds + windowed page) are covered by the Mapper's integration tests in
// internal/storage/mysql.
type RecoveryFixture struct {
	*gunit.Fixture
	ctx     context.Context
	db      *fakeRecoveryDB
	subject *Recovery
}

func (this *RecoveryFixture) Setup() {
	this.ctx = context.WithValue(this.Context(), "testing", this.Name())
	this.db = &fakeRecoveryDB{expectedCtxValue: this.Name(), fixture: this.Fixture}
	this.subject = NewRecovery(this.db)
}

// drain pages the subject with the given limit until an empty page, returning
// every recovered message flattened in dispatch order.
func (this *RecoveryFixture) drain(limit int) (results []*contracts.Message) {
	for {
		page, err := this.subject.Recover(this.ctx, limit)
		this.So(err, should.BeNil)
		if len(page) == 0 {
			return results
		}
		results = append(results, page...)
	}
}

func ids(messages []*contracts.Message) (results []uint64) {
	for _, message := range messages {
		results = append(results, message.ID)
	}
	return results
}

func (this *RecoveryFixture) TestEmptyBacklog_CompletesOnFirstCall() {
	// No undispatched rows: bounds reports Found=false.

	page, err := this.subject.Recover(this.ctx, 64)

	this.So(err, should.BeNil)
	this.So(page, should.BeEmpty)
	this.So(this.db.boundsCalls, should.Equal, 1)
	this.So(this.db.pageCalls, should.Equal, 0) // never issues a page query
}

func (this *RecoveryFixture) TestReturnsUndispatchedRowsInIDOrder() {
	this.db.rows = []fakeRow{{id: 7}, {id: 8}}

	messages := this.drain(64)

	this.So(ids(messages), should.Equal, []uint64{7, 8})
}

func (this *RecoveryFixture) TestSnapshotOnce_BoundsQueriedOnlyOnFirstCall() {
	this.db.rows = []fakeRow{{id: 1}, {id: 2}, {id: 3}}

	_ = this.drain(1)

	this.So(this.db.boundsCalls, should.Equal, 1)
}

func (this *RecoveryFixture) TestCursorStartsAtMinMinusOne_BoundaryIsMax() {
	this.db.rows = []fakeRow{{id: 5}, {id: 6}, {id: 7}}

	_, err := this.subject.Recover(this.ctx, 64)

	this.So(err, should.BeNil)
	this.So(this.db.firstPage.AfterID, should.Equal, uint64(4))   // Min(5) - 1
	this.So(this.db.firstPage.ThroughID, should.Equal, uint64(7)) // Max
}

func (this *RecoveryFixture) TestAdvancesToLastIDAfterCleanPage() {
	this.db.rows = []fakeRow{{id: 5}, {id: 6}, {id: 7}, {id: 8}}

	_, err := this.subject.Recover(this.ctx, 2) // serves 5, 6
	this.So(err, should.BeNil)
	_, err = this.subject.Recover(this.ctx, 2) // next page
	this.So(err, should.BeNil)

	this.So(this.db.lastPage.AfterID, should.Equal, uint64(6)) // advanced to last id of the prior page
}

func (this *RecoveryFixture) TestPagesEveryRowExactlyOnceInOrder() {
	this.db.rows = []fakeRow{{id: 10}, {id: 11}, {id: 12}, {id: 13}, {id: 14}}

	messages := this.drain(2)

	this.So(ids(messages), should.Equal, []uint64{10, 11, 12, 13, 14})
}

func (this *RecoveryFixture) TestBoundaryCompletion_NoPageQueryOnceDrained() {
	this.db.rows = []fakeRow{{id: 1}, {id: 2}}

	_ = this.drain(64) // one non-empty page exhausts the backlog (cursor reaches boundary)

	this.So(this.db.pageCalls, should.Equal, 1) // the post-drain call returns early without querying
}

func (this *RecoveryFixture) TestFailedPageIsReServed() {
	this.db.rows = []fakeRow{{id: 1}, {id: 2}}
	this.db.pageErr = errors.New("query failed")

	failed, err := this.subject.Recover(this.ctx, 2)
	this.So(err, should.WrapError, this.db.pageErr)
	this.So(failed, should.BeEmpty)

	// The cursor did not advance, so a recovered db re-serves the same page.
	this.db.pageErr = nil
	page, err := this.subject.Recover(this.ctx, 2)
	this.So(err, should.BeNil)
	this.So(ids(page), should.Equal, []uint64{1, 2})
	this.So(this.db.lastPage.AfterID, should.Equal, uint64(0)) // same window as the failed attempt (Min(1)-1)
}

func (this *RecoveryFixture) TestSnapshotError_Propagates_AndRetriesOnNextCall() {
	this.db.rows = []fakeRow{{id: 1}}
	this.db.boundsErr = errors.New("bounds query failed")

	_, err := this.subject.Recover(this.ctx, 64)
	this.So(err, should.WrapError, this.db.boundsErr)

	// Not snapshotted: the next call retries the bounds query rather than paging blind.
	this.db.boundsErr = nil
	messages, err := this.subject.Recover(this.ctx, 64)
	this.So(err, should.BeNil)
	this.So(ids(messages), should.Equal, []uint64{1})
	this.So(this.db.boundsCalls, should.Equal, 2)
}

func (this *RecoveryFixture) TestFrozenBoundary_ExcludesRowsWrittenDuringRecovery() {
	this.db.rows = []fakeRow{{id: 5}, {id: 6}}

	first, err := this.subject.Recover(this.ctx, 1) // snapshots boundary at Max=6
	this.So(err, should.BeNil)
	this.So(ids(first), should.Equal, []uint64{5})

	this.db.rows = append(this.db.rows, fakeRow{id: 10}) // live traffic during recovery

	messages := this.drain(1)

	this.So(ids(messages), should.Equal, []uint64{6}) // 10 excluded by the frozen boundary
	this.So(this.db.lastPage.ThroughID, should.Equal, uint64(6))
}

type fakeRow struct {
	id       uint64
	typeName string
	payload  string
}

// fakeRecoveryDB answers LoadUndispatchedBounds / LoadUndispatchedPage from an
// in-memory, id-ordered backlog, mutating the operation in place as the Mapper
// would. It records call counts and the most recent page op so tests can assert
// the cursor windowing.
type fakeRecoveryDB struct {
	fixture          *gunit.Fixture
	expectedCtxValue any
	rows             []fakeRow

	boundsErr error
	pageErr   error

	boundsCalls int
	pageCalls   int
	firstPage   storage.LoadUndispatchedPage
	lastPage    storage.LoadUndispatchedPage
}

func (this *fakeRecoveryDB) Exec(ctx context.Context, operation any) error {
	this.fixture.So(ctx.Value("testing"), should.Equal, this.expectedCtxValue)
	switch op := operation.(type) {
	case *storage.LoadUndispatchedBounds:
		this.boundsCalls++
		if this.boundsErr != nil {
			return this.boundsErr
		}
		if len(this.rows) > 0 {
			op.Found = true
			op.Min = this.rows[0].id
			op.Max = this.rows[len(this.rows)-1].id
		}
		return nil
	case *storage.LoadUndispatchedPage:
		this.pageCalls++
		if this.pageCalls == 1 {
			this.firstPage = *op
		}
		if this.pageErr != nil {
			return this.pageErr
		}
		for _, row := range this.rows {
			if row.id <= op.AfterID || row.id > op.ThroughID {
				continue
			}
			if len(op.Messages) == op.Limit {
				break
			}
			op.Messages = append(op.Messages, &contracts.Message{ID: row.id, Type: row.typeName})
		}
		this.lastPage = *op
		return nil
	default:
		return storage.ErrUnsupportedOperation
	}
}
