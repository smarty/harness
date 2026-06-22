package mysql

import (
	"testing"

	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/should"
	"github.com/smarty/harness/v2/contracts"
)

func TestAssignIDsFixture(t *testing.T) {
	gunit.Run(new(AssignIDsFixture), t)
}

// AssignIDsFixture exercises the pure ID-derivation logic without a database,
// covering the stride arithmetic and the defensive guards that a live MySQL
// server would never naturally trip.
type AssignIDsFixture struct {
	*gunit.Fixture
}

func (this *AssignIDsFixture) mapper(stride uint64) *Mapper {
	result := NewMapper(nil)
	result.stride.Store(stride)
	return result
}
func (this *AssignIDsFixture) messages(count int) (results []*contracts.Message) {
	for range count {
		results = append(results, &contracts.Message{})
	}
	return results
}

func (this *AssignIDsFixture) TestStridedIDsDerivedFromFirstIdentity() {
	messages := this.messages(3)

	err := this.mapper(7).assignIDs(messages, 3, 100)

	this.So(err, should.BeNil)
	this.So(messages[0].ID, should.Equal, uint64(100))
	this.So(messages[1].ID, should.Equal, uint64(107))
	this.So(messages[2].ID, should.Equal, uint64(114))
}

func (this *AssignIDsFixture) TestSingleMessage() {
	messages := this.messages(1)

	err := this.mapper(7).assignIDs(messages, 1, 42)

	this.So(err, should.BeNil)
	this.So(messages[0].ID, should.Equal, uint64(42))
}

func (this *AssignIDsFixture) TestRowsAffectedMismatchReturnsErrorAndLeavesIDsUntouched() {
	messages := this.messages(3)

	err := this.mapper(7).assignIDs(messages, 2, 100)

	this.So(err, should.Equal, errRowsAffected)
	this.So(messages[0].ID, should.Equal, uint64(0))
	this.So(messages[1].ID, should.Equal, uint64(0))
	this.So(messages[2].ID, should.Equal, uint64(0))
}

func (this *AssignIDsFixture) TestZeroIdentityReturnsErrorAndLeavesIDsUntouched() {
	messages := this.messages(2)

	err := this.mapper(7).assignIDs(messages, 2, 0)

	this.So(err, should.Equal, errIdentityFailure)
	this.So(messages[0].ID, should.Equal, uint64(0))
	this.So(messages[1].ID, should.Equal, uint64(0))
}

func (this *AssignIDsFixture) TestNegativeIdentityReturnsError() {
	messages := this.messages(1)

	err := this.mapper(7).assignIDs(messages, 1, -1)

	this.So(err, should.Equal, errIdentityFailure)
	this.So(messages[0].ID, should.Equal, uint64(0))
}
