package mysql

import (
	"bytes"

	"github.com/smarty/harness/v2/internal/generic"
)

type statement struct {
	args []any
	text *bytes.Buffer
}

func newStatement() *statement {
	return &statement{
		args: make([]any, 0, statementArgsCapacity),
		text: bytes.NewBuffer(make([]byte, 0, statementTextCapacity)),
	}
}

func (this *statement) reset() {
	this.args = generic.Reclaim(this.args, statementArgsCapacity)
	this.text = generic.ReclaimBuffer(this.text, statementTextCapacity)
}

// Steady-state capacities retained for the Mapper's reused buffers; a
// pathologically large batch has its oversized backing arrays discarded on the
// next call rather than pinned for the life of the process (see generic.Reclaim).
const (
	statementArgsCapacity = 512
	statementTextCapacity = 1024 * 8
)
