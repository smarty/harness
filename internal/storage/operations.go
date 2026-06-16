package storage

import (
	"errors"

	"github.com/smarty/harness/v2/contracts"
)

var ErrUnsupportedOperation = errors.New("harness: unsupported storage operation")

type MarkMessagesDispatched struct {
	Messages []*contracts.Message
}
