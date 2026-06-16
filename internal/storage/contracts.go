package storage

import "context"

type DB interface {
	Handle(ctx context.Context, operation any) error
}
