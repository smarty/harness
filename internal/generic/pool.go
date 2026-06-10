package generic

import "sync"

func NewT[T any]() *T { return new(T) }

type PoolT[T any] struct{ pool *sync.Pool }

func NewPoolT[T any](new func() T) *PoolT[T] {
	return &PoolT[T]{pool: &sync.Pool{New: func() any { return new() }}}
}
func (this *PoolT[T]) Get() T  { return this.pool.Get().(T) }
func (this *PoolT[T]) Put(t T) { this.pool.Put(t) }
