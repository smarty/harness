package pipeline

import "iter"

func Drain[T any](input chan T) iter.Seq[T] {
	return func(yield func(T) bool) {
		for v := range input {
			if !yield(v) {
				return
			}
		}
	}
}
