// Package domainscan holds the reflective method-shape detection shared by the
// pipeline router and the snapshots event-replay machinery: it answers "what does
// a discoverable Execute<Foo>/Apply<Foo> handler method look like?" in one place.
package domainscan

import (
	"reflect"
	"strings"
)

var broadcastFuncType = reflect.TypeOf(func(...any) {})

// AppliedTypes returns the de-duplicated message types discoverable on v via
// methods shaped like Apply<Foo>(Foo). It returns nil for a nil value or one
// with no such methods.
func AppliedTypes(v any) (results []reflect.Type) {
	if v == nil {
		return nil
	}
	seen := make(map[reflect.Type]bool)
	t := reflect.TypeOf(v)
	for x := range t.NumMethod() {
		method := t.Method(x)
		if !IsApplyMethod(method) {
			continue
		}
		if handled := HandledType(method); !seen[handled] {
			seen[handled] = true
			results = append(results, handled)
		}
	}
	return results
}

func IsExecuteMethod(method reflect.Method) bool {
	if !strings.HasPrefix(method.Name, "Execute") || len(method.Name) == len("Execute") {
		return false
	}
	if method.Type.NumIn() != 3 || method.Type.NumOut() > 0 {
		return false
	}
	return method.Type.In(2) == broadcastFuncType
}
func IsApplyMethod(method reflect.Method) bool {
	if !strings.HasPrefix(method.Name, "Apply") || len(method.Name) == len("Apply") {
		return false
	}
	return method.Type.NumIn() == 2 && method.Type.NumOut() == 0
}
func HandledType(method reflect.Method) reflect.Type {
	return method.Type.In(1)
}
