package pipeline

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/smarty/harness/v2/contracts"
)

var broadcastFuncType = reflect.TypeOf(func(...any) {})

func scan(types ...any) (executors map[reflect.Type][]executor, applicators map[reflect.Type][]applicator) {
	executors = make(map[reflect.Type][]executor)
	applicators = make(map[reflect.Type][]applicator)
	for _, v := range types {
		e, isExecutor := v.(executor)
		a, isApplicator := v.(applicator)
		if !isExecutor && !isApplicator {
			continue
		}
		t := reflect.TypeOf(v)
		for x := range t.NumMethod() {
			method := t.Method(x)
			if isExecutor && isExecuteMethod(method) {
				executors[handledType(method)] = append(executors[handledType(method)], e)
			}
			if isApplicator && isApplyMethod(method) {
				applicators[handledType(method)] = append(applicators[handledType(method)], a)
			}
		}
	}
	return executors, applicators
}

// validateDomainTypes catches the routing misconfigurations that scan would
// otherwise swallow silently: a type that carries discoverable Execute.../
// Apply... methods but never implements the generic interface scan dispatches
// through, and a prefixed method whose message parameter is an interface type
// (its map key can never match the concrete reflect.TypeOf of a runtime
// message). It does NOT — cannot — catch a generic Execute/Apply switch that
// forgets a case it advertises; that drift stays the caller's contract.
func validateDomainTypes(types ...any) (err error) {
	for _, v := range types {
		if v == nil {
			err = errors.Join(err, fmt.Errorf(
				"%w: nil domain type registered via DomainTypes(...)", contracts.ErrInvalidConfiguration))
			continue
		}
		_, isExecutor := v.(executor)
		_, isApplicator := v.(applicator)
		t := reflect.TypeOf(v)
		for x := range t.NumMethod() {
			method := t.Method(x)
			switch {
			case isExecuteMethod(method):
				err = errors.Join(err, validateHandler(t, method, isExecutor, "Execute(any, func(...any))"))
			case isApplyMethod(method):
				err = errors.Join(err, validateHandler(t, method, isApplicator, "Apply(any)"))
			}
		}
	}
	return err
}
func validateHandler(t reflect.Type, method reflect.Method, implementsInterface bool, generic string) error {
	if !implementsInterface {
		return fmt.Errorf(
			"%w: %s has discoverable method %s but does not implement the generic %s interface, so it routes nothing",
			contracts.ErrInvalidConfiguration, t, method.Name, generic)
	}
	if handledType(method).Kind() == reflect.Interface {
		return fmt.Errorf(
			"%w: method %s.%s routes interface type %s, which can never match the concrete runtime type of a message",
			contracts.ErrInvalidConfiguration, t, method.Name, handledType(method))
	}
	return nil
}
func isExecuteMethod(method reflect.Method) bool {
	if !strings.HasPrefix(method.Name, "Execute") || len(method.Name) == len("Execute") {
		return false
	}
	if method.Type.NumIn() != 3 || method.Type.NumOut() > 0 {
		return false
	}
	return method.Type.In(2) == broadcastFuncType
}
func isApplyMethod(method reflect.Method) bool {
	if !strings.HasPrefix(method.Name, "Apply") || len(method.Name) == len("Apply") {
		return false
	}
	return method.Type.NumIn() == 2 && method.Type.NumOut() == 0
}
func handledType(method reflect.Method) reflect.Type {
	return method.Type.In(1)
}
