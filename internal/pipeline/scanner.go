package pipeline

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/smarty/harness/v2/contracts"
	"github.com/smarty/harness/v2/internal/domainscan"
)

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
			if isExecutor && domainscan.IsExecuteMethod(method) {
				executors[domainscan.HandledType(method)] = append(executors[domainscan.HandledType(method)], e)
			}
			if isApplicator && domainscan.IsApplyMethod(method) {
				applicators[domainscan.HandledType(method)] = append(applicators[domainscan.HandledType(method)], a)
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
			case domainscan.IsExecuteMethod(method):
				err = errors.Join(err, validateHandler(t, method, isExecutor, "Execute(any, func(...any))"))
			case domainscan.IsApplyMethod(method):
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
	if domainscan.HandledType(method).Kind() == reflect.Interface {
		return fmt.Errorf(
			"%w: method %s.%s routes interface type %s, which can never match the concrete runtime type of a message",
			contracts.ErrInvalidConfiguration, t, method.Name, domainscan.HandledType(method))
	}
	return nil
}
