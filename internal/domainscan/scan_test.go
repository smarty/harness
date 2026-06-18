package domainscan

import (
	"reflect"
	"testing"

	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/should"
)

func TestDomainScanFixture(t *testing.T) {
	gunit.Run(new(DomainScanFixture), t)
}

type DomainScanFixture struct {
	*gunit.Fixture
}

type alpha struct{}
type beta struct{}

// twoApply has two typed Apply methods (plus the generic Apply, which must be ignored).
type twoApply struct{}

func (twoApply) Apply(any)        {}
func (twoApply) ApplyAlpha(alpha) {}
func (twoApply) ApplyBeta(beta)   {}

// genericOnly has only the generic Apply(any) — nothing typed to discover.
type genericOnly struct{}

func (genericOnly) Apply(any) {}

// applyAndExecute mixes a typed Apply with a typed Execute; Execute must be ignored.
type applyAndExecute struct{}

func (applyAndExecute) ApplyAlpha(alpha)               {}
func (applyAndExecute) ExecuteBeta(beta, func(...any)) {}

// duplicateApply applies the same type from two methods; the result must de-duplicate.
type duplicateApply struct{}

func (duplicateApply) ApplyAlpha(alpha)      {}
func (duplicateApply) ApplyAlphaAgain(alpha) {}

// malformedExecutors carry the Execute prefix but the wrong shape.
type malformedExecutors struct{}

func (malformedExecutors) ExecuteBadArity(beta)      {} // missing the broadcast func
func (malformedExecutors) ExecuteBadParam(beta, int) {} // last arg is not func(...any)

type noMethods struct{}

func (this *DomainScanFixture) TestTwoTypedApplyMethods() {
	result := AppliedTypes(twoApply{})
	this.So(result, should.Equal, []reflect.Type{
		reflect.TypeOf(alpha{}),
		reflect.TypeOf(beta{}),
	})
}

func (this *DomainScanFixture) TestGenericApplyOnlyDiscoversNothing() {
	this.So(AppliedTypes(genericOnly{}), should.BeNil)
}

func (this *DomainScanFixture) TestExecuteMethodsAreIgnored() {
	this.So(AppliedTypes(applyAndExecute{}), should.Equal, []reflect.Type{
		reflect.TypeOf(alpha{}),
	})
}

func (this *DomainScanFixture) TestDuplicateAppliedTypeDeduplicated() {
	this.So(AppliedTypes(duplicateApply{}), should.Equal, []reflect.Type{
		reflect.TypeOf(alpha{}),
	})
}

func (this *DomainScanFixture) TestNilValueReturnsNil() {
	this.So(AppliedTypes(nil), should.BeNil)
}

func (this *DomainScanFixture) TestNoMethodsReturnsNil() {
	this.So(AppliedTypes(noMethods{}), should.BeNil)
}

func (this *DomainScanFixture) TestIsApplyMethodDistinguishesShapes() {
	methods := methodsByName(reflect.TypeOf(applyAndExecute{}))
	this.So(IsApplyMethod(methods["ApplyAlpha"]), should.BeTrue)
	this.So(IsApplyMethod(methods["ExecuteBeta"]), should.BeFalse)
}

func (this *DomainScanFixture) TestIsExecuteMethodDistinguishesShapes() {
	methods := methodsByName(reflect.TypeOf(applyAndExecute{}))
	this.So(IsExecuteMethod(methods["ExecuteBeta"]), should.BeTrue)
	this.So(IsExecuteMethod(methods["ApplyAlpha"]), should.BeFalse)
}

func (this *DomainScanFixture) TestIsExecuteMethodRejectsMalformedShapes() {
	methods := methodsByName(reflect.TypeOf(malformedExecutors{}))
	this.So(IsExecuteMethod(methods["ExecuteBadArity"]), should.BeFalse)
	this.So(IsExecuteMethod(methods["ExecuteBadParam"]), should.BeFalse)
}

func (this *DomainScanFixture) TestHandledTypeReturnsMessageParameter() {
	methods := methodsByName(reflect.TypeOf(twoApply{}))
	this.So(HandledType(methods["ApplyAlpha"]), should.Equal, reflect.TypeOf(alpha{}))
}

func methodsByName(t reflect.Type) map[string]reflect.Method {
	result := make(map[string]reflect.Method)
	for x := range t.NumMethod() {
		method := t.Method(x)
		result[method.Name] = method
	}
	return result
}
