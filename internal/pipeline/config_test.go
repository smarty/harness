package pipeline

import (
	"context"
	"io"
	"math"
	"sync"
	"testing"

	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/better"
	"github.com/smarty/gunit/v2/assert/should"
	"github.com/smarty/harness/v2/contracts"
)

func TestValidationFixture(t *testing.T) {
	gunit.Run(new(ValidationFixture), t)
}

type ValidationFixture struct {
	*gunit.Fixture
}

func (this *ValidationFixture) validConfiguration() Configuration {
	return Configuration{
		Monitor:                nopCollaborator{},
		Storage:                nopCollaborator{},
		Serializer:             nopCollaborator{},
		Dispatcher:             nopCollaborator{},
		MessageTypes:           nil,
		DomainTypes:            nil,
		BurstCapacity:          1024,
		PipelineBufferCapacity: 4,
		ExecutionUnitSize:      64,
		ShedThreshold:          0.80,
	}
}

func (this *ValidationFixture) assertInvalid(config Configuration, fragments ...string) {
	pipeline, err := Build(this.Context(), config)
	this.So(pipeline, better.BeZeroValue)
	this.So(err, better.WrapError, contracts.ErrInvalidConfiguration)
	for _, fragment := range fragments {
		this.So(err.Error(), should.Contain, fragment)
	}
}

func (this *ValidationFixture) assertValid(config Configuration) {
	ctx, cancel := context.WithCancel(this.Context())
	defer cancel()
	pipeline, err := Build(ctx, config)
	this.So(err, should.BeNil)
	this.So(pipeline.BlockingEntrypoint, should.NOT.BeNil)

	// Drain the constructed pipeline so its goroutine resources unwind cleanly.
	var waiter sync.WaitGroup
	for _, listener := range pipeline.Listeners {
		waiter.Go(listener.Listen)
	}
	this.So(pipeline.BlockingEntrypoint.(interface{ Close() error }).Close(), should.BeNil)
	waiter.Wait()
}

func (this *ValidationFixture) TestBurstCapacityZero() {
	config := this.validConfiguration()
	config.BurstCapacity = 0
	this.assertInvalid(config, "BurstCapacity", "(got 0)")
}

func (this *ValidationFixture) TestBurstCapacityNegative() {
	config := this.validConfiguration()
	config.BurstCapacity = -1
	this.assertInvalid(config, "BurstCapacity", "(got -1)")
}

func (this *ValidationFixture) TestPipelineBufferCapacityNegative() {
	config := this.validConfiguration()
	config.PipelineBufferCapacity = -1
	this.assertInvalid(config, "PipelineBufferCapacity", "(got -1)")
}

func (this *ValidationFixture) TestExecutionUnitSizeZero() {
	config := this.validConfiguration()
	config.ExecutionUnitSize = 0
	this.assertInvalid(config, "ExecutionUnitSize", "(got 0)")
}

func (this *ValidationFixture) TestShedThresholdBelowRange() {
	config := this.validConfiguration()
	config.ShedThreshold = -0.1
	this.assertInvalid(config, "ShedThreshold", "(got -0.1)")
}

func (this *ValidationFixture) TestShedThresholdAboveRange() {
	config := this.validConfiguration()
	config.ShedThreshold = 1.1
	this.assertInvalid(config, "ShedThreshold", "(got 1.1)")
}

func (this *ValidationFixture) TestShedThresholdNaN() {
	config := this.validConfiguration()
	config.ShedThreshold = math.NaN()
	this.assertInvalid(config, "ShedThreshold", "(got NaN)")
}

func (this *ValidationFixture) TestMultipleViolationsReportedTogether() {
	config := this.validConfiguration()
	config.BurstCapacity = 0
	config.ShedThreshold = 1.5
	this.assertInvalid(config, "BurstCapacity", "ShedThreshold")
}

func (this *ValidationFixture) TestBoundaryValuesAreValid() {
	config := this.validConfiguration()
	config.BurstCapacity = 1
	config.PipelineBufferCapacity = 0
	config.ExecutionUnitSize = 1
	config.ShedThreshold = 0
	this.assertValid(config)
}

func (this *ValidationFixture) TestShedThresholdOfOneIsValid() {
	config := this.validConfiguration()
	config.ShedThreshold = 1
	this.assertValid(config)
}

func (this *ValidationFixture) TestNilDomainTypeRejected() {
	config := this.validConfiguration()
	config.DomainTypes = []any{nil}
	this.assertInvalid(config, "nil domain type")
}

func (this *ValidationFixture) TestExecutorWithDiscoverableMethodButNoGenericInterface() {
	config := this.validConfiguration()
	config.DomainTypes = []any{missingGenericExecutor{}}
	this.assertInvalid(config, "ExecuteOrder", "Execute(any, func(...any))")
}

func (this *ValidationFixture) TestApplicatorWithDiscoverableMethodButNoGenericInterface() {
	config := this.validConfiguration()
	config.DomainTypes = []any{missingGenericApplicator{}}
	this.assertInvalid(config, "ApplyView", "Apply(any)")
}

func (this *ValidationFixture) TestExecutorRoutesInterfaceParameter() {
	config := this.validConfiguration()
	config.DomainTypes = []any{interfaceRoutingExecutor{}}
	this.assertInvalid(config, "ExecuteThing", "interface")
}

func (this *ValidationFixture) TestApplicatorRoutesInterfaceParameter() {
	config := this.validConfiguration()
	config.DomainTypes = []any{interfaceRoutingApplicator{}}
	this.assertInvalid(config, "ApplyThing", "interface")
}

func (this *ValidationFixture) TestWellFormedDomainTypeAccepted() {
	config := this.validConfiguration()
	config.DomainTypes = []any{wellFormedHandler{}}
	this.assertValid(config)
}

type (
	orderCommand    struct{}
	viewModel       struct{}
	concreteCommand struct{}
	thing           interface{ isThing() }
)

// missingGenericExecutor exposes a discoverable Execute* method but never
// implements the generic executor interface, so scan would silently skip it.
type missingGenericExecutor struct{}

func (missingGenericExecutor) ExecuteOrder(_ orderCommand, _ func(...any)) {}

// missingGenericApplicator is the Apply-side analog of missingGenericExecutor.
type missingGenericApplicator struct{}

func (missingGenericApplicator) ApplyView(_ viewModel) {}

// interfaceRoutingExecutor implements the generic interface but routes an
// interface type, whose key can never match a concrete runtime message type.
type interfaceRoutingExecutor struct{}

func (interfaceRoutingExecutor) Execute(any, func(...any))            {}
func (interfaceRoutingExecutor) ExecuteThing(_ thing, _ func(...any)) {}

// interfaceRoutingApplicator is the Apply-side analog of interfaceRoutingExecutor.
type interfaceRoutingApplicator struct{}

func (interfaceRoutingApplicator) Apply(any)          {}
func (interfaceRoutingApplicator) ApplyThing(_ thing) {}

// wellFormedHandler implements the generic interface and routes a concrete type.
type wellFormedHandler struct{}

func (wellFormedHandler) Execute(any, func(...any))                         {}
func (wellFormedHandler) ExecuteConcrete(_ concreteCommand, _ func(...any)) {}

type nopCollaborator struct{}

func (nopCollaborator) Track(any)                                             {}
func (nopCollaborator) Serialize(io.Writer, any) error                        { return nil }
func (nopCollaborator) ContentType() string                                   { return "" }
func (nopCollaborator) Dispatch(context.Context, ...*contracts.Message) error { return nil }
func (nopCollaborator) Handle(ctx context.Context, operation any) error       { return nil }
