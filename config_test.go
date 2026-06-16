package harness

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/should"
	"github.com/smarty/harness/v2/contracts"
	"github.com/smarty/harness/v2/internal/pipeline"
)

func TestConfigFixture(t *testing.T) {
	gunit.Run(new(ConfigFixture), t)
}

type ConfigFixture struct {
	*gunit.Fixture
}

func (this *ConfigFixture) apply(options ...option) pipeline.Configuration {
	var cfg pipeline.Configuration
	for _, item := range Options.defaults(options...) {
		item(&cfg)
	}
	return cfg
}

func (this *ConfigFixture) TestNop() {
	var n nop
	this.So(func() { n.Track(nil) }, should.NOT.Panic)
	this.So(n.Serialize(nil, nil), should.BeNil)
	this.So(n.ContentType(), should.BeEmpty)
	this.So(n.Dispatch(nil), should.BeNil)
	this.So(n.Handle(nil, nil), should.BeNil)
}

func (this *ConfigFixture) TestDefaultsPopulateCapacities() {
	cfg := this.apply()
	this.So(cfg.BurstCapacity, should.Equal, 1024)
	this.So(cfg.PipelineBufferCapacity, should.Equal, 4)
	this.So(cfg.ExecutionUnitSize, should.Equal, 64)
	this.So(cfg.ShedThreshold, should.Equal, 0.80)
	this.So(cfg.MessageTypes, should.BeNil)
}

func (this *ConfigFixture) TestDefaultCollaboratorsAreNop() {
	cfg := this.apply()
	this.So(cfg.Monitor, should.Equal, nop{})
	this.So(cfg.Serializer, should.Equal, nop{})
	this.So(cfg.Dispatcher, should.Equal, nop{})
	this.So(cfg.Storage, should.Equal, nop{})
}

func (this *ConfigFixture) TestTypesOptionStoresValuesVerbatim() {
	cfg := this.apply(Options.DomainTypes("a", 42, struct{}{}))
	this.So(cfg.DomainTypes, should.Equal, []any{"a", 42, struct{}{}})
}

func (this *ConfigFixture) TestTunableOptionsOverrideDefaults() {
	cfg := this.apply(
		Options.BurstCapacity(2),
		Options.PipelineBufferCapacity(2),
		Options.ExecutionUnitSize(8),
		Options.ShedThreshold(0.5),
		Options.MessageTypes(map[reflect.Type]string{reflect.TypeOf(""): "simple-string"}),
	)
	this.So(cfg.BurstCapacity, should.Equal, 2)
	this.So(cfg.PipelineBufferCapacity, should.Equal, 2)
	this.So(cfg.ExecutionUnitSize, should.Equal, 8)
	this.So(cfg.ShedThreshold, should.Equal, 0.5)
	this.So(cfg.MessageTypes, should.Equal, map[reflect.Type]string{reflect.TypeOf(""): "simple-string"})
}

func (this *ConfigFixture) TestCollaboratorOptionsOverrideDefaults() {
	recorder := &recordingMonitor{}
	cfg := this.apply(Options.Monitor(recorder))
	this.So(cfg.Monitor, should.Equal, recorder)
}

func (this *ConfigFixture) TestInvalidConfigurationYieldsErrorAndZeroPipeline() {
	result, err := New(context.Background(), Options.BurstCapacity(0))
	this.So(errors.Is(err, contracts.ErrInvalidConfiguration), should.BeTrue)
	this.So(result, should.Equal, contracts.Pipeline{})
}

func (this *ConfigFixture) TestDefaultConfigurationYieldsNilError() {
	_, err := New(context.Background())
	this.So(err, should.BeNil)
}

func (this *ConfigFixture) TestZeroOptionsPipelineRunsInertly() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	inert, err := New(ctx)
	this.So(err, should.BeNil)

	done := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		for _, listener := range inert.Listeners {
			wg.Go(listener.Listen)
		}
		wg.Wait()
		close(done)
	}()

	inert.BlockingEntrypoint.Handle(ctx, "payload")
	inert.SheddingEntrypoint.Handle(ctx, "payload")
	this.So(inert.BlockingEntrypoint.(interface{ Close() error }).Close(), should.BeNil)
	<-done
}

type recordingMonitor struct{ observations []any }

func (this *recordingMonitor) Track(observation any) {
	this.observations = append(this.observations, observation)
}
