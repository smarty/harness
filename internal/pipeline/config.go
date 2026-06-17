package pipeline

import (
	"errors"
	"fmt"
	"math"
	"reflect"

	"github.com/smarty/harness/v2/contracts"
)

type Configuration struct {
	Monitor                contracts.Monitor
	Storage                contracts.Storage
	Serializer             contracts.Serializer
	Dispatcher             contracts.Dispatcher
	MessageTypes           map[reflect.Type]string
	DomainTypes            []any
	BurstCapacity          int
	PipelineBufferCapacity int
	ExecutionUnitSize      int
	ShedThreshold          float64
}

func (this *Configuration) validate() (err error) {
	if this.BurstCapacity <= 0 {
		err = errors.Join(err, fmt.Errorf(
			"%w: BurstCapacity must be >= 1 (got %d)", contracts.ErrInvalidConfiguration, this.BurstCapacity))
	}
	if this.PipelineBufferCapacity < 0 {
		err = errors.Join(err, fmt.Errorf(
			"%w: PipelineBufferCapacity must be >= 0 (got %d)", contracts.ErrInvalidConfiguration, this.PipelineBufferCapacity))
	}
	if this.ExecutionUnitSize <= 0 {
		err = errors.Join(err, fmt.Errorf(
			"%w: ExecutionUnitSize must be > 0 (got %d)", contracts.ErrInvalidConfiguration, this.ExecutionUnitSize))
	}
	if this.ShedThreshold < 0 || this.ShedThreshold > 1 || math.IsNaN(this.ShedThreshold) {
		err = errors.Join(err, fmt.Errorf(
			"%w: ShedThreshold must be within 0 and 1 (got %g)", contracts.ErrInvalidConfiguration, this.ShedThreshold))
	}
	err = errors.Join(err, validateDomainTypes(this.DomainTypes...))
	return err
}
