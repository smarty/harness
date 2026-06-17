package snapshots

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"time"

	"github.com/smarty/harness/v2/contracts"
	"github.com/smarty/harness/v2/internal/storage"
)

// Save persists a snapshot record to storage.
func Save(ctx context.Context, options ...saveOption) error {
	var config saveConfig
	for _, option := range append(SaveOptions.defaults(), options...) {
		option(&config)
	}
	return save(ctx, config)
}

type saveConfig struct {
	Storage       contracts.Storage
	Timestamp     time.Time
	HighWatermark uint64
	Snapshot      any
}

type saveOption func(*saveConfig)

var SaveOptions saving

type saving struct{}

func (saving) Storage(storage contracts.Storage) saveOption {
	return func(config *saveConfig) { config.Storage = storage }
}
func (saving) Timestamp(timestamp time.Time) saveOption {
	return func(config *saveConfig) { config.Timestamp = timestamp }
}
func (saving) HighWatermark(highWatermark uint64) saveOption {
	return func(config *saveConfig) { config.HighWatermark = highWatermark }
}
func (saving) Snapshot(snapshot any) saveOption {
	return func(config *saveConfig) { config.Snapshot = snapshot }
}
func (saving) defaults(options ...saveOption) []saveOption {
	return append([]saveOption{
		SaveOptions.Timestamp(time.Now().UTC()),
	}, options...)
}

func save(ctx context.Context, config saveConfig) error {
	payload, err := jsonMarshalCompressed(config.Snapshot)
	if err != nil {
		return err
	}
	return config.Storage.Exec(ctx, &storage.SaveSnapshot{
		Timestamp:       config.Timestamp,
		HighWatermark:   config.HighWatermark,
		Payload:         payload,
		ContentType:     "application/json",
		ContentEncoding: "gzip",
	})
}

func jsonMarshalCompressed(v any) ([]byte, error) {
	var buffer bytes.Buffer
	gzipWriter := gzip.NewWriter(&buffer)
	defer func() { _ = gzipWriter.Close() }()

	err := json.NewEncoder(gzipWriter).Encode(v)
	if err != nil {
		return nil, err
	}
	if err := gzipWriter.Close(); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}
