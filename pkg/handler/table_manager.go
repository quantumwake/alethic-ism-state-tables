package handler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/quantumwake/alethic-ism-core-go/pkg/data/models"
	"github.com/quantumwake/alethic-ism-core-go/pkg/repository/processor"
	"github.com/quantumwake/alethic-ism-core-go/pkg/repository/processor/tables"
)

// BatchWriter handles batched inserts to a database table with automatic flushing based on size and time thresholds
type BatchWriter struct {
	config    *tables.TableProcessorConfig
	tableName string

	mu         sync.Mutex
	batch      []models.Data   // Current batch of records waiting to be inserted
	routeIDs   map[string]bool // Track unique routes in current batch for status publishing
	lastFlush  time.Time       // When we last flushed the batch
	lastUsed   time.Time       // Track for cleanup of idle managers
	tableReady bool            // Whether the table has been created
	stopFlush  chan struct{}   // Signal to stop background flush goroutine
}

// NewBatchWriter creates a new BatchWriter for a specific processor with the given configuration
func NewBatchWriter(processorID string, config *tables.TableProcessorConfig) *BatchWriter {
	var tableName string
	if config.TableName != nil {
		tableName = FormatTableName(processorID, *config.TableName)
	} else {
		tableName = FormatTableName(processorID, "")
	}

	writer := &BatchWriter{
		config:    config,
		tableName: tableName,
		batch:     make([]models.Data, 0),
		routeIDs:  make(map[string]bool),
		lastFlush: time.Now(),
		lastUsed:  time.Now(),
		stopFlush: make(chan struct{}),
	}

	// Start background flush goroutine if time-based batching is configured
	if config.BatchWindowTTL != nil && *config.BatchWindowTTL >= 5 {
		go writer.backgroundFlush(time.Duration(*config.BatchWindowTTL) * time.Second)
	}

	return writer
}

// Add appends records to the batch and flushes if size threshold is reached
func (bw *BatchWriter) Add(routeID string, records []models.Data) error {
	bw.mu.Lock()
	defer bw.mu.Unlock()

	bw.lastUsed = time.Now()
	bw.routeIDs[routeID] = true

	// Add timestamp to each record if configured
	if bw.config.IncludeTimestamp != nil && *bw.config.IncludeTimestamp {
		timestamp := time.Now().UTC().Format(time.RFC3339)
		for i := range records {
			if records[i] == nil {
				records[i] = make(models.Data)
			}
			records[i]["_timestamp"] = timestamp
		}
	}

	bw.batch = append(bw.batch, records...)

	// Flush if we've reached the configured batch size
	// TODO: Make async flush configurable via processor properties flag
	if bw.config.BatchSize != nil && len(bw.batch) >= *bw.config.BatchSize {
		go func() {
			err := bw.Flush()
			if err != nil {
				fmt.Printf("error flushing batch writer: %v\n", err)
			}
		}()
	}

	return nil
}

// Flush forces an immediate flush of the current batch
func (bw *BatchWriter) Flush() error {
	bw.mu.Lock()
	defer bw.mu.Unlock()
	return bw.flush()
}

// flush performs the actual database operations (must be called with lock held)
func (bw *BatchWriter) flush() error {
	if len(bw.batch) == 0 {
		return nil
	}

	// Create table on first flush using the schema from the first record
	if !bw.tableReady && len(bw.batch) > 0 {
		if err := bw.ensureTable(bw.batch[0]); err != nil {
			return err
		}
		bw.tableReady = true
	}

	// Insert all records in the batch
	for _, record := range bw.batch {
		if err := InsertRecord(bw.tableName, record); err != nil {
			return fmt.Errorf("failed to insert record: %w", err)
		}
	}

	// Publish completion status for each route in the batch
	for routeID := range bw.routeIDs {
		PublishRouteStatus(context.Background(), routeID, processor.Completed, "", nil)
	}

	// Reset batch, routeIDs, and update flush time
	bw.batch = bw.batch[:0]
	bw.routeIDs = make(map[string]bool)
	bw.lastFlush = time.Now()

	return nil
}

// ensureTable creates the table if it doesn't exist using the schema from a sample record
func (bw *BatchWriter) ensureTable(sample models.Data) error {
	return CreateTableFromMap(bw.tableName, sample)
}

// backgroundFlush runs a goroutine that periodically flushes the batch based on time
func (bw *BatchWriter) backgroundFlush(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			_ = bw.Flush()
		case <-bw.stopFlush:
			// Flush any remaining data before stopping
			_ = bw.Flush()
			return
		}
	}
}

// Stop gracefully shuts down the BatchWriter and its background goroutine
func (bw *BatchWriter) Stop() {
	close(bw.stopFlush)
}

// LastUsed returns when this writer was last used (for cleanup purposes)
func (bw *BatchWriter) LastUsed() time.Time {
	bw.mu.Lock()
	defer bw.mu.Unlock()
	return bw.lastUsed
}
