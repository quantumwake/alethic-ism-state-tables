package handler

import (
	"sync"
	"time"

	"github.com/quantumwake/alethic-ism-core-go/pkg/repository/processor/tables"
)

const (
	cleanupInterval = 5 * time.Minute  // How often to check for idle writers
	maxIdleTime     = 10 * time.Minute // Writers idle longer than this are removed
)

var (
	// Global cache for BatchWriter instances, one per processor
	writerCache = &WriterCache{
		writers:     make(map[string]*BatchWriter),
		stopCleanup: make(chan struct{}),
	}
)

// WriterCache manages BatchWriter instances with automatic cleanup of idle writers
type WriterCache struct {
	mu          sync.RWMutex
	writers     map[string]*BatchWriter  // ProcessorID -> BatchWriter mapping
	stopCleanup chan struct{}             // Signal to stop cleanup goroutine
}

func init() {
	// Start background cleanup goroutine on initialization
	go writerCache.cleanupRoutine()
}

// GetBatchWriter returns a cached BatchWriter for the given processor, creating one if needed
func GetBatchWriter(processorID string, config *tables.TableProcessorConfig) *BatchWriter {
	writerCache.mu.RLock()
	if writer, exists := writerCache.writers[processorID]; exists {
		writerCache.mu.RUnlock()
		return writer
	}
	writerCache.mu.RUnlock()

	// Create new writer with write lock
	writerCache.mu.Lock()
	defer writerCache.mu.Unlock()
	
	// Double-check pattern to avoid race conditions
	if writer, exists := writerCache.writers[processorID]; exists {
		return writer
	}

	writer := NewBatchWriter(processorID, config)
	writerCache.writers[processorID] = writer
	
	return writer
}

// cleanupRoutine periodically removes idle BatchWriters
func (wc *WriterCache) cleanupRoutine() {
	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			wc.cleanup()
		case <-wc.stopCleanup:
			wc.stopAll()
			return
		}
	}
}

// cleanup removes BatchWriters that have been idle too long
func (wc *WriterCache) cleanup() {
	wc.mu.Lock()
	defer wc.mu.Unlock()

	now := time.Now()
	for id, writer := range wc.writers {
		if now.Sub(writer.LastUsed()) > maxIdleTime {
			writer.Stop()  // Gracefully stop the writer
			delete(wc.writers, id)
		}
	}
}

// stopAll gracefully stops all BatchWriters (called on shutdown)
func (wc *WriterCache) stopAll() {
	wc.mu.Lock()
	defer wc.mu.Unlock()

	for _, writer := range wc.writers {
		writer.Stop()
	}
	wc.writers = make(map[string]*BatchWriter)
}

// StopWriterCache gracefully shuts down the cache and all writers
func StopWriterCache() {
	close(writerCache.stopCleanup)
}