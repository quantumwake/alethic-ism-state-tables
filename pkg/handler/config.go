package handler

import (
	"encoding/json"
	"fmt"
	"github.com/quantumwake/alethic-ism-core-go/pkg/data"
	"github.com/quantumwake/alethic-ism-core-go/pkg/repository/processor/tables"
)

// getProcessorConfig fetches and parses the table processor configuration from processor properties
func getProcessorConfig(processorID string) (*tables.TableProcessorConfig, error) {
	// Fetch processor from database
	proc, err := processorBackend.FindProcessorByID(processorID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch processor %s: %v", processorID, err)
	}

	// Parse configuration from properties
	config, err := parseProperties(proc.Properties)
	if err != nil {
		return nil, fmt.Errorf("failed to parse table processor config: %v", err)
	}
	return config, nil
}

// parseProperties extracts table processor configuration from processor properties using JSON unmarshaling
func parseProperties(properties *data.JSON) (*tables.TableProcessorConfig, error) {
	// Start with default configuration
	config := tables.DefaultTableProcessorConfig()

	if properties == nil || *properties == nil {
		return config, nil
	}

	// Marshal properties to JSON bytes
	bytes, err := json.Marshal(*properties)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal properties: %v", err)
	}

	// Unmarshal directly into config - nil fields won't be overwritten
	if err := json.Unmarshal(bytes, config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal table processor config: %v", err)
	}

	return config, nil
}
