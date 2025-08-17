package handler

import (
	"context"
	"encoding/json"
	
	"github.com/quantumwake/alethic-ism-core-go/pkg/data/models"
	"github.com/quantumwake/alethic-ism-core-go/pkg/repository/processor"
	"github.com/quantumwake/alethic-ism-core-go/pkg/routing"
)

func MessageCallback(ctx context.Context, msg routing.MessageEnvelop) {
	ingestedRawMessage, err := msg.MessageRaw()

	// unmarshal the message into a map object for processing the message ingestedRawMessage and metadata fields (e.g. binding)
	var ingestedRouteMsg models.RouteMessage
	err = json.Unmarshal(ingestedRawMessage, &ingestedRouteMsg)
	if err != nil {
		PublishRouteStatus(ctx, ingestedRouteMsg.RouteID, processor.Failed, err.Error(), ingestedRouteMsg.QueryState)
		return
	}

	defer Finalize(FinalizerOptions{
		Ctx:        ctx,
		Msg:        msg,
		RouteID:    ingestedRouteMsg.RouteID,
		QueryState: ingestedRouteMsg.QueryState,
		RetErr:     &err,
		IsTerminal: IsTerminalError,
		NakDelay:   nil,
		PanicTerm:  true,
		OnPublish:  PublishRouteStatus,
	})

	route, err := routeBackend.FindRouteByID(ingestedRouteMsg.RouteID)
	if err != nil {
		return
	}

	config, err := getProcessorConfig(route.ProcessorID)
	if err != nil {
		return
	}

	// Get or create batch writer for this processor
	writer := GetBatchWriter(route.ProcessorID, config)
	
	// Add records to batch (will auto-flush based on config thresholds)
	err = writer.Add(ingestedRouteMsg.QueryState)
}

func IsTerminalError(err error) bool {
	return true
}
