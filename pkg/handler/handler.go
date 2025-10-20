package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/quantumwake/alethic-ism-core-go/pkg/data/models"
	"github.com/quantumwake/alethic-ism-core-go/pkg/routing"
	"log"
)

func MessageCallback(ctx context.Context, msg routing.MessageEnvelop) {

	defer func() {
		err := msg.Ack(ctx)
		if err != nil {
			fmt.Printf("error acking message in finalizer: %v\n", err)
		}
	}()

	ingestedRawMessage, err := msg.MessageRaw()

	// unmarshal the message into a map object for processing the message ingestedRawMessage and metadata fields (e.g. binding)
	var ingestedRouteMsg models.RouteMessage
	err = json.Unmarshal(ingestedRawMessage, &ingestedRouteMsg)
	if err != nil {
		log.Printf("error unmarshalling route message: %v\n", err)
		return
	}

	//defer Finalize(FinalizerOptions{
	//	Ctx:        ctx,
	//	Msg:        msg,
	//	RouteID:    ingestedRouteMsg.RouteID,
	//	QueryState: ingestedRouteMsg.QueryState,
	//	RetErr:     &err,
	//	IsTerminal: IsTerminalError,
	//	NakDelay:   nil,
	//	PanicTerm:  true,
	//	OnPublish:  nil, // Status publishing handled by batch flush
	//})

	route, err := routeBackend.FindRouteByID(ingestedRouteMsg.RouteID)
	if err != nil {
		log.Printf("error finding route: %v\n", err)
		return
	}

	config, err := getProcessorConfig(route.ProcessorID)
	if err != nil {
		log.Printf("error getting processor config for processor ID %v: %v\n", route.ProcessorID, err)
		return
	}

	// Get or create batch writer for this processor
	writer := GetBatchWriter(route.ProcessorID, config)

	// Add records to batch (will auto-flush based on config thresholds)
	// Status will be published when the batch flushes
	err = writer.Add(ingestedRouteMsg.RouteID, ingestedRouteMsg.QueryState)
}

func IsTerminalError(err error) bool {
	return true
}
