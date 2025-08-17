package handler

import (
	"context"
	"fmt"
	"github.com/quantumwake/alethic-ism-core-go/pkg/repository/processor"
	"github.com/quantumwake/alethic-ism-core-go/pkg/routing"
	"runtime/debug"
	"time"
)

// ---- reusable finalizer ----

type AckAction int

const (
	AckOnSuccess AckAction = iota // always ACK on success
)

type FinalizerOptions struct {
	Ctx        context.Context
	Msg        routing.MessageEnvelop
	RouteID    string // optional; empty if N/A
	QueryState any    // optional
	RetErr     *error // pointer to caller's named return err
	// Per-consumer classification and policy:
	IsTerminal func(error) bool          // REQUIRED: decide terminal vs transient
	NakDelay   func(error) time.Duration // OPTIONAL: delay for transient; default 1m
	PanicTerm  bool                      // if true => treat panic as terminal (ACK); else transient (NAK)
	// Hooks (optional)
	OnPublish func(ctx context.Context, routeID string, status processor.Status, note string, qstate any)
}

func Finalize(opts FinalizerOptions) {
	publish := func(st processor.Status, note string, q any) {
		if opts.OnPublish != nil {
			opts.OnPublish(opts.Ctx, opts.RouteID, st, note, q)
		} else if opts.RouteID != "" { // only publish if we have a route context
			PublishRouteStatus(opts.Ctx, opts.RouteID, st, note, q)
		}
	}

	//defer func() {
	if rec := recover(); rec != nil {
		note := fmt.Sprintf("panic: %v\n%s", rec, debug.Stack())
		publish(processor.Failed, note, opts.QueryState)
		if opts.PanicTerm {
			_ = opts.Msg.Ack(opts.Ctx) // consume (no retry)
		} else {
			delay := time.Minute
			if opts.NakDelay != nil {
				delay = opts.NakDelay(fmt.Errorf("panic"))
			}
			_ = opts.Msg.NakWithDelay(opts.Ctx, delay)
		}
		return
	}

	if opts.RetErr != nil && *opts.RetErr != nil {
		err := *opts.RetErr
		term := false
		if opts.IsTerminal != nil {
			term = opts.IsTerminal(err)
		}
		publish(processor.Failed, err.Error(), opts.QueryState)
		if term {
			_ = opts.Msg.Ack(opts.Ctx) // drop (no retry)
		} else {
			delay := time.Minute
			if opts.NakDelay != nil {
				delay = opts.NakDelay(err)
			}
			_ = opts.Msg.NakWithDelay(opts.Ctx, delay)
		}
		return
	}

	// success
	publish(processor.Completed, "", nil)
	_ = opts.Msg.Ack(opts.Ctx)
	//}()
}
