package consumer

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
)

func Retry(maxAttempts int, baseDelay time.Duration) Middleware {
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	return func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, msg Message) error {
			var lastErr error
			delay := baseDelay
			for attempt := range maxAttempts {
				if err := next(ctx, msg); err != nil {
					lastErr = err

					if attempt < maxAttempts-1 {
						sleepCtx(ctx, delay)
						delay *= 2
						if ctx.Err() != nil {
							return ctx.Err()
						}
					}

					continue
				}
				return nil
			}
			return lastErr
		}
	}
}

func Logging(logger *zap.SugaredLogger) Middleware {
	return func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, msg Message) error {
			start := time.Now()
			err := next(ctx, msg)

			data := []any{ // here we use json only, but i know that we use proto in our company, as an improvement i can also add a proto here
				"topic", msg.Topic,
				"partition", msg.Partition,
				"offset", msg.Offset,
				"duration", time.Since(start),
			}
			if err != nil {
				logger.Errorw("processing failed", append(data, "err", err)...)
			} else {
				logger.Debugw("processed", data...)
			}
			return err
		}
	}
}

type DLQWriter interface {
	Write(ctx context.Context, msg Message, processingErr error) error
}

func DLQ(w DLQWriter) Middleware {
	return func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, msg Message) error {
			err := next(ctx, msg)
			if err == nil {
				return nil
			}

			if dlqErr := w.Write(ctx, msg, err); dlqErr != nil {
				return fmt.Errorf("%w: dlq write failed: %w (original: %w)", ErrForRetry, dlqErr, err)
			}
			return nil
		}
	}
}
