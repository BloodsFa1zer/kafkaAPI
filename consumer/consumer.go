package consumer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// ErrForRetry used to avoid message commit, so that after next rebalance we can try to commit message again
var ErrForRetry = errors.New("for retry")

type Message struct {
	Key       []byte
	Value     []byte
	Headers   map[string][]byte
	Topic     string
	Partition int
	Offset    int64
	Time      time.Time
}

type HandlerFunc func(ctx context.Context, msg Message) error

type Middleware func(next HandlerFunc) HandlerFunc

type Config struct {
	Brokers []string
	Topic   string
	GroupID string

	WorkersCount int

	StartOffset      int64
	MaxWait          time.Duration
	MaxBytes         int
	RebalanceTimeout time.Duration

	FetchErrorDelay time.Duration
	GracefulTimeout time.Duration

	Logger *zap.SugaredLogger
	Dialer *kafka.Dialer
}

func (c *Config) setDefaults() {
	if c.WorkersCount < 1 {
		c.WorkersCount = 1
	}
	if c.StartOffset == 0 {
		c.StartOffset = kafka.LastOffset
	}
	if c.MaxWait == 0 {
		c.MaxWait = 3 * time.Second
	}
	if c.MaxBytes == 0 {
		c.MaxBytes = 10 << 20
	}
	if c.RebalanceTimeout == 0 {
		c.RebalanceTimeout = 30 * time.Second
	}
	if c.FetchErrorDelay == 0 {
		c.FetchErrorDelay = time.Second
	}
	if c.GracefulTimeout == 0 {
		c.GracefulTimeout = 10 * time.Second
	}
}

func (c *Config) validate() error {
	var brokerErr, topicErr, groupErr error
	if len(c.Brokers) == 0 {
		brokerErr = errors.New("brokers required")
	}
	if c.Topic == "" {
		topicErr = errors.New("topic required")
	}
	if c.GroupID == "" {
		groupErr = errors.New("group ID required")
	}

	return errors.Join(brokerErr, topicErr, groupErr)
}

type Consumer struct {
	cfg     Config
	handler HandlerFunc
	logger  *zap.SugaredLogger

	shutdownC chan struct{}
	doneC     chan struct{}
	shutdown  sync.Once
}

func New(cfg Config, handler HandlerFunc, mw ...Middleware) (*Consumer, error) {
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("consumer: %w", err)
	}
	cfg.setDefaults()

	for _, m := range mw {
		handler = m(handler)
	}

	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop().Sugar()
	}

	return &Consumer{
		cfg:       cfg,
		handler:   handler,
		logger:    logger,
		shutdownC: make(chan struct{}),
		doneC:     make(chan struct{}),
	}, nil
}

func (c *Consumer) Run(ctx context.Context) error {
	defer close(c.doneC)

	readCtx, cancelRead := context.WithCancel(context.Background())
	defer cancelRead()

	processCtx, cancelProcess := context.WithCancel(context.Background())
	defer cancelProcess()

	workersDone := make(chan struct{})

	go c.awaitShutdown(ctx, workersDone, cancelRead, cancelProcess)

	var errG errgroup.Group

	for i := range c.cfg.WorkersCount {
		errG.Go(func() error {
			return c.worker(readCtx, processCtx, i)
		})
	}

	err := errG.Wait()
	close(workersDone)
	return err
}

func (c *Consumer) Shutdown(ctx context.Context) error {
	c.shutdown.Do(func() { close(c.shutdownC) })
	select {
	case <-c.doneC:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("shutdown: %w", ctx.Err())
	}
}

func (c *Consumer) awaitShutdown(ctx context.Context, workersDone <-chan struct{}, cancelRead, cancelProcess context.CancelFunc) {
	select {
	case <-c.shutdownC: // catch shutdown() case
		cancelRead()
	case <-ctx.Done(): // main ctx cancelled
		cancelRead()
	case <-workersDone:
		return
	}

	timer := time.NewTimer(c.cfg.GracefulTimeout)
	defer timer.Stop()
	select {
	case <-timer.C: // exceed graceful period
		cancelProcess()
	case <-workersDone: // workers drained before timeout
		cancelProcess()
	}
}

func (c *Consumer) worker(readCtx, processCtx context.Context, id int) error {
	rcfg := kafka.ReaderConfig{
		Brokers:          c.cfg.Brokers,
		Topic:            c.cfg.Topic,
		GroupID:          c.cfg.GroupID,
		StartOffset:      c.cfg.StartOffset,
		MaxWait:          c.cfg.MaxWait,
		MaxBytes:         c.cfg.MaxBytes,
		RebalanceTimeout: c.cfg.RebalanceTimeout,
	}
	if c.cfg.Dialer != nil {
		rcfg.Dialer = c.cfg.Dialer
	}

	reader := kafka.NewReader(rcfg)
	defer func() {
		if err := reader.Close(); err != nil {
			c.logger.Errorw("reader close error", "worker", id, "err", err)
		}
	}()

	for {
		msg, err := reader.FetchMessage(readCtx)
		if err != nil {
			if readCtx.Err() != nil {
				return nil
			}
			c.logger.Warnw("fetch error", "worker", id, "err", err)
			sleepCtx(processCtx, c.cfg.FetchErrorDelay)

			continue
		}

		m := toMessage(msg)

		if err := c.handler(processCtx, m); err != nil {
			if errors.Is(err, ErrForRetry) {
				c.logger.Warnw("commit skipped",
					"worker", id, "topic", m.Topic,
					"partition", m.Partition, "offset", m.Offset,
					"reason", err,
				)
				continue
			}

			c.logger.Errorw("handler error",
				"worker", id, "topic", m.Topic,
				"partition", m.Partition, "offset", m.Offset,
				"err", err,
			)
		}

		if err := c.commit(processCtx, reader, msg); err != nil {
			if processCtx.Err() != nil {
				return nil
			}
			c.logger.Errorw("commit failed",
				"worker", id, "topic", m.Topic,
				"partition", m.Partition, "offset", m.Offset,
				"err", err,
			)
		}
	}
}

const commitRetries = 3

func (c *Consumer) commit(ctx context.Context, reader *kafka.Reader, msg kafka.Message) error {
	var lastErr error
	for attempt := range commitRetries {
		if err := reader.CommitMessages(ctx, msg); err != nil {
			lastErr = err
			if ctx.Err() != nil {
				return ctx.Err()
			}

			delay := time.Duration(attempt+1) * time.Second
			c.logger.Warnw("commit retry", "attempt", attempt+1, "err", err, "backoff", delay)
			sleepCtx(ctx, delay)

			continue
		}
		return nil
	}
	return fmt.Errorf("commit failed after %d attempts: %w", commitRetries, lastErr)
}

func toMessage(m kafka.Message) Message {
	h := make(map[string][]byte, len(m.Headers))
	for _, kh := range m.Headers {
		h[kh.Key] = kh.Value
	}

	return Message{
		Key:       m.Key,
		Value:     m.Value,
		Headers:   h,
		Topic:     m.Topic,
		Partition: m.Partition,
		Offset:    m.Offset,
		Time:      m.Time,
	}
}

func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-t.C:
	case <-ctx.Done():
	}
}
