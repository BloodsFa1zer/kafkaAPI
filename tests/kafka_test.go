package tests

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"kafkaAPI/consumer"
	"kafkaAPI/producer"
)

func setup(t *testing.T, partitions int) (brokers []string, topic string) {
	t.Helper()
	b := os.Getenv("KAFKA_BROKERS")
	if b == "" {
		t.Skip("set KAFKA_BROKERS to run tests")
	}
	brokers = strings.Split(b, ",")
	topic = strings.ReplaceAll(t.Name(), "/", "-") + fmt.Sprintf("-%d", time.Now().UnixNano())
	createTopic(t, brokers, topic, partitions)
	return
}

func publish(t *testing.T, brokers []string, topic string, msgs ...producer.Message) {
	t.Helper()
	p, err := producer.New(producer.Config{Brokers: brokers, Topic: topic})
	require.NoError(t, err)
	defer p.Close()
	require.NoError(t, p.Publish(context.Background(), msgs...))
}

func shutdown(t *testing.T, c *consumer.Consumer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, c.Shutdown(ctx))
}

func deleteTopic(t *testing.T, brokers []string, topic string) {
	t.Helper()
	for _, addr := range brokers {
		conn, err := kafka.Dial("tcp", addr)
		if err != nil {
			continue
		}

		_ = conn.DeleteTopics(topic)
		conn.Close()
		return
	}
}

func createTopic(t *testing.T, brokers []string, topic string, partitions int) {
	t.Helper()

	var createErr error
	for _, addr := range brokers {
		conn, err := kafka.Dial("tcp", addr)
		if err != nil {
			createErr = err
			continue
		}

		createErr = conn.CreateTopics(kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     partitions,
			ReplicationFactor: 1,
		})

		conn.Close()
		if createErr == nil {
			break
		}
	}
	require.NoError(t, createErr, "create topic %s", topic)

	for _, addr := range brokers {
		deadline := time.Now().Add(15 * time.Second)

		for time.Now().Before(deadline) {
			conn, err := kafka.Dial("tcp", addr)
			if err != nil {
				time.Sleep(500 * time.Millisecond)
				continue
			}

			parts, err := conn.ReadPartitions(topic)
			conn.Close()
			if err == nil && len(parts) >= partitions {
				break
			}

			time.Sleep(500 * time.Millisecond)
		}
	}
	time.Sleep(2 * time.Second)
}

func TestProduceAndConsume(t *testing.T) {
	brokers, topic := setup(t, 3)

	sent := map[string]string{"1": "one", "2": "two", "3": "three"}
	msgs := make([]producer.Message, 0, len(sent))
	for k, v := range sent {
		msgs = append(msgs, producer.Message{Key: []byte(k), Value: []byte(v)})
	}
	publish(t, brokers, topic, msgs...)

	var mu sync.Mutex
	received := make(map[string]string)
	allReceived := make(chan struct{})

	c, err := consumer.New(
		consumer.Config{
			Brokers:     brokers,
			Topic:       topic,
			GroupID:     "test-" + topic,
			StartOffset: kafka.FirstOffset,
		},
		func(_ context.Context, msg consumer.Message) error {
			mu.Lock()
			received[string(msg.Key)] = string(msg.Value)
			done := len(received) >= len(sent)
			mu.Unlock()
			if done {
				select {
				case allReceived <- struct{}{}:
				default:
				}
			}
			return nil
		},
	)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	go c.Run(ctx)

	select {
	case <-allReceived:
	case <-ctx.Done():
		t.Fatal("timeout waiting for all messages")
	}

	shutdown(t, c)

	for k, v := range sent {
		assert.Equal(t, v, received[k], "key=%s", k)
	}
}

func TestConsumerShutdown(t *testing.T) {
	brokers, topic := setup(t, 1)

	c, err := consumer.New(
		consumer.Config{
			Brokers: brokers,
			Topic:   topic,
			GroupID: "test-shutdown-" + topic,
		},
		func(context.Context, consumer.Message) error { return nil },
	)
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- c.Run(context.Background()) }()

	time.Sleep(2 * time.Second)
	shutdown(t, c)

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after Shutdown")
	}
}

type memDLQ struct {
	mu   sync.Mutex
	msgs []consumer.Message
	errs []error
	ch   chan struct{}
}

func newMemDLQ() *memDLQ {
	return &memDLQ{ch: make(chan struct{}, 1)}
}

func (d *memDLQ) Write(_ context.Context, msg consumer.Message, err error) error {
	d.mu.Lock()
	d.msgs = append(d.msgs, msg)
	d.errs = append(d.errs, err)

	d.mu.Unlock()
	select {
	case d.ch <- struct{}{}:
	default:
	}
	return nil
}

func TestConsumerDLQ(t *testing.T) {
	brokers, topic := setup(t, 1)
	publish(t, brokers, topic, producer.Message{Key: []byte("bad"), Value: []byte("bad-data")})

	dlq := newMemDLQ()

	c, err := consumer.New(
		consumer.Config{
			Brokers:     brokers,
			Topic:       topic,
			GroupID:     "test-dlq-" + topic,
			StartOffset: kafka.FirstOffset,
		},
		func(context.Context, consumer.Message) error { return fmt.Errorf("bad data") },
		consumer.DLQ(dlq),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	go c.Run(ctx)

	select {
	case <-dlq.ch:
	case <-ctx.Done():
		t.Fatal("timeout waiting for DLQ write")
	}

	shutdown(t, c)

	dlq.mu.Lock()
	defer dlq.mu.Unlock()
	require.GreaterOrEqual(t, len(dlq.msgs), 1)
	assert.Equal(t, []byte("bad"), dlq.msgs[0].Key)
	assert.Equal(t, []byte("bad-data"), dlq.msgs[0].Value)
	assert.EqualError(t, dlq.errs[0], "bad data")
}

func TestShutdownWaits(t *testing.T) {
	brokers, topic := setup(t, 1)
	publish(t, brokers, topic, producer.Message{Key: []byte("k"), Value: []byte("v")})

	handlerStarted := make(chan struct{})
	var handlerCompleted bool
	var ctxCancelledDuringProcessing bool

	c, err := consumer.New(
		consumer.Config{
			Brokers:         brokers,
			Topic:           topic,
			GroupID:         "test-graceful-" + topic,
			StartOffset:     kafka.FirstOffset,
			GracefulTimeout: 5 * time.Second,
		},
		func(ctx context.Context, _ consumer.Message) error {
			close(handlerStarted)
			time.Sleep(500 * time.Millisecond)
			ctxCancelledDuringProcessing = ctx.Err() != nil
			handlerCompleted = true
			return nil
		},
	)
	require.NoError(t, err)

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	runDone := make(chan error, 1)
	go func() { runDone <- c.Run(runCtx) }()

	select {
	case <-handlerStarted:
	case <-time.After(15 * time.Second):
		t.Fatal("handler never started")
	}

	cancelRun()

	select {
	case err := <-runDone:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after ctx cancellation")
	}

	assert.True(t, handlerCompleted)
	assert.False(t, ctxCancelledDuringProcessing)
}

func TestGracefulTimeoutCancels(t *testing.T) {
	brokers, topic := setup(t, 1)
	publish(t, brokers, topic, producer.Message{Key: []byte("k"), Value: []byte("v")})

	handlerStarted := make(chan struct{})
	var ctxCancelledDuringProcessing bool

	c, err := consumer.New(
		consumer.Config{
			Brokers:         brokers,
			Topic:           topic,
			GroupID:         "test-graceful-timeout-" + topic,
			StartOffset:     kafka.FirstOffset,
			GracefulTimeout: 200 * time.Millisecond,
		},
		func(ctx context.Context, _ consumer.Message) error {
			close(handlerStarted)
			select {
			case <-time.After(5 * time.Second):
			case <-ctx.Done():
			}
			ctxCancelledDuringProcessing = ctx.Err() != nil
			return ctx.Err()
		},
	)
	require.NoError(t, err)

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	runDone := make(chan error, 1)
	go func() { runDone <- c.Run(runCtx) }()

	select {
	case <-handlerStarted:
	case <-time.After(15 * time.Second):
		t.Fatal("handler never started")
	}

	cancelRun()

	select {
	case <-runDone:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after GracefulTimeout")
	}

	assert.True(t, ctxCancelledDuringProcessing)
}

func TestConsumerRetryThenSucceed(t *testing.T) {
	brokers, topic := setup(t, 1)
	publish(t, brokers, topic, producer.Message{Key: []byte("retry-me"), Value: []byte("data")})

	var attempts atomic.Int32
	done := make(chan struct{})

	c, err := consumer.New(
		consumer.Config{
			Brokers:     brokers,
			Topic:       topic,
			GroupID:     "test-retry-" + topic,
			StartOffset: kafka.FirstOffset,
		},
		func(context.Context, consumer.Message) error {
			a := attempts.Add(1)
			if a < 3 {
				return fmt.Errorf("transient (attempt %d)", a)
			}
			select {
			case done <- struct{}{}:
			default:
			}
			return nil
		},
		consumer.Retry(3, 50*time.Millisecond),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	go c.Run(ctx)

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timeout waiting for retry success")
	}

	shutdown(t, c)

	assert.Equal(t, int32(3), attempts.Load())
}
