package consumer

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

type mockDLQWriter struct {
	messages []Message
	errs     []error
	writeErr error
}

func (m *mockDLQWriter) Write(_ context.Context, msg Message, err error) error {
	m.messages = append(m.messages, msg)
	m.errs = append(m.errs, err)
	return m.writeErr
}

func testLogger() (*zap.SugaredLogger, *observer.ObservedLogs) {
	core, logs := observer.New(zapcore.DebugLevel)
	return zap.New(core).Sugar(), logs
}

func TestRetrySuccessFirstAttempt(t *testing.T) {
	calls := 0
	h := Retry(3, time.Millisecond)(func(context.Context, Message) error {
		calls++
		return nil
	})

	require.NoError(t, h(context.Background(), Message{}))
	assert.Equal(t, 1, calls)
}

func TestRetrySuccessAfterFailures(t *testing.T) {
	calls := 0
	h := Retry(3, time.Millisecond)(func(context.Context, Message) error {
		calls++
		if calls < 3 {
			return errors.New("transient")
		}
		return nil
	})

	require.NoError(t, h(context.Background(), Message{}))
	assert.Equal(t, 3, calls)
}

func TestRetryExhaustsAttempts(t *testing.T) {
	calls := 0
	h := Retry(3, time.Millisecond)(func(context.Context, Message) error {
		calls++
		return errors.New("permanent")
	})

	err := h(context.Background(), Message{})
	require.EqualError(t, err, "permanent")
	assert.Equal(t, 3, calls)
}

func TestRetryRespectsContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		calls := 0
		h := Retry(10, 30*time.Second)(func(_ context.Context, _ Message) error {
			calls++
			if calls == 1 {
				cancel()
			}
			return errors.New("fail")
		})

		err := h(ctx, Message{})
		assert.ErrorIs(t, err, context.Canceled)
		assert.Equal(t, 1, calls)
	})
}

func TestRetryAttemptsSetToZero(t *testing.T) {
	calls := 0
	h := Retry(0, time.Millisecond)(func(context.Context, Message) error {
		calls++
		return errors.New("fail")
	})

	require.Error(t, h(context.Background(), Message{}))
	assert.Equal(t, 1, calls)
}

func TestRetryExponentialBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		var timestamps []time.Duration
		h := Retry(4, time.Second)(func(_ context.Context, _ Message) error {
			timestamps = append(timestamps, time.Since(start))
			return errors.New("fail")
		})

		_ = h(context.Background(), Message{})

		require.Len(t, timestamps, 4)
		assert.Equal(t, time.Duration(0), timestamps[0]) // attempt 1: immediate
		assert.Equal(t, 1*time.Second, timestamps[1])    // attempt 2: +1s
		assert.Equal(t, 3*time.Second, timestamps[2])    // attempt 3: +2s
		assert.Equal(t, 7*time.Second, timestamps[3])    // attempt 4: +4s
	})
}

func TestLoggingSuccess(t *testing.T) {
	logger, logs := testLogger()
	h := Logging(logger)(func(context.Context, Message) error { return nil })

	require.NoError(t, h(context.Background(), Message{Topic: "t"}))

	require.Equal(t, 1, logs.Len())
	assert.Equal(t, "processed", logs.All()[0].Message)
	assert.Equal(t, zapcore.DebugLevel, logs.All()[0].Level)
}

func TestLoggingError(t *testing.T) {
	logger, logs := testLogger()
	h := Logging(logger)(func(context.Context, Message) error { return errors.New("bad") })

	err := h(context.Background(), Message{Topic: "t"})
	require.Error(t, err)

	require.Equal(t, 1, logs.Len())
	assert.Equal(t, "processing failed", logs.All()[0].Message)
	assert.Equal(t, zapcore.ErrorLevel, logs.All()[0].Level)
}

func TestLoggingPropagatesError(t *testing.T) {
	logger, _ := testLogger()
	sentinel := errors.New("sentinel")
	h := Logging(logger)(func(context.Context, Message) error { return sentinel })

	err := h(context.Background(), Message{})
	assert.ErrorIs(t, err, sentinel)
}

func TestDLQNotCalled(t *testing.T) {
	dlq := &mockDLQWriter{}
	h := DLQ(dlq)(func(context.Context, Message) error { return nil })

	require.NoError(t, h(context.Background(), Message{}))
	assert.Empty(t, dlq.messages)
}

func TestDLQRoutesToWriter(t *testing.T) {
	dlq := &mockDLQWriter{}
	h := DLQ(dlq)(func(context.Context, Message) error {
		return errors.New("process failed")
	})

	msg := Message{Key: []byte("k"), Value: []byte("v"), Topic: "t"}
	err := h(context.Background(), msg)

	require.NoError(t, err)
	require.Len(t, dlq.messages, 1)
	assert.Equal(t, []byte("k"), dlq.messages[0].Key)
	assert.Equal(t, []byte("v"), dlq.messages[0].Value)
	assert.EqualError(t, dlq.errs[0], "process failed")
}

func TestDLQWriteFailsReturnsError(t *testing.T) {
	dlq := &mockDLQWriter{writeErr: errors.New("dlq down")}
	h := DLQ(dlq)(func(context.Context, Message) error {
		return errors.New("process failed")
	})

	err := h(context.Background(), Message{})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrForRetry)
}

func TestRetryThenDLQ(t *testing.T) {
	calls := 0
	dlq := &mockDLQWriter{}

	h := DLQ(dlq)(Retry(2, time.Millisecond)(func(context.Context, Message) error {
		calls++
		return errors.New("always fails")
	}))

	err := h(context.Background(), Message{Key: []byte("k")})
	require.NoError(t, err)
	assert.Equal(t, 2, calls)
	require.Len(t, dlq.messages, 1)
	assert.Equal(t, []byte("k"), dlq.messages[0].Key)
}
