package consumer

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSleepCtxExpires(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		sleepCtx(context.Background(), 5*time.Second)
		assert.Equal(t, 5*time.Second, time.Since(start))
	})
}

func TestSleepCtxCancelled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		start := time.Now()
		sleepCtx(ctx, time.Minute)
		assert.Equal(t, time.Duration(0), time.Since(start))
	})
}
