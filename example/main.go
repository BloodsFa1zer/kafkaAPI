package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"kafkaAPI/consumer"
	"kafkaAPI/producer"
)

type Order struct {
	ID     string `json:"id"`
	Amount int    `json:"amount"`
}

type kafkaDLQ struct {
	p     *producer.Producer
	topic string
}

func (d *kafkaDLQ) Write(ctx context.Context, msg consumer.Message, processingErr error) error {
	return d.p.Publish(ctx, producer.Message{
		Topic: d.topic,
		Key:   msg.Key,
		Value: msg.Value,
		Headers: map[string][]byte{
			"dlq-error":          []byte(processingErr.Error()),
			"dlq-original-topic": []byte(msg.Topic),
		},
	})
}

func main() {
	simulate()
}

func simulate() {
	z, _ := zap.NewDevelopment()
	defer z.Sync()
	logger := z.Sugar()

	brokers := []string{"localhost:29092", "localhost:39092", "localhost:49092"}
	topic := "simulate-orders"

	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		logger.Fatalw("dial broker", "err", err)
	}
	_ = conn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     3,
		ReplicationFactor: 1,
	})
	conn.Close()
	time.Sleep(2 * time.Second)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	p, err := producer.New(producer.Config{
		Brokers:      brokers,
		Topic:        topic,
		RequiredAcks: producer.RequireAll,
	})
	if err != nil {
		logger.Fatalw("create producer", "err", err)
	}
	defer p.Close()

	// Publish a batch: 5 valid orders, 2 malformed JSON, 3 retryable orders
	var msgs []producer.Message
	for i := 1; i <= 5; i++ {
		val, _ := json.Marshal(Order{
			ID:     fmt.Sprintf("ord-%d", i),
			Amount: 500,
		})
		msgs = append(msgs, producer.Message{Key: []byte(fmt.Sprintf("ord-%d", i)), Value: val})
	}
	for i := 1; i <= 2; i++ {
		msgs = append(msgs, producer.Message{
			Key:   []byte(fmt.Sprintf("bad-%d", i)),
			Value: []byte("{not valid json!!!"),
		})
	}
	for i := 1; i <= 3; i++ {
		val, _ := json.Marshal(Order{
			ID:     fmt.Sprintf("flaky-%d", i),
			Amount: 100,
		})
		msgs = append(msgs, producer.Message{Key: []byte(fmt.Sprintf("flaky-%d", i)), Value: val})
	}

	if err := p.Publish(ctx, msgs...); err != nil {
		logger.Fatalw("publish batch", "err", err)
	}
	logger.Infow("published messages", "count", len(msgs))

	dlqProd, err := producer.New(producer.Config{Brokers: brokers})
	if err != nil {
		logger.Fatalw("create dlq producer", "err", err)
	}
	defer dlqProd.Close()

	dlq := &kafkaDLQ{p: dlqProd, topic: topic + "-dlq"}

	handle := func(_ context.Context, msg consumer.Message) error {
		key := string(msg.Key)

		var o Order
		if err := json.Unmarshal(msg.Value, &o); err != nil {
			return fmt.Errorf("unmarshal %s: %w", key, err)
		}

		if len(key) > 6 && key[:6] == "flaky-" {
			return fmt.Errorf("transient error: %s", key)
		}

		logger.Infow("order processed", "id", o.ID, "amount", o.Amount)
		return nil
	}

	c, err := consumer.New(
		consumer.Config{
			Brokers:      brokers,
			Topic:        topic,
			GroupID:      "simulate-group",
			WorkersCount: 2,
			StartOffset:  kafka.FirstOffset,
			Logger:       logger,
		},
		handle,
		consumer.Retry(3, 500*time.Millisecond),
		consumer.DLQ(dlq),
		consumer.Logging(logger),
	)
	if err != nil {
		logger.Fatalw("create consumer", "err", err)
	}

	runDone := make(chan error, 1)
	go func() { runDone <- c.Run() }()

	<-ctx.Done()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := c.Shutdown(shutdownCtx); err != nil {
		logger.Fatalw("shutdown error", "err", err)
	}

	if err := <-runDone; err != nil {
		logger.Fatalw("consumer stopped with error", "err", err)
	}
	logger.Infow("simulation complete")
}
