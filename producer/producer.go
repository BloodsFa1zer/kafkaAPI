package producer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

type Message struct {
	Key     []byte
	Value   []byte
	Headers map[string][]byte
	Topic   string
}

type Config struct {
	Brokers []string
	Topic   string

	Async        bool
	RequiredAcks RequiredAcks
	BatchSize    int
	BatchTimeout time.Duration
	MaxAttempts  int
	WriteTimeout time.Duration
	ReadTimeout  time.Duration

	Balancer Balancer

	Dialer *kafka.Dialer
}

func (c *Config) setDefaults() {
	if c.RequiredAcks == 0 {
		c.RequiredAcks = RequireOne
	}
	if c.BatchSize == 0 {
		c.BatchSize = 100
	}
	if c.BatchTimeout == 0 {
		c.BatchTimeout = 10 * time.Millisecond
	}
	if c.MaxAttempts == 0 {
		c.MaxAttempts = 3
	}
	if c.WriteTimeout == 0 {
		c.WriteTimeout = 10 * time.Second
	}
	if c.ReadTimeout == 0 {
		c.ReadTimeout = 10 * time.Second
	}
}

func (c *Config) validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("brokers required")
	}
	return nil
}

type Producer struct {
	writer *kafka.Writer
}

func New(cfg Config) (*Producer, error) {
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("producer: %w", err)
	}
	cfg.setDefaults()

	w := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.Topic,
		Balancer:     resolveBalancer(cfg.Balancer),
		RequiredAcks: kafka.RequiredAcks(cfg.RequiredAcks),
		BatchSize:    cfg.BatchSize,
		BatchTimeout: cfg.BatchTimeout,
		MaxAttempts:  cfg.MaxAttempts,
		WriteTimeout: cfg.WriteTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		Async:        cfg.Async,
	}
	if cfg.Dialer != nil {
		w.Transport = &kafka.Transport{
			TLS:  cfg.Dialer.TLS,
			SASL: cfg.Dialer.SASLMechanism,
		}
	}

	return &Producer{writer: w}, nil
}

func (p *Producer) Publish(ctx context.Context, msgs ...Message) error {
	if len(msgs) == 0 {
		return nil
	}
	km := make([]kafka.Message, len(msgs))

	for i, m := range msgs {
		km[i] = toKafkaMessage(m)
	}
	if err := p.writer.WriteMessages(ctx, km...); err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	return nil
}

func (p *Producer) Close() error {
	return p.writer.Close()
}

func toKafkaMessage(m Message) kafka.Message {
	headers := make([]kafka.Header, 0, len(m.Headers))
	for k, v := range m.Headers {
		headers = append(headers, kafka.Header{Key: k, Value: v})
	}
	return kafka.Message{
		Topic:   m.Topic,
		Key:     m.Key,
		Value:   m.Value,
		Headers: headers,
	}
}

type RequiredAcks int

const (
	RequireNone RequiredAcks = 0
	RequireOne  RequiredAcks = 1
	RequireAll  RequiredAcks = -1
)

type Balancer string

const (
	BalancerHash       Balancer = "hash"
	BalancerLeastBytes Balancer = "least-bytes"
	BalancerCRC32      Balancer = "crc32"
)

func resolveBalancer(b Balancer) kafka.Balancer {
	switch b {
	case BalancerHash:
		return &kafka.Hash{}
	case BalancerLeastBytes:
		return &kafka.LeastBytes{}
	case BalancerCRC32:
		return &kafka.CRC32Balancer{}
	default:
		return &kafka.RoundRobin{}
	}
}
