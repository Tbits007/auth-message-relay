package producer

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{
		writer: kafka.NewWriter(
			kafka.WriterConfig{
				Brokers:       brokers,
				Topic:         topic,
				Balancer:      &kafka.LeastBytes{},
				BatchSize:     20,
				BatchTimeout:  50 * time.Millisecond,
				RequiredAcks:  -1,
				Async:         true,
			},
		),
	}
}
   

func (p *Producer) SendMessage(ctx context.Context, key, value []byte) error {
	return p.writer.WriteMessages(
		ctx, 
		kafka.Message{
			Key:   key,
			Value: value,
		},
	)
}
   
   
func (p *Producer) Close() error {
	return p.writer.Close()
}