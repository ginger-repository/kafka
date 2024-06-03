package producer

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/ginger-core/errors"
	"github.com/ginger-repository/kafka/message"
)

func (p *p) Produce(ctx context.Context, msg message.Message) errors.Error {
	if !p.config.enabled {
		return isNotEnabledError.Clone()
	}
	p.producerMtx.RLock()
	defer p.producerMtx.RUnlock()
	if p.producerMtx == nil {
		return writerNotInitializedError.Clone()
	}

	input := &sarama.ProducerMessage{
		Topic: p.config.Topic,
		Value: sarama.ByteEncoder(msg.GetValue()),
	}
	if topic := msg.GetTopic(); topic != nil {
		input.Topic = topic.GetName()
	}
	if key := msg.GetKey(); key != nil {
		input.Key = sarama.ByteEncoder(key)
	}
	if headers := msg.GetHeaders(); len(headers) > 0 {
		for _, h := range headers {
			input.Headers = append(input.Headers,
				sarama.RecordHeader{
					Key:   []byte(h.Key),
					Value: h.Value,
				})
		}
	}
	err := p.writer.write(ctx, input)
	if err != nil {
		return errors.New(err).
			WithTrace("producer.SendMessage")
	}
	return nil
}
