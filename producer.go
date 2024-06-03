package kafka

import (
	"github.com/ginger-core/compound/registry"
	"github.com/ginger-core/log/logger"
	p "github.com/ginger-repository/kafka/internal/producer"
	"github.com/ginger-repository/kafka/producer"
)

type Producer interface {
	producer.Producer
}

func NewProducer(logger logger.Logger,
	registry registry.Registry) Producer {
	if registry == nil {
		return nil
	}
	return p.New(logger, registry)
}

func NewAsyncProducer(logger logger.Logger,
	registry registry.Registry) Producer {
	if registry == nil {
		return nil
	}
	return p.NewAsync(logger, registry)
}
