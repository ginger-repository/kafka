package kafka

import (
	"github.com/ginger-core/compound/registry"
	"github.com/ginger-core/log/logger"
	"github.com/ginger-repository/kafka/consumer"
	c "github.com/ginger-repository/kafka/internal/consumer"
)

type Consumer interface {
	consumer.Consumer
}

func NewConsumer(logger logger.Logger, registry registry.Registry) Consumer {
	return c.New(logger, registry)
}
