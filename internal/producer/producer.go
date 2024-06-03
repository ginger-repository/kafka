package producer

import (
	"sync"

	"github.com/IBM/sarama"
	"github.com/ginger-core/compound/registry"
	"github.com/ginger-core/log/logger"
	"github.com/ginger-repository/kafka/producer"
)

type p struct {
	logger logger.Logger
	config config

	writer      writer
	producerMtx *sync.RWMutex
}

func newBase(logger logger.Logger, registry registry.Registry) *p {
	p := &p{
		logger:      logger,
		producerMtx: new(sync.RWMutex),
		config: config{
			Properties: sarama.NewConfig(),
		},
	}
	if err := registry.WithDelimiter("..").Unmarshal(&p.config); err != nil {
		panic(err)
	}
	p.config.initialize()

	p.config.Properties.Producer.Return.Errors = true
	p.config.Properties.Producer.Return.Successes = true
	return p
}

func New(logger logger.Logger,
	registry registry.Registry) producer.Producer {
	p := newBase(logger, registry)
	p.writer = newSyncWriter(p.config.Brokers, p.config.Properties)
	return p
}

func NewAsync(logger logger.Logger,
	registry registry.Registry) producer.Producer {
	p := newBase(logger, registry)
	p.writer = newAsyncWriter(p.config.Brokers, p.config.Properties)
	return p
}
