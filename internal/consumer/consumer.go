package consumer

import (
	"sync"

	"github.com/ginger-core/compound/registry"
	"github.com/ginger-core/log/logger"
	"github.com/ginger-repository/kafka/consumer"
	"github.com/ginger-repository/kafka/message"
)

type c struct {
	logger logger.Logger
	config config

	handlers []groupHandler

	started  bool
	startMtx *sync.RWMutex

	msgChan        chan<- message.Message
	msgHandlerFunc consumer.MessageHandlerFunc

	_msgChan chan message.Message

	quitCh  chan bool
	closeCh chan bool
}

func New(logger logger.Logger, registry registry.Registry) consumer.Consumer {
	c := &c{
		logger:   logger,
		config:   newConfig(),
		startMtx: new(sync.RWMutex),
	}
	if err := registry.Unmarshal(&c.config); err != nil {
		panic(err)
	}
	c.config.initialize()
	return c
}
