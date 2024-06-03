package consumer

import (
	"github.com/ginger-core/errors"
	"github.com/ginger-repository/kafka/message"
)

func (c *c) Start() errors.Error {
	if !c.config.enabled {
		return isNotEnabledError.Clone()
	}
	if c.started {
		return errors.Validation().
			WithTrace("Start.started").
			WithMessage("already started")
	}
	c.startMtx.Lock()
	defer c.startMtx.Unlock()

	c.quitCh = make(chan bool)
	c.closeCh = make(chan bool)
	c.started = true

	c._msgChan = make(chan message.Message)

	c.assignHandlers()

	for i := 0; i < c.config.ConcurrentCount; i++ {
		go c.beginHandleMessage()
	}
	go c.beginCheckBrokers()
	return nil
}
