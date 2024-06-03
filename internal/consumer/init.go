package consumer

import (
	"github.com/ginger-repository/kafka/consumer"
	"github.com/ginger-repository/kafka/message"
)

func (c *c) WithChannel(channel chan<- message.Message) consumer.Consumer {
	c.msgChan = channel
	return c
}

func (c *c) WithMessageHandler(f consumer.MessageHandlerFunc) consumer.Consumer {
	c.msgHandlerFunc = f
	return c
}
