package consumer

import (
	"context"
	"time"

	"github.com/ginger-core/errors"
	"github.com/ginger-core/log/logger"
)

func (c *c) beginHandleMessage() {
	for {
		_msg, ok := <-c._msgChan
		if !ok {
			return
		}
		if c.msgChan != nil {
			c.msgChan <- _msg
		}
		if c.msgHandlerFunc != nil {
			if c.config.Debug {
				c.logger.
					With(logger.Field{
						"partition": _msg.GetKafkaMessage().Partition,
						"offset":    _msg.GetKafkaMessage().Offset,
					}).
					WithTrace("beginHandleMessage.call.before").
					Debugf("handing message from queue")
			}
			for {
				err := c.msgHandlerFunc(_msg)
				if err != nil {
					if c.config.Debug {
						if err != nil {
							c.logger.
								With(logger.Field{
									"partition": _msg.GetKafkaMessage().Partition,
									"offset":    _msg.GetKafkaMessage().Offset,
									"error":     err,
								}).
								WithTrace("beginHandleMessage.call.after.error").
								Errorf("handled message from queue")
						}
					}
					continue
				}
				if c.config.Debug {
					c.logger.
						With(logger.Field{
							"partition": _msg.GetKafkaMessage().Partition,
							"offset":    _msg.GetKafkaMessage().Offset,
						}).
						WithTrace("beginHandleMessage.call.after").
						Debugf("handled message from queue")
				}
				for {
					ctx, cancel := context.WithTimeout(
						context.Background(), c.config.Timeout)
					if err := c.CommitMessage(ctx, _msg); err != nil {
						c.logger.
							With(logger.Field{
								"error": err.Error(),
							}).
							WithTrace("CommitMessage.err").
							Criticalf("error while committing message")
						cancel()
						if err.IsType(errors.TypeNotFound) {
							break
						}
						time.Sleep(time.Second)
						continue
					}
					cancel()
					break
				}
				break
			}
		}
	}
}
