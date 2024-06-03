package consumer

import (
	"context"

	"github.com/ginger-core/errors"
	"github.com/ginger-repository/kafka/message"
)

func (c *c) CommitMessage(ctx context.Context,
	msg message.Message) errors.Error {
	err := c.handlers[msg.GetHandlerIndex()].MarkMessage(ctx, msg)
	if err != nil {
		_err := errors.New(err).
			WithTrace("handler.MarkMessage")
		return _err
	}

	return nil
}
