package consumer

import (
	"github.com/ginger-core/errors"
)

func (c *c) Close() errors.Error {
	if !c.started {
		return nil
	}
	if err := c.close(); err != nil {
		return err.WithTrace("close")
	}
	close(c.closeCh)
	return nil
}

func (c *c) close() errors.Error {
	for _, handler := range c.handlers {
		if err := handler.Close(); err != nil {
			return errors.New(err).
				WithTrace("handler.Close")
		}
	}
	return nil
}
