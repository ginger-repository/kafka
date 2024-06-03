package consumer

import (
	"github.com/ginger-core/errors"
)

func (c *c) Reconnect() errors.Error {
	if !c.started {
		return isNotStartedError.Clone()
	}
	if c.config.Debug {
		c.logger.
			WithTrace("Reconnect").
			Debugf("reconnting...")
	}
	err := c.close()
	if err != nil {
		return err.WithTrace("close")
	}
	c.assignHandlers()
	return nil
}
