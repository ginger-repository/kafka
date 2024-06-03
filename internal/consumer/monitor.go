package consumer

import (
	"context"

	"github.com/ginger-core/errors"
	"github.com/ginger-core/log/logger"
)

func (c *c) GetMonitorName() string {
	return "consumer-" + c.config.Properties.GroupId
}

func (c *c) CheckHealth(ctx context.Context) errors.Error {
	for _, h := range c.handlers {
		if err := h.CheckHealth(ctx); err != nil {
			rErr := c.Reconnect()
			if rErr != nil {
				c.logger.
					With(logger.Field{
						"error": rErr.Error(),
					}).
					WithTrace("Reconnect").
					Errorf("error on reconnect")
			}
			return err.
				WithTrace("handlers.CheckHealth")
		}
	}
	return nil
}
