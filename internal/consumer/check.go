package consumer

import (
	"context"
	"time"

	"github.com/ginger-core/log/logger"
)

func (c *c) beginCheckBrokers() {
	ctx := context.Background()
	for c.started {
		time.Sleep(c.config.CheckDelay)
		for _, h := range c.handlers {
			if err := h.CheckHealth(ctx); err != nil {
				c.logger.
					With(logger.Field{
						"error": err.Error(),
					}).
					WithTrace("h.HealthCheck").
					Errorf("check brokers failed")
				err = c.Reconnect()
				if err != nil {
					c.logger.
						With(logger.Field{
							"error": err.Error(),
						}).
						WithTrace("Reconnect").
						Errorf("reconnect failed")
				}
			}
		}
	}
}
