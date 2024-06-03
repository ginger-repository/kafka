package consumer

import (
	"sync"

	"github.com/ginger-core/errors"
)

func (c *c) Stop() errors.Error {
	if !c.started {
		if !c.config.enabled {
			return isNotEnabledError.Clone()
		}
		return errors.Validation().
			WithTrace("Stop.!started").
			WithMessage("not started")
	}

	close(c.quitCh)

	c.started = false

	c.logger.
		WithTrace("handlers.stop.before").
		Debugf("stopping handlers...")
	wg := new(sync.WaitGroup)
	for _, h := range c.handlers {
		wg.Add(1)
		go func(h groupHandler) {
			h.Stop()
			wg.Done()
		}(h)
	}
	wg.Wait()
	c.logger.
		WithTrace("handlers.stop.after").
		Debugf("handlers stopped")

	c.startMtx.Lock()
	defer c.startMtx.Unlock()
	return nil
}
