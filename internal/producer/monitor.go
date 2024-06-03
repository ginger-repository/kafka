package producer

import (
	"context"

	"github.com/ginger-core/errors"
)

func (p *p) GetMonitorName() string {
	return "producer-" + p.config.Topic
}

func (p *p) CheckHealth(ctx context.Context) errors.Error {
	return p.writer.CheckHealth(ctx)
}
