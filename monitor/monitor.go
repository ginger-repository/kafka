package monitor

import (
	"context"

	"github.com/ginger-core/errors"
)

type Monitor interface {
	GetMonitorName() string
	CheckHealth(ctx context.Context) errors.Error
}
