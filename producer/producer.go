package producer

import (
	"context"

	"github.com/ginger-core/errors"
	"github.com/ginger-repository/kafka/message"
	"github.com/ginger-repository/kafka/monitor"
)

type Producer interface {
	monitor.Monitor

	Produce(ctx context.Context, msg message.Message) errors.Error
	Close() errors.Error
}
