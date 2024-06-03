package consumer

import (
	"context"

	"github.com/ginger-core/errors"
	"github.com/ginger-repository/kafka/message"
	"github.com/ginger-repository/kafka/monitor"
)

type Consumer interface {
	monitor.Monitor

	WithChannel(channel chan<- message.Message) Consumer
	WithMessageHandler(f MessageHandlerFunc) Consumer

	Start() errors.Error
	Stop() errors.Error
	Close() errors.Error

	CommitMessage(ctx context.Context, msg message.Message) errors.Error
}
