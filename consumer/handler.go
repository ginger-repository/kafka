package consumer

import (
	"github.com/ginger-core/errors"
	"github.com/ginger-repository/kafka/message"
)

type MessageHandlerFunc func(msg message.Message) errors.Error
