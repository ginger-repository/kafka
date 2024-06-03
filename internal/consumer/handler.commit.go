package consumer

import (
	"context"

	"github.com/ginger-core/errors"
	"github.com/ginger-repository/kafka/message"
)

func (h *handler) MarkMessage(ctx context.Context,
	msg message.Message) errors.Error {
	h.spMtx.RLock()
	sp := h.spMap[msg.GetMemberId()]
	h.spMtx.RUnlock()

	if sp == nil {
		return errors.NotFound().
			WithTrace("h.spMap[msg.GetMemberId()]=nil")
	}

	return sp.MarkMessage(ctx, msg)
}
