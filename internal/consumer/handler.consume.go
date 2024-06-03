package consumer

import (
	"context"

	"github.com/ginger-core/errors"
	"github.com/IBM/sarama"
)

func (h *handler) ConsumeClaim(
	s sarama.ConsumerGroupSession, c sarama.ConsumerGroupClaim) error {
	h.spMtx.RLock()
	sp := h.spMap[s.MemberID()]
	defer h.spMtx.RUnlock()
	if sp == nil {
		return errors.NotFound().
			WithDesc("memberId: " + s.MemberID()).
			WithTrace("spMap[s.MemberID()]=nil")
	}
	return sp.startPartition(s, c)
}

func (h *handler) Consume(ctx context.Context, topics []string) errors.Error {
	if err := h.ensureConsumer(); err != nil {
		return errors.New(err).
			WithTrace("ensureConsumer")
	}

	err := h.reader.Consume(ctx, topics, h)
	if err != nil {
		return errors.New(err).
			WithTrace("reader.Consume")
	}
	return nil
}
