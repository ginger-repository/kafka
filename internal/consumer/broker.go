package consumer

import (
	"errors"

	errs "github.com/ginger-core/errors"
	"github.com/IBM/sarama"
)

func (h *handler) checkBrokers() (err errs.Error) {
	h.clientMtx.RLock()
	defer h.clientMtx.RUnlock()
	if h.client == nil {
		return nil
	}

	brokers := h.client.Brokers()
	if len(h.client.Brokers()) == 0 {
		return errs.New().
			WithTrace("client.Brokers.len=0")
	}
	for _, broker := range brokers {
		oErr := broker.Open(h.client.Config())
		if oErr != nil && !errors.Is(oErr, sarama.ErrAlreadyConnected) {
			return errs.New(oErr).
				WithTrace("broker.Ope")
		}
	}
	return
}
