package consumer

import (
	"context"

	"github.com/ginger-core/errors"
	"github.com/IBM/sarama"
)

func (h *handler) GetMonitorName() string {
	return "handler-" + h.config.properties.GroupId
}

func (h *handler) CheckHealth(ctx context.Context) errors.Error {
	h.clientMtx.RLock()
	defer h.clientMtx.RUnlock()
	if h.client == nil {
		return errors.New().
			WithTrace("h.client.nil")
	}
	if err := h.checkBrokers(); err != nil {
		return err.WithTrace("checkBrokers")
	}
	return nil
}

func (h *handler) ensureConsumer() errors.Error {
	if err := h.ensureClient(); err != nil {
		return err.WithTrace("ensureClient")
	}
	if err := h.ensureConsumerConn(); err != nil {
		return err.WithTrace("ensureConsumerConn")
	}
	return nil
}

func (h *handler) ensureClient() errors.Error {
	if !h.started {
		return nil
	}

	h.clientMtx.RLock()
	if h.client != nil {
		h.clientMtx.RUnlock()
		return nil
	}
	h.clientMtx.RUnlock()
	//
	h.clientMtx.Lock()
	defer h.clientMtx.Unlock()

	client, err := sarama.NewClient(
		h.config.properties.Brokers,
		h.config.properties.Config,
	)
	if err != nil {
		return errors.New(err).
			WithTrace("sarama.NewClient")
	}
	h.client = client
	return nil
}

func (h *handler) ensureConsumerConn() errors.Error {
	if !h.started {
		return nil
	}

	h.readerMtx.RLock()
	if h.reader != nil {
		h.readerMtx.RUnlock()
		return nil
	}
	h.readerMtx.RUnlock()
	//
	h.readerMtx.Lock()
	defer h.readerMtx.Unlock()

	reader, err := sarama.NewConsumerGroupFromClient(
		h.config.properties.GroupId, h.client)
	if err != nil {
		return errors.New(err).
			WithTrace("sarama.NewConsumerGroupFromClient")
	}
	h.reader = reader
	return nil
}
