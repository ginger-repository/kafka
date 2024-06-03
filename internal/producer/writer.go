package producer

import (
	"context"

	"github.com/ginger-core/errors"
	"github.com/IBM/sarama"
)

type writer interface {
	CheckHealth(ctx context.Context) errors.Error
	write(ctx context.Context, msg *sarama.ProducerMessage) errors.Error
	close() errors.Error
}

type baseWriter struct {
	config  *sarama.Config
	brokers []string

	client  sarama.Client
	started bool
}

func (w *baseWriter) initialize() {
	client, err := sarama.NewClient(
		w.brokers,
		w.config,
	)
	if err != nil {
		panic(err)
	}
	w.client = client
}

func (w *baseWriter) CheckHealth(ctx context.Context) errors.Error {
	if w.client == nil {
		return errors.New().
			WithTrace("client.nil")
	}
	if len(w.client.Brokers()) == 0 {
		return errors.New().
			WithTrace("client.Brokers.len0")
	}
	return nil
}

type syncWriter struct {
	baseWriter
	sarama.SyncProducer
}

func newSyncWriter(brokers []string, config *sarama.Config) writer {
	w := &syncWriter{
		baseWriter: baseWriter{
			brokers: brokers,
			config:  config,
		},
	}
	w.initialize()
	return w
}

func (w *syncWriter) initialize() {
	w.baseWriter.initialize()
	producer, err := sarama.NewSyncProducerFromClient(w.client)
	if err != nil {
		panic(err)
	}
	w.SyncProducer = producer
}

func (w *syncWriter) write(ctx context.Context,
	msg *sarama.ProducerMessage) errors.Error {
	_, _, err := w.SendMessage(msg)
	if err != nil {
		return errors.New(err).
			WithTrace("SyncProducer.SendMessage")
	}
	return nil
}

func (w *syncWriter) close() errors.Error {
	err := w.SyncProducer.Close()
	if err != nil {
		errors.New(err).
			WithTrace("SyncProducer.Close")
	}
	return nil
}

type asyncWriter struct {
	baseWriter
	sarama.AsyncProducer
}

func newAsyncWriter(brokers []string, config *sarama.Config) writer {
	w := &asyncWriter{
		baseWriter: baseWriter{
			brokers: brokers,
			config:  config,
		},
	}
	w.initialize()
	return w
}

func (w *asyncWriter) initialize() {
	w.baseWriter.initialize()
	producer, err := sarama.NewAsyncProducerFromClient(w.client)
	if err != nil {
		panic(err)
	}
	w.AsyncProducer = producer
	w.started = true
	go w.listenSuccess()
}

func (w *asyncWriter) listenSuccess() {
	for w.started {
		<-w.Successes()
	}
}

func (w *asyncWriter) write(ctx context.Context,
	msg *sarama.ProducerMessage) errors.Error {
	w.Input() <- msg
	return nil
}

func (w *asyncWriter) close() errors.Error {
	w.started = false
	err := w.AsyncProducer.Close()
	if err != nil {
		return errors.New(err).
			WithTrace("AsyncProducer.Close")
	}
	return nil
}
