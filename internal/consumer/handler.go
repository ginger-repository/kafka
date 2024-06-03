package consumer

import (
	"context"
	"sync"

	"github.com/IBM/sarama"
	"github.com/ginger-core/errors"
	"github.com/ginger-core/log/logger"
	"github.com/ginger-repository/kafka/message"
	"github.com/ginger-repository/kafka/monitor"
)

func (c *c) assignHandlers() {
	c.handlers = make([]groupHandler, c.config.ConcurrentCount)
	for i := 0; i < c.config.ConcurrentCount; i++ {
		c.handlers[i] = newHandler(
			c.logger.WithTrace("handler"),
			c.config.Handler,
			c._msgChan,
		)
		go c.handlers[i].Start()
	}
}

type groupHandler interface {
	monitor.Monitor
	sarama.ConsumerGroupHandler

	Start()
	Stop()
	Close() errors.Error
	Consume(ctx context.Context, topics []string) errors.Error
	MarkMessage(ctx context.Context, msg message.Message) errors.Error

	CheckHealth(ctx context.Context) errors.Error
}

type handler struct {
	logger logger.Logger

	config *handlerConfig

	client    sarama.Client
	clientMtx *sync.RWMutex
	reader    sarama.ConsumerGroup
	readerMtx *sync.RWMutex

	spMap map[string]*partitions
	spMtx *sync.RWMutex

	msgChan chan<- message.Message

	started   bool
	startChan chan bool
	startMtx  *sync.RWMutex

	cancel context.CancelFunc
}

func newHandler(logger logger.Logger, config *handlerConfig,
	msgChan chan<- message.Message) groupHandler {
	h := &handler{
		logger:    logger,
		config:    config,
		clientMtx: new(sync.RWMutex),
		readerMtx: new(sync.RWMutex),
		spMap:     make(map[string]*partitions),
		spMtx:     new(sync.RWMutex),
		msgChan:   msgChan,
		startMtx:  new(sync.RWMutex),
	}
	return h
}

func (h *handler) Setup(s sarama.ConsumerGroupSession) error {
	h.spMtx.Lock()
	defer h.spMtx.Unlock()
	h.spMap[s.MemberID()] = newPartitions(h, s)
	return nil
}

func (h *handler) Cleanup(s sarama.ConsumerGroupSession) error {
	h.spMtx.Lock()
	defer h.spMtx.Unlock()

	sp := h.spMap[s.MemberID()]
	if sp == nil {
		return nil
	}
	sp.closeAll()
	delete(h.spMap, s.MemberID())
	return nil
}

func (h *handler) Start() {
	h.startChan = make(chan bool)
	h.started = true
	go h.start()
}

func (h *handler) start() {
	h.startMtx.RLock()

	defer func() {
		// if h.msgChan != nil {
		// 	close(h.msgChan)
		// }
		h.startMtx.RUnlock()

		h.logger.
			WithTrace("start.quitCh").
			Debugf("stopped")
	}()

	if h.reader != nil {
		h.reader.ResumeAll()
	}

	for h.started {
		ctx, cancel := context.WithCancel(context.Background())
		h.cancel = cancel
		err := h.Consume(ctx, h.config.properties.GroupTopics)
		if err != nil {
			h.logger.
				With(logger.Field{
					"error": err.Error(),
				}).
				WithTrace("Consume").
				Errorf("error on consume")
		}
	}
}

func (h *handler) Stop() {
	if !h.started {
		return
	}
	h.reader.PauseAll()
	h.started = false
	close(h.startChan)
	h.cancel()
	h.startMtx.Lock()
	defer h.startMtx.Unlock()
}

func (h *handler) Close() errors.Error {
	h.spMtx.Lock()
	for _, sp := range h.spMap {
		err := sp.closeAll()
		if err != nil {
			return err.
				WithTrace("sp.closeAll")
		}
	}
	h.spMtx.Unlock()

	h.readerMtx.Lock()
	defer h.readerMtx.Unlock()

	if h.reader != nil {
		if err := h.reader.Close(); err != nil {
			return errors.New(err).
				WithTrace("reader.Close")
		}
		h.reader = nil
	}

	h.clientMtx.Lock()
	defer h.clientMtx.Unlock()
	if h.client != nil {
		if err := h.client.Close(); err != nil {
			return errors.New(err).
				WithTrace("client.Close")
		}
		h.client = nil
	}
	return nil
}
