package consumer

import (
	"context"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/ginger-core/errors"
	"github.com/ginger-core/log/logger"
	"github.com/ginger-repository/kafka/message"
)

type partition struct {
	logger logger.Logger

	handler *handler
	session sarama.ConsumerGroupSession
	claim   sarama.ConsumerGroupClaim

	isDirty bool // is marked any message and waiting to commit

	stats handlerStats

	started   bool
	startChan chan bool
	startMtx  *sync.RWMutex
	stopMtx   *sync.Mutex
}

func newPartition(logger logger.Logger, handler *handler,
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim) *partition {
	return &partition{
		logger:   logger,
		handler:  handler,
		session:  session,
		claim:    claim,
		startMtx: new(sync.RWMutex),
		stopMtx:  new(sync.Mutex),
	}
}

func (p *partition) start() error {
	p.startMtx.RLock()
	if p.started {
		p.stop()
	}
	p.started = true
	p.startChan = make(chan bool)
	p.logger.
		With(logger.Field{
			"generationId": p.session.GenerationID(),
			"memberId":     p.session.MemberID(),
		}).
		WithTrace("ConsumeClaim").
		Debugf("consume claim")
	p.startMtx.RUnlock()

	go p.beginCheckClaim()
	go p.beginCommit()
	p.beginRead()
	return nil
}

func (p *partition) stop() {
	p.stopMtx.Lock()
	defer p.stopMtx.Unlock()

	if !p.started {
		return
	}
	p.commit()
	p.started = false
	close(p.startChan)

	p.startMtx.Lock()
	defer p.startMtx.Unlock()
}

func (p *partition) beginRead() {
	p.startMtx.RLock()
	defer p.startMtx.RUnlock()

	for p.started {
		select {
		case <-p.startChan:
			return
		case <-p.session.Context().Done():
			return
		case msg, ok := <-p.claim.Messages():
			if !ok {
				return
			}
			p.stats.currentOffset = msg.Offset
			p.handler.msgChan <- message.New().
				WithMemberId(p.session.MemberID()).
				WithKafkaMessage(msg)
		}
	}
}

func (p *partition) MarkMessage(ctx context.Context,
	msg message.Message) errors.Error {
	p.session.MarkMessage(msg.GetKafkaMessage(), "")
	p.isDirty = true
	return nil
}

func (p *partition) commit() {
	if !p.isDirty {
		return
	}
	p.session.Commit()
	p.isDirty = false
}

func (p *partition) beginCommit() {
	p.startMtx.RLock()
	defer p.startMtx.RUnlock()

	t := time.NewTicker(p.handler.config.CommitDelay)
	defer func() {
		t.Stop()
	}()

	for p.started {
		select {
		case <-p.handler.startChan:
			return
		case <-t.C:
			p.commit()
		}
	}
}
