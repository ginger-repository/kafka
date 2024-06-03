package consumer

import (
	"context"
	"sync"

	"github.com/IBM/sarama"
	"github.com/ginger-core/errors"
	"github.com/ginger-core/log/logger"
	"github.com/ginger-repository/kafka/message"
)

type partitions struct {
	handler *handler
	session sarama.ConsumerGroupSession
	pMap    map[int32]*partition // map[partition]partition
	pMtx    *sync.RWMutex
}

func newPartitions(handler *handler,
	session sarama.ConsumerGroupSession) *partitions {
	return &partitions{
		handler: handler,
		session: session,
		pMap:    make(map[int32]*partition),
		pMtx:    new(sync.RWMutex),
	}
}

func (p *partitions) startPartition(s sarama.ConsumerGroupSession,
	c sarama.ConsumerGroupClaim) errors.Error {
	itm := newPartition(
		p.handler.logger.With(logger.Field{
			"partition": c.Partition(),
		}),
		p.handler, s, c,
	)
	//
	p.pMtx.Lock()
	p.pMap[c.Partition()] = itm
	p.pMtx.Unlock()
	itm.start()
	return nil
}

func (p *partitions) closeAll() errors.Error {
	p.pMtx.Lock()
	defer p.pMtx.Unlock()
	wg := new(sync.WaitGroup)
	for _, itm := range p.pMap {
		wg.Add(1)
		go func(itm *partition) {
			itm.stop()
			wg.Done()
		}(itm)
	}
	wg.Wait()
	for _, itm := range p.pMap {
		p.disposePartition(itm)
	}
	return nil
}

func (p *partitions) disposePartition(part *partition) errors.Error {
	part.stop()
	delete(p.pMap, part.claim.Partition())
	return nil
}

func (p *partitions) MarkMessage(ctx context.Context,
	msg message.Message) errors.Error {
	p.pMtx.RLock()
	partition := p.pMap[msg.GetKafkaMessage().Partition]
	p.pMtx.RUnlock()
	if partition == nil {
		return nil
	}
	err := partition.MarkMessage(ctx, msg)
	if err != nil {
		return errors.New(err).
			WithTrace("partition.MarkMessage")
	}
	return nil
}
