package consumer

import (
	"time"

	"github.com/ginger-core/log/logger"
)

type handlerStats struct {
	highOffset    int64
	currentOffset int64

	lastCheckedOffset int64
}

func (p *partition) beginCheckClaim() {
	p.startMtx.RLock()
	defer p.startMtx.RUnlock()

	t := time.NewTicker(p.handler.config.Claim.CheckCycle)
	defer func() {
		t.Stop()
	}()

	stuckTimes := 0
	var high int64
	var partition int32
	for p.started {
		select {
		case <-p.startChan:
			return
		case <-t.C:
			high = p.claim.HighWaterMarkOffset()
			if p.stats.currentOffset == 0 {
				p.stats.currentOffset = p.claim.InitialOffset()
			}
			partition = p.claim.Partition()
			p.stats.highOffset = high
			if p.stats.currentOffset < p.stats.highOffset-1 {
				if p.handler.config.debug {
					p.logger.
						With(logger.Field{
							"high":       p.stats.highOffset,
							"current":    p.stats.currentOffset,
							"partition":  partition,
							"stuckTimes": stuckTimes,
						}).
						WithTrace("behind").
						Warnf("partition is behind")
				}
				if p.stats.lastCheckedOffset == p.stats.currentOffset {
					// stuck
					if p.handler.config.debug {
						p.logger.
							With(logger.Field{
								"high":       p.stats.highOffset,
								"current":    p.stats.currentOffset,
								"partition":  partition,
								"stuckTimes": stuckTimes,
							}).
							WithTrace("stuck==").
							Warnf("partition is stuck")
					}
					stuckTimes++
				} else {
					stuckTimes = 0
					p.stats.lastCheckedOffset = p.stats.currentOffset
				}
				if stuckTimes > p.handler.config.Claim.MaximumStuckTimes {
					p.logger.
						With(logger.Field{
							"high":      p.stats.highOffset,
							"current":   p.stats.currentOffset,
							"partition": partition,
						}).
						WithTrace("stuck").
						Warnf("handler closed since consumer stuck")
					p.stop()
					p.start()
					return
				}
			}
		}
	}
}
