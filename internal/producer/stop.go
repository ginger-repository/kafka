package producer

import "github.com/ginger-core/errors"

func (p *p) Close() errors.Error {
	p.producerMtx.Lock()
	defer p.producerMtx.Unlock()

	if err := p.writer.close(); err != nil {
		return err.WithTrace("writer.close")
	}
	return nil
}
