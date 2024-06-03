package producer

import (
	"github.com/IBM/sarama"
)

type config struct {
	Enabled *bool
	enabled bool

	Brokers []string
	Topic   string

	Properties *sarama.Config
}

func (c *config) initialize() {
	c.enabled = c.Enabled == nil || *c.Enabled
	// c.Properties.Producer.Transaction.ID = os.Getenv("HOSTNAME")
}
