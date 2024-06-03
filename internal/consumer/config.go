package consumer

import (
	"log"
	"os"
	"time"

	"github.com/IBM/sarama"
)

type claimConfig struct {
	CheckCycle        time.Duration
	MaximumStuckTimes int
}

func (c *claimConfig) initialize() {
	if c.CheckCycle == 0 {
		c.CheckCycle = time.Second * 5
	}
	if c.MaximumStuckTimes == 0 {
		c.MaximumStuckTimes = 10
	}
}

type handlerConfig struct {
	CommitDelay time.Duration

	Claim claimConfig

	properties *properties
	//
	debug bool
}

func (c *handlerConfig) initialize() {
	if c.CommitDelay == 0 {
		c.CommitDelay = time.Second * 3
	}
	c.Claim.initialize()
}

type properties struct {
	*sarama.Config

	RebalanceStrategy string
	Brokers           []string
	GroupId           string
	GroupTopics       []string
}

func newProperties() *properties {
	r := &properties{
		Config:            sarama.NewConfig(),
		RebalanceStrategy: "sticky",
	}
	// r.Consumer.Group.Session.Timeout = time.Second * 120
	// r.Consumer.Group.Heartbeat.Interval = time.Second * 20
	r.Consumer.Offsets.AutoCommit.Enable = false
	return r
}

func (c *properties) initialize() {
	if c.RebalanceStrategy == "" {
		c.RebalanceStrategy = "roundrobin"
	}
	switch c.RebalanceStrategy {
	case "sticky":
		c.Config.Consumer.Group.Rebalance.GroupStrategies =
			[]sarama.BalanceStrategy{
				sarama.NewBalanceStrategySticky(),
			}
	case "roundrobin":
		c.Config.Consumer.Group.Rebalance.GroupStrategies =
			[]sarama.BalanceStrategy{
				sarama.NewBalanceStrategyRoundRobin(),
			}
	case "range":
		c.Config.Consumer.Group.Rebalance.GroupStrategies =
			[]sarama.BalanceStrategy{
				sarama.NewBalanceStrategyRange(),
			}
	default:
		log.Panicf("Unrecognized consumer group partition assignor: %s", c.RebalanceStrategy)
	}
	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	c.Config.ClientID = hostname
}

type config struct {
	Debug   bool
	Enabled *bool
	enabled bool

	Properties      *properties
	Timeout         time.Duration
	Delay           time.Duration
	CheckDelay      time.Duration
	ConcurrentCount int
	Handler         *handlerConfig
}

func newConfig() config {
	return config{
		Properties: newProperties(),
	}
}

func (c *config) initialize() {
	c.enabled = c.Enabled == nil || *c.Enabled
	c.Properties.initialize()
	if c.ConcurrentCount == 0 {
		c.ConcurrentCount = 1
	}
	if c.Timeout == 0 {
		c.Timeout = time.Second * 10
	}
	if c.CheckDelay == 0 {
		c.CheckDelay = time.Second * 15
	}
	if c.Handler == nil {
		c.Handler = new(handlerConfig)
	}
	c.Handler.properties = c.Properties
	c.Handler.initialize()
	c.Handler.debug = c.Debug
}
