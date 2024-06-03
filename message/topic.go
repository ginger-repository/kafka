package message

import (
	"github.com/IBM/sarama"
)

type Topic interface {
	GetKafkaTopic() sarama.TopicMetadata

	WithName(name string) Topic
	GetName() string
}

type topic struct {
	Topic sarama.TopicMetadata
}

func NewTopic(t sarama.TopicMetadata) Topic {
	return &topic{Topic: t}
}

func NewEmptyTopic(name string) Topic {
	return new(topic).
		WithName(name)
}

func (t *topic) GetKafkaTopic() sarama.TopicMetadata {
	return t.Topic
}

func (t *topic) WithName(name string) Topic {
	t.Topic.Name = name
	return t
}

func (t *topic) GetName() string {
	return t.Topic.Name
}
