package message

import (
	"context"
	"encoding/json"

	"github.com/ginger-core/errors"
	"github.com/IBM/sarama"
)

type Message interface {
	WithContext(ctx context.Context) Message
	GetContext() context.Context

	WithMemberId(memberId string) Message
	GetMemberId() string

	WithKey(key []byte) Message
	GetKey() []byte

	WithTopic(topic Topic) Message
	GetTopic() Topic

	WithBody(v any) (Message, errors.Error)
	WithValue(v []byte) Message
	GetValue() []byte

	WithKafkaMessage(msg *sarama.ConsumerMessage) Message
	GetKafkaMessage() *sarama.ConsumerMessage

	WithHeader(key string, value []byte) Message
	GetHeaders() []*sarama.RecordHeader
	GetHeader(key string) []byte

	WithHandlerIndex(idx int)
	GetHandlerIndex() int
}

type message struct {
	ctx          context.Context
	memberId     string
	key          []byte
	topic        Topic
	value        []byte
	headers      []*sarama.RecordHeader
	kafkaMessage *sarama.ConsumerMessage
	handlerIdx   int
}

func New() Message {
	return &message{
		headers: make([]*sarama.RecordHeader, 0),
	}
}

func (m *message) WithContext(ctx context.Context) Message {
	m.ctx = ctx
	return m
}

func (m *message) GetContext() context.Context {
	return m.ctx
}

func (m *message) WithMemberId(memberId string) Message {
	m.memberId = memberId
	return m
}

func (m *message) GetMemberId() string {
	return m.memberId
}

func (m *message) WithKey(key []byte) Message {
	m.key = key
	return m
}

func (m *message) GetKey() []byte {
	return m.key
}

func (m *message) WithTopic(topic Topic) Message {
	m.topic = topic
	return m
}

func (m *message) GetTopic() Topic {
	if m.topic != nil {
		return m.topic
	}
	if m.kafkaMessage != nil {
		m.topic = NewEmptyTopic(m.kafkaMessage.Topic)
	}
	return m.topic
}

func (m *message) WithBody(v any) (Message, errors.Error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, errors.New(err).WithTrace("message.WithBody")
	}
	return m.WithValue(data), nil
}

func (m *message) WithValue(v []byte) Message {
	m.value = v
	return m
}

func (m *message) GetValue() []byte {
	if m.kafkaMessage != nil {
		return m.kafkaMessage.Value
	}
	return m.value
}

func (m *message) WithKafkaMessage(msg *sarama.ConsumerMessage) Message {
	m.kafkaMessage = msg
	m.headers = msg.Headers
	return m
}

func (m *message) GetKafkaMessage() *sarama.ConsumerMessage {
	if m.kafkaMessage == nil {
		m.kafkaMessage = &sarama.ConsumerMessage{
			Key:     m.key,
			Value:   m.value,
			Headers: m.headers,
		}
		if m.topic != nil {
			m.kafkaMessage.Topic = m.topic.GetName()
		}
	}
	return m.kafkaMessage
}

func (m *message) WithHeader(key string, value []byte) Message {
	m.headers = append(m.headers,
		&sarama.RecordHeader{Key: []byte(key), Value: value})
	return m
}

func (m *message) GetHeaders() []*sarama.RecordHeader {
	return m.headers
}

func (m *message) GetHeader(key string) []byte {
	for _, h := range m.headers {
		if string(h.Key) == key {
			return h.Value
		}
	}
	return nil
}

func (m *message) WithHandlerIndex(idx int) {
	m.handlerIdx = idx
}

func (m *message) GetHandlerIndex() int {
	return m.handlerIdx
}
