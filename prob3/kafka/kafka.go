package kafka

import (
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

var config *sarama.Config

func init() {
	config = sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Producer.Return.Successes = true
}

type producer struct {
	prod sarama.SyncProducer
}

type consumer struct {
	cons sarama.Consumer
}

// producer
func NewProducer() producer {
	var p producer
	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalln("Failed to create producer:", err)
	}
	p.prod = producer
	return p
}
func (p producer) Close() {
	p.prod.Close()
}

func (p producer) SendMessage(topic, message string) {

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	partition, offset, err := p.prod.SendMessage(msg)
	if err != nil {
		log.Fatalln("Failed to send message:", err)
	}

	fmt.Printf("Message sent to partition %d at offset %d\n", partition, offset)
}

// consumer
func NewConsumer() consumer {
	var c consumer
	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalln("Failed to create consumer:", err)
	}
	c.cons = consumer
	return c
}
func (c consumer) Close() {
	c.cons.Close()
}

func (c consumer) Listen(topic string) <-chan *sarama.ConsumerMessage {
	partitionConsumer, err := c.cons.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("Failed to start consumer for partition 0: %v", err)
	}

	// Create a channel to return messages from the partition consumer
	messages := make(chan *sarama.ConsumerMessage)
	go func() {
		defer partitionConsumer.Close()
		for msg := range partitionConsumer.Messages() {
			messages <- msg
		}
		close(messages)
	}()

	return messages


}

func (c consumer) ListenOnce(topic string) (*sarama.ConsumerMessage, error) {
	partitionConsumer, err := c.cons.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		return nil, fmt.Errorf("failed to start consumer for partition 0: %w", err)
	}
	defer partitionConsumer.Close()

	// Create a channel to receive the message
	msgChan := make(chan *sarama.ConsumerMessage)

	// Use a goroutine to consume the first message
	go func() {
		defer close(msgChan)
		select {
		case msg := <-partitionConsumer.Messages():
			msgChan <- msg
		case err := <-partitionConsumer.Errors():
			log.Printf("Error while consuming messages: %v", err)
		}
	}()

	// Wait for the message or an error and return
	msg, ok := <-msgChan
	if !ok {
		return nil, fmt.Errorf("message channel closed unexpectedly")
	}
	return msg, nil
}

