package main

import (
	"fmt"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func produce() {
	fmt.Println("I am a kafka producer!")

	// connects with localhost
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": KAFKASERVER,
		"acks":              "all",
	})

	// connects with remote kafka server
	// p, err := kafka.NewProducer(&kafka.ConfigMap{
	// 	//User specific properties
	// 	"bootstrap.servers": "<KAFKASERVER>",
	// 		"sasl.username":     "<CLUSTER API KEY>",
	// 		"sasl.password":     "<CLUSTER API SECRET>",

	// 	// Fixed properties
	// 		"security.protocol": "SASL_SSL",
	// 		"sasl.mechanisms":   "PLAIN",
	// 	"acks": "all",
	// })

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	// Go-routine to handle message delivery reports and
	// possibly other event types (errors, stats, etc)
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Failed to deliver message: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Produced event to topic %s: key = %-10s value = %s\n",
						*ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))
					fmt.Printf("partition = %d offset = %s TimeStamp = %v\n",
						ev.TopicPartition.Partition, ev.TopicPartition.Offset.String(), ev.Timestamp.Format(time.RFC822Z))
				}
			}
		}
	}()

	KAFKATOPIC := "demo_kafka"
	data := "hello world from go"
	p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &KAFKATOPIC, Partition: kafka.PartitionAny},
		Key:            nil,
		Value:          []byte(data),
	}, nil)

	// Wait for all messages to be delivered
	p.Flush(15 * 1000)
	p.Close()

}
