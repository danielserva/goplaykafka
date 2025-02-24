package main

import (
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func produce() {
	fmt.Println("I am a kafka producer written in go!")

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

	KAFKATOPIC := "demo_kafka"
	data := "hello world from go"
	p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &KAFKATOPIC, Partition: kafka.PartitionAny},
		Key:            nil,
		Value:          []byte(data),
	}, nil)

	// Wait for all messages to be delivered
	p.Flush(15 * 100)
	p.Close()

}
