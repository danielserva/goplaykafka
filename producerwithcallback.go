package main

import (
	"fmt"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func produceWithCallback() {
	fmt.Println("I am a kafka producer written in go with a callback!")

	// connects with localhost
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": KAFKASERVER,
		"acks":              "all",
	})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	// Go-routine to handle message delivery reports and
	// possibly other event types (errors, stats, etc)
	// This is equivalent to Callback in the java kafka library
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Failed to deliver message: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Produced event to \n topic: %s \n key = %-10s \n value = %s\n partition = %d \n offset = %s \n TimeStamp = %v\n",
						*ev.TopicPartition.Topic,
						string(ev.Key),
						string(ev.Value),
						ev.TopicPartition.Partition,
						ev.TopicPartition.Offset.String(),
						ev.Timestamp.Format(time.RFC822Z))
				}
			}
		}
	}()

	KAFKATOPIC := "demo_kafka"
	data := "value with callback"
	p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &KAFKATOPIC, Partition: kafka.PartitionAny},
		Key:            nil,
		Value:          []byte(data),
	}, nil)

	// users := [...]string{"daniel", "britt", "roy", "mats", "mama", "papa"}
	// items := [...]string{"book", "alarm clock", "t-shirts", "gift card", "batteries"}
	// KAFKATOPIC := "purchases"
	// for n := 0; n < 10; n++ {
	// 	key := users[rand.Intn(len(users))]
	// 	data := items[rand.Intn(len(items))]
	// 	p.Produce(&kafka.Message{
	// 		TopicPartition: kafka.TopicPartition{Topic: &KAFKATOPIC, Partition: kafka.PartitionAny},
	// 		Key:            []byte(key),
	// 		Value:          []byte(data),
	// 	}, nil)
	// }

	// Wait for all messages to be delivered
	p.Flush(15 * 1000)
	p.Close()

}
