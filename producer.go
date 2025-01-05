package main

import (
	"fmt"
	"math/rand"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	KAFKASERVER = "localhost:19092"
	KAFKATOPIC  = "purchases"
)

func main() {
	fmt.Println("I am a kafka producer!")
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		//User specific properties
		"bootstrap.servers": KAFKASERVER,
		//		"sasl.username":     "<CLUSTER API KEY>",
		//		"sasl.password":     "<CLUSTER API SECRET>",

		// Fixed properties
		//		"security.protocol": "SASL_SSL",
		//		"sasl.mechanisms":   "PLAIN",
		"acks": "all",
	})

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
				}
			}
		}
	}()

	users := [...]string{"daniel", "britt", "roy", "mats", "mama", "papa"}
	items := [...]string{"book", "alarm clock", "t-shirts", "gift card", "batteries"}

	for n := 0; n < 10; n++ {
		key := users[rand.Intn(len(users))]
		data := items[rand.Intn(len(items))]
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &KAFKATOPIC, Partition: kafka.PartitionAny},
			Key:            []byte(key),
			Value:          []byte(data),
		}, nil)
	}

	// Wait for all messages to be delivered
	p.Flush(15 * 1000)
	p.Close()

}
