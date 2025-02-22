package main

const (
	KAFKASERVER = string("localhost:19092")
)

func main() {
	produce()
	produceWithCallback()

}
