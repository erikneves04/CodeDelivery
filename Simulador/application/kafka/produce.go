package kafka

import (
	"encoding/json"
	route "github.com/erikneves04/codedelivery-simulator/application/route"
	"github.com/erikneves04/codedelivery-simulator/infra/kafka"
	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"os"
	"time"
)

func Produce(msg *ckafka.Message) {
	producer := kafka.NewKafkaProducer()
	route := route.NewRoute()

	json.Unmarshal(msg.Value, &route)
	route.LoadPositions()

	positions, err := route.ExportJsonPositions()
	if err != nil {
		log.Println(err.Error())
	}

	for _, p := range positions {
		kafka.Publish(p, os.Getenv("kafkaProduceTopic"), producer)
		time.Sleep(time.Millisecond  * 500)
	}
}