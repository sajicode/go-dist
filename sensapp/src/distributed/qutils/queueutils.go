package qutils

import (
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

// SensorDiscoveryExchange is the exchange for all sensors
const SensorDiscoveryExchange = "SensorDiscovery"

// GetChannel returns a channel & connection from a rabbitmq instance
func GetChannel(url string) (*amqp.Connection, *amqp.Channel) {
	conn, err := amqp.Dial(url)
	failOnError(err, "Failed to establish a connection to the message broker")
	ch, err := conn.Channel()
	failOnError(err, "Failed to get channel for connection")

	return conn, ch
}

// GetQueue function returns a queue from a rabbitmq channel
func GetQueue(name string, ch *amqp.Channel) *amqp.Queue {
	q, err := ch.QueueDeclare(
		name, //name,
		false, //durable bool,
		false, //autoDelete bool,
		false, //exclusive bool,
		false, //noWait bool,
		nil,   //args amqp.Table,
	)
	failOnError(err, "Failed to declare a queue")

	return &q
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}
