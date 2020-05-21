package main

import (
	"bytes"
	"encoding/gob"
	"flag"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/streadway/amqp"

	"github.com/sajicode/go-dist-app/src/distributed/dto"
	"github.com/sajicode/go-dist-app/src/distributed/qutils"
)

var url = "amqp://adams:adams@localhost:5672"

var name = flag.String("name", "sensor", "name of the sensor")
var freq = flag.Uint("freq", 5, "update frequency in cycles/sec")
var max = flag.Float64("max", 5., "maximum value for generated readings")
var min = flag.Float64("min", 1., "minimum value for generated readings")
var stepSize = flag.Float64("step", 0.1, "maximum allowable change per measurement")

var r = rand.New(rand.NewSource(time.Now().UnixNano()))

// * create random value
var value = r.Float64()*(*max-*min) + *min

// * nominal value of censor
var nom = (*max-*min)/2 + *min

func main() {
	flag.Parse()

	conn, ch := qutils.GetChannel(url)
	defer conn.Close()
	defer ch.Close()

	datatQueue := qutils.GetQueue(*name, ch)

	publishQueueName(ch)

	discoveryQueue := qutils.GetQueue("", ch)
	ch.QueueBind(
		discoveryQueue.Name,            // name string,
		"",                             //key string,
		qutils.SensorDiscoveryExchange, //exchange string,
		false,                          //noWait bool,
		nil,                            //args amqp.Table,
	)

	go listenForDiscoverRequests(discoveryQueue.Name, ch)

	// * milliseconds per cycle == 5 cycles / sec or 200 ms / cycle
	dur, _ := time.ParseDuration(strconv.Itoa(1000/int(*freq)) + "ms")
	// * create a channel that ticks at regular intervals
	signal := time.Tick(dur)

	// * define buffer to hold encoded data
	// * for json, we could use the json encoder
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)

	for range signal {
		calcValue()
		reading := dto.SensorMessage{
			Name:      *name,
			Value:     value,
			Timestamp: time.Now(),
		}

		buf.Reset()
		enc = gob.NewEncoder(buf)
		enc.Encode(reading)

		msg := amqp.Publishing{
			Body: buf.Bytes(),
		}

		ch.Publish(
			"",              //exchange string
			datatQueue.Name, //key string = queue name
			false,           //mandatory bool
			false,           // immediate bool
			msg,             // msg amqp.Publishing
		)

		log.Printf("Reading sent, Value: %v\n", value)
	}

}

func publishQueueName(ch *amqp.Channel) {
	// * sensors should publish the routing key of the queue they would be publishing to
	msg := amqp.Publishing{Body: []byte(*name)}

	// * sensors should publish their message to a fanout exchange
	ch.Publish(
		"amq.fanout", //exchange string
		"",           //key string = queue name
		false,        //mandatory bool
		false,        // immediate bool
		msg,          // msg amqp.Publishing
	)
}

func listenForDiscoverRequests(name string, ch *amqp.Channel) {
	msgs, _ := ch.Consume(
		name,  //queue string,
		"",    //consumer string,
		true,  //autoAck bool,
		false, //exclusive bool,
		false, //noLocal bool,
		false, //noWait bool,
		nil,   //args amqp.Table
	)

	for range msgs {
		publishQueueName(ch)
	}
}

func calcValue() {
	// * maximum increase & decrease amount of value
	var maxStep, minStep float64

	if value < nom {
		maxStep = *stepSize
		minStep = -1 * *stepSize * (value - *min) / (nom - *min)
	} else {
		maxStep = *stepSize * (*max - value) / (*max - nom)
		minStep = -1 * *stepSize
	}

	// * generate random position within range & add to correct value
	value += r.Float64()*(maxStep-minStep) + minStep
}
