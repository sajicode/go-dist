package coordinator

import (
	"bytes"
	"encoding/gob"
	"fmt"

	"github.com/streadway/amqp"

	"github.com/sajicode/go-dist-app/src/distributed/dto"
	"github.com/sajicode/go-dist-app/src/distributed/qutils"
)

const url = "amqp://adams:adams@localhost:5672"

type QueueListener struct {
	conn    *amqp.Connection
	ch      *amqp.Channel
	sources map[string]<-chan amqp.Delivery
	ea      *EventAggregator
}

func NewQueueListener() *QueueListener {
	ql := QueueListener{
		sources: make(map[string]<-chan amqp.Delivery),
		ea:      NewEventAggregator(),
	}

	ql.conn, ql.ch = qutils.GetChannel(url)

	return &ql
}

func (ql *QueueListener) DiscoverSensors() {
	ql.ch.ExchangeDeclare(
		qutils.SensorDiscoveryExchange, //name string
		"fanout",                       //kind string,
		false,                          //durable bool,
		false,                          //autoDelete bool,
		false,                          //internal bool,
		false,                          //noWait bool,
		nil,                            //args amqp.Table,
	)

	ql.ch.Publish(
		qutils.SensorDiscoveryExchange, //exchange string,
		"",                             //key string,
		false,                          //mandatory bool,
		false,                          // immediate bool,
		amqp.Publishing{},              //msg amqp.Publishing,
	)
}

// ListenForNewSource is a func that allows the queue to listen for new sensors
func (ql *QueueListener) ListenForNewSource() {
	q := qutils.GetQueue("", ql.ch)

	ql.ch.QueueBind(
		q.Name,       // name string
		"",           // key string - not needed for fanout
		"amq.fanout", //exchange string,
		false,        // noWait bool,
		nil,          //args amqp.Table
	)

	msgs, _ := ql.ch.Consume(
		q.Name, //queue string,
		"",     //consumer string,
		true,   //autoAck bool,
		false,  //exclusive bool,
		false,  //noLocal bool,
		false,  //noWait bool,
		nil,    //args amqp.Table
	)

	ql.DiscoverSensors()

	fmt.Println("listening for new sources")
	for msg := range msgs {
		// * get access to sensor's queue
		fmt.Println("new source discovered")
		sourceChan, _ := ql.ch.Consume(
			string(msg.Body), //queue string,
			"",               //consumer string,
			true,             //autoAck bool,
			false,            //exclusive bool,
			false,            //noLocal bool,
			false,            //noWait bool,
			nil,              //args amqp.Table
		)

		// * check if new message has been registered, if no, store source channel in the map
		if ql.sources[string(msg.Body)] == nil {
			ql.sources[string(msg.Body)] = sourceChan

			go ql.AddListener(sourceChan)
		}
	}
}

// AddListener listens for new queues
func (ql *QueueListener) AddListener(msgs <-chan amqp.Delivery) {
	for msg := range msgs {
		// * decode message
		r := bytes.NewReader(msg.Body)
		d := gob.NewDecoder(r)
		sd := new(dto.SensorMessage)
		d.Decode(sd)

		fmt.Printf("Received message: %v\n", sd)

		ed := EventData{
			Name:      sd.Name,
			Timestamp: sd.Timestamp,
			Value:     sd.Value,
		}

		ql.ea.PublishEvent("Message Received_"+msg.RoutingKey, ed)
	}
}
