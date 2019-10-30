package rabbit

import (
	"fmt"
	"log"

	"github.com/streadway/amqp"

	"github.com/kc0isg/azure-logging-shipper/internal/pkg/batcher"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	TotalCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "azure_log_rabbit_consumer_read_total",
		Help: "Total number of log events read from rabbitmq",
	})
)

type Consumer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	batcher *batcher.Batcher
	tag     string
	done    chan error
}

func (c *Consumer) CreateExchange(exchangeName string, exchangeType string) error {
	log.Printf("declaring Exchange (%q)", exchangeName)
	if err := c.channel.ExchangeDeclare(
		exchangeName, // name of the exchange
		exchangeType, // type
		true,         // durable
		false,        // delete when complete
		false,        // internal
		false,        // noWait
		nil,          // arguments
	); err != nil {
		return fmt.Errorf("Exchange Declare: %s", err)
	}

	return nil
}

func (c *Consumer) CreateQueue(queueName string) (amqp.Queue, error) {
	log.Printf("declaring Queue %q", queueName)
	queue, err := c.channel.QueueDeclare(
		queueName, // name of the queue
		false,     // durable
		true,      // delete when unused
		false,     // exclusive
		false,     // noWait
		nil,       // arguments
	)

	if err != nil {
		return queue, fmt.Errorf("Queue Declare: %s", err)
	}

	return queue, nil
}

func (c *Consumer) BindQueue(queue amqp.Queue, exchange string, key string) error {

	log.Printf("binding queue (%q %d messages, %d consumers) to Exchange (key %q)",
		queue.Name, queue.Messages, queue.Consumers, key)

	if err := c.channel.QueueBind(
		queue.Name, // name of the queue
		key,        // bindingKey
		exchange,   // sourceExchange
		false,      // noWait
		nil,        // arguments
	); err != nil {
		return fmt.Errorf("Queue Bind: %s", err)
	}

	return nil
}

func (c *Consumer) ConsumeQueue(queueName string) error {
	log.Printf("starting Consumer (consumer tag %q)", c.tag)

	deliveries, err := c.channel.Consume(
		queueName, // name
		c.tag,     // consumerTag,
		false,     // noAck
		false,     // exclusive
		false,     // noLocal
		false,     // noWait
		nil,       // arguments
	)

	if err != nil {
		return fmt.Errorf("Queue Consume: %s", err)
	}

	go handle(c.batcher, deliveries, c.done)

	return nil
}

func NewConsumer(amqpURI, ctag string, batcher *batcher.Batcher) (*Consumer, error) {
	c := &Consumer{
		conn:    nil,
		channel: nil,
		tag:     ctag,
		done:    make(chan error),
		batcher: batcher,
	}

	var err error

	log.Printf("dialing %q", amqpURI)
	c.conn, err = amqp.Dial(amqpURI)
	if err != nil {
		return nil, fmt.Errorf("Dial: %s", err)
	}

	go func() {
		fmt.Printf("closing: %s", <-c.conn.NotifyClose(make(chan *amqp.Error)))
	}()

	log.Printf("got Connection, getting Channel")
	c.channel, err = c.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("Channel: %s", err)
	}

	return c, nil
}

func (c *Consumer) Shutdown() error {
	// will close() the deliveries channel
	if err := c.channel.Cancel(c.tag, true); err != nil {
		return fmt.Errorf("Consumer cancel failed: %s", err)
	}

	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %s", err)
	}

	defer log.Printf("AMQP shutdown OK")

	// wait for handle() to exit
	return <-c.done
}

func handle(batcher *batcher.Batcher, deliveries <-chan amqp.Delivery, done chan error) {
	for d := range deliveries {

		TotalCounter.Inc()
		batcher.AddToBatch(d)

	}

	log.Printf("handle: deliveries channel closed")
	batcher.FlushBatch()
	done <- nil
}
