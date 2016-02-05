package proxy

import (
  "fmt"
  "log"
  "github.com/streadway/amqp"
)

type Consumer struct {
  channel  *amqp.Channel
  tag      string
  delivery <-chan amqp.Delivery
}

func NewConsumer(channel *amqp.Channel, queue string, ctag string) (*Consumer, error) {

  c := &Consumer{
    channel: channel,
    tag:     ctag,
  }

  var err error

  log.Printf("got Channel, starting Consume (consumer tag %q)", c.tag)
  c.delivery, err = c.channel.Consume(
    queue, // name
    c.tag, // consumerTag,
    false, // noAck
    false, // exclusive
    false, // noLocal
    false, // noWait
    nil, // arguments
  )

  if err != nil {
    return nil, fmt.Errorf("Queue Consume: %s", err)
  }

  return c, nil

}

func (c *Consumer) Shutdown() error {
  // will close() the deliveries channel
  
  defer log.Printf("AMQP consumer [%q] shutdown OK", c.tag)

  if err := c.channel.Cancel(c.tag, true); err != nil {
    return fmt.Errorf("Consumer cancel failed: %s", err)
  }

  return nil
}
