package proxy

import (
  "fmt"
  "log"
  "github.com/streadway/amqp"
)

type AmqpConnection struct {
  conn    *amqp.Connection
  channel *amqp.Channel
}

func NewAmqpConnection(uri string, prefetchCount int) (*AmqpConnection, error) {

  c := &AmqpConnection{
    conn: nil,
    channel: nil,
  }

  var err error

  log.Printf("dialing %q", uri)
  c.conn, err = amqp.Dial(uri)

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

  err = c.channel.Qos(
    prefetchCount, // prefetch count
    0, // prefetch size
    false, // global
  )

  if err != nil {
    return nil, fmt.Errorf("Failed to set QoS")
  }
  
  if err := c.channel.Confirm(false); err != nil {
    return nil, fmt.Errorf("Channel could not be put into confirm mode: %s", err)
  }

  return c, nil

}

func (c *AmqpConnection) Shutdown() error {

  defer log.Printf("AMQP Connection shutdown OK")

  if err := c.conn.Close(); err != nil {
    return fmt.Errorf("AMQP connection close error: %s", err)
  }

  return nil

}