package proxy

import (
  "fmt"
  "log"
  "github.com/streadway/amqp"
)

type AmqpConnection struct {
  conn    *amqp.Connection
}

func NewAmqpConnection(config *BrokerConfig) (*AmqpConnection, error) {
  
  uri := "amqp://" + config.User + ":" + config.Password+ "@" + config.Host 
  c := &AmqpConnection{conn: nil}
  
  var err error
  
  log.Printf("Broker %q dialing to %q", config.Id, uri)
  c.conn, err = amqp.Dial(uri)
  
  if err != nil {
    return nil, fmt.Errorf("Broker %v dial error: %s", config, err)
  }

  go func() {
    fmt.Printf("closing: %s", <-c.conn.NotifyClose(make(chan *amqp.Error)))
  }()
  
  return c, nil

}

func (c *AmqpConnection) Shutdown() error {

  defer log.Printf("AMQP Connection shutdown OK")

  if err := c.conn.Close(); err != nil {
    return fmt.Errorf("AMQP connection close error: %s", err)
  }

  return nil

}