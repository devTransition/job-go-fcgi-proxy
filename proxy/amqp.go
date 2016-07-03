package proxy

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"sync"
	"time"
)

type AmqpConnection struct {
	uri          string
	config       *BrokerConfig
	mutex        sync.Mutex
	opener       sync.Once
	conn         *amqp.Connection
	openNotifies []chan *AmqpConnection
	shutdown     bool
}

func NewAmqpConnection(config *BrokerConfig) (*AmqpConnection, error) {

	c := &AmqpConnection{
		config: config,
		uri:    "amqp://" + config.User + ":" + config.Password + "@" + config.Host,
	}

	/*
		notify := c.NotifyOpen(make(chan *AmqpConnection))

		go func() {

			log.Printf("Broker: connection ready: %s", (<-notify).config)

		}()
	*/

	/*
		var err error

		log.Printf("Broker %q dialing to %q", config.Id, c.uri)
		c.conn, err = amqp.Dial(c.uri)

		if err != nil {
			return nil, fmt.Errorf("Broker %v dial error: %s", config, err)
		}

		go func() {
			log.Printf("Broker: closing: %s", <-c.conn.NotifyClose(make(chan *amqp.Error)))
		}()
	*/

	return c, nil

}

func (c *AmqpConnection) Open() {

	go c.tryDial()

}

func (c *AmqpConnection) tryDial() {

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for ; ; <-ticker.C {

		c.mutex.Lock()

		if c.shutdown {
			log.Printf("cancel reconnect, in shutdown")
			return
		}

		_, err := c.dial()
		c.mutex.Unlock()

		if err != nil {
			log.Printf("trying to reconnect in 2 seconds...")
			continue
		}

		return
	}

}

func (c *AmqpConnection) dial() (*amqp.Connection, error) {

	log.Printf("Broker %q: dialing to %q", c.config.Id, c.uri)

	var err error
	c.conn, err = amqp.Dial(c.uri)

	if err != nil {
		return nil, fmt.Errorf("Broker %v dial error: %s", c.config, err)
	}

	log.Printf("Broker %q: dial success", c.config.Id)

	for _, ch := range c.openNotifies {
		ch <- c
	}

	//closed := c.conn.NotifyClose(make(chan *amqp.Error, 1))
	closed := c.NotifyClose(make(chan *amqp.Error, 1))

	go func() {

		log.Printf("Broker %q: connection closed, %s", c.config.Id, <-closed)

	}()

	return c.conn, err

}

func (c *AmqpConnection) NotifyOpen(ch chan *AmqpConnection) chan *AmqpConnection {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.openNotifies = append(c.openNotifies, ch)

	return ch
}

func (c *AmqpConnection) NotifyClose(ch chan *amqp.Error) chan *amqp.Error {

	return c.conn.NotifyClose(ch)

}

func (c *AmqpConnection) Shutdown() error {

	c.mutex.Lock()
	defer c.mutex.Unlock()
	defer log.Printf("AMQP Connection shutdown OK")

	c.shutdown = true

	for _, ch := range c.openNotifies {
		close(ch)
	}

	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			return fmt.Errorf("AMQP connection close error: %s", err)
		}
	}

	return nil

}
