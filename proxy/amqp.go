package proxy

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

type AmqpConnection struct {
	uri          string
	config       *BrokerConfig
	mutex        sync.Mutex
	conn         *amqp.Connection
	openNotifies []chan *AmqpConnection
	shutdownNotifies []chan struct{}
	inShutdown   chan struct{}
}

func NewAmqpConnection(config *BrokerConfig) (*AmqpConnection, error) {

	c := &AmqpConnection{
		config: config,
		uri:    "amqp://" + config.User + ":" + config.Password + "@" + config.Host,
	}
	
	c.inShutdown = c.NotifyShutdown(make(chan struct{}))
	
	return c, nil

}

func (c *AmqpConnection) Open() {

	go c.tryDial()

}

func (c *AmqpConnection) tryDial() {

	ticker := time.NewTicker(2 * time.Second)
	//defer ticker.Stop()

	for {

		select {
		case <-ticker.C:
			_, err := c.dial()
			
			select {
			case <-c.inShutdown:
				// unblock shutdown call and finish
				if err != nil {
					log.Printf("Broker: %q: got shutdown on reconnect", c.config.Id)
				} else {
					log.Printf("Broker: %q: got shutdown when connected", c.config.Id)
				}
				return
			default:
				if err != nil {
					log.Printf("Broker %q: trying to reconnect in 2 seconds...", c.config.Id)
				} else {
					// init broker and wait for shutdown
					c.init()
					ticker.Stop()
					ticker.C = nil
				}
			}
			
		case <-c.inShutdown:
			log.Printf("Broker %q: got shutdown", c.config.Id)
			// unblock shutdown call and finish
			return
		}

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

	return c.conn, err

}

func (c *AmqpConnection) init() {
	
	defer log.Printf("Broker %q: initialized", c.config.Id)
	
	for _, ch := range c.openNotifies {
		ch <- c
	}

	closed := c.NotifyClose(make(chan *amqp.Error, 1))

	go func() {

		log.Printf("Broker %q: connection closed, %s", c.config.Id, <-closed)

	}()

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

func (c *AmqpConnection) NotifyShutdown(ch chan struct{}) chan struct{} {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.shutdownNotifies = append(c.shutdownNotifies, ch)

	return ch
}

func (c *AmqpConnection) Shutdown() error {

	for _, ch := range c.shutdownNotifies {
		ch <- struct {}{}
	}
	
	defer log.Printf("AMQP Connection shutdown OK")

	c.mutex.Lock()
	defer c.mutex.Unlock()

	for _, ch := range c.openNotifies {
		close(ch)
	}
	
	for _, ch := range c.shutdownNotifies {
		close(ch)
	}

	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			return fmt.Errorf("AMQP connection close error: %s", err)
		}
	}

	return nil

}
