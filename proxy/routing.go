package proxy

import (
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

type Route struct {
	config       *RouteConfig
	workerConfig WorkerConfig
	channel      *amqp.Channel
	consumer     *Consumer
	dispatcher   *Dispatcher
	closed       chan *amqp.Error
}

func CreateRoute(amqpConnection *AmqpConnection, config *RouteConfig, workerConfig WorkerConfig) (*Route, error) {

	log.Printf("Creating route: %v", config)
	channel, err := amqpConnection.conn.Channel()

	/*
		go func() {
			// not nil when amqp connection broken
			log.Printf("Route: closing channel: %s", <-channel.NotifyClose(make(chan *amqp.Error)))
		}()
	*/

	// TODO add open/close connection handlers to re-create route on reconnect
	// TODO before re-creating the route on amqp reconnect we need to wait for workers from old one

	if err != nil {
		return nil, fmt.Errorf("Route %v channel: %s", config, err)
	}

	err = channel.Qos(
		config.PrefetchCount, // prefetch count
		0,                    // prefetch size
		false,                // global
	)

	if err != nil {
		return nil, fmt.Errorf("Route %v channel: Failed to set QoS: %s", config, err)
	}

	if err := channel.Confirm(false); err != nil {
		return nil, fmt.Errorf("Route %v channel: Confirm mode failed: %s", config, err)
	}

	consumer, err := NewConsumer(channel, config.Queue, config.Name)

	if err != nil {
		return nil, err
	}

	worker := workerConfig.CreateWorker(config)

	dispatcher := NewDispatcher(consumer.delivery, worker, channel)
	dispatcher.Run()

	closing := amqpConnection.conn.NotifyClose(make(chan *amqp.Error, 1))

	route := &Route{
		config:       config,
		workerConfig: workerConfig,
		channel:      channel,
		consumer:     consumer,
		dispatcher:   dispatcher,
		closed:       make(chan *amqp.Error, 1),
	}

	go func() {

		err := <-closing

		if err != nil {

			// if not <nil> connection is broken

			log.Printf("Route: closing start: %s", err)
			// close channel, to release delivery channel for dispatcher
			channel.Close()
			// wait for job queue finish in dispatcher
			<-dispatcher.NotifyFinished(make(chan error, 1))

			log.Printf("Route: closed")

			route.closed <- err

		}

	}()

	return route, nil

}

func (instance *Route) WaitClosed() error {

	return <-instance.closed

}

func (instance *Route) Shutdown() error {

	// will shutdown Route

	log.Printf("Route %q: Start shutdown ...", instance.config.Name)

	defer log.Printf("Route %q: shutdown OK", instance.config.Name)

	if err := instance.consumer.Shutdown(); err != nil {
		log.Printf("Route %q: Error during AMQP consume shutdown: %s", instance.config.Name, err)
	} else {

		<-instance.dispatcher.NotifyFinished(make(chan error, 1))

		if err := instance.channel.Close(); err != nil {
			log.Printf("Route %q: Error during AMQP Channel close: %s", instance.config.Name, err)
		}

	}

	return nil
}
