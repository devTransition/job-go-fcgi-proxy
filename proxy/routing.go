package proxy

import (
  "fmt"
  "log"
  "strings"
  "time"
  "github.com/streadway/amqp"
)

type Route struct {
  config *RouteConfig
  workerConfig *WorkerConfig
  channel *amqp.Channel
  consumer *Consumer
  dispatcher *Dispatcher
}

func (instance *Route) Shutdown() error {
  
  // will shutdown Route
  
  log.Printf("Route %q: Start shutdown ...", instance.config.Name)
  
  defer log.Printf("Route %q: shutdown OK", instance.config.Name)

  if err := instance.consumer.Shutdown(); err != nil {
    return fmt.Errorf("Route %q: Error during AMQP consume shutdown: %s", instance.config.Name, err)
  }

  if err := instance.dispatcher.Shutdown(); err != nil {
    return fmt.Errorf("Route %q: Error during Dispatcher shutdown: %s", instance.config.Name, err)
  }

  if err := instance.channel.Close(); err != nil {
    return fmt.Errorf("Route %q: Error during AMQP Channel close: %s", instance.config.Name, err)
  }
  
  return nil
}

func CreateRoute(amqpConnection *AmqpConnection, config *RouteConfig, workerConfig *WorkerConfig) (*Route, error) {
  
  log.Printf("Creating route: %v", config)
  
  channel, err := amqpConnection.conn.Channel()
  
  if err != nil {
    return nil, fmt.Errorf("Route %v channel: %s", config, err)
  }
  
  err = channel.Qos(
    config.PrefetchCount, // prefetch count
    0, // prefetch size
    false, // global
  )
  
  if err != nil {
    return nil, fmt.Errorf("Route %v channel: Failed to set QoS: %s", config, err)
  }
  
  if err := channel.Confirm(false); err != nil {
    return nil, fmt.Errorf("Route %v channel: Confirm mode failed: %s", config, err)
  }
  
  consumer, err := NewConsumer(channel, config.Queue, config.Name)
  
  if err != nil {
    return nil, err;
  }
  
  fcgiTimeout := time.Duration(workerConfig.Timeout)*time.Second
  fcgiHostAddr, fcgiHostPort := strings.Split(workerConfig.Host, ":")[0], strings.Split(workerConfig.Host, ":")[1]
  
  fcgiParams := make(map[string]string)
  
  fcgiParams["SERVER_PROTOCOL"] = workerConfig.ServerProtocol
  
  fcgiParams["SERVER_ADDR"] = fcgiHostAddr
  fcgiParams["SERVER_PORT"] = fcgiHostPort
  fcgiParams["SERVER_NAME"] = config.Name+".job-go-fcgi-proxy.local"
  
  fcgiParams["REMOTE_ADDR"] = "127.0.0.1"
  fcgiParams["REMOTE_PORT"] = fcgiHostPort
  
  
  fcgiParams["SCRIPT_NAME"] = workerConfig.ScriptName
  fcgiParams["SCRIPT_FILENAME"] = workerConfig.ScriptFilename
  fcgiParams["REQUEST_URI"] = workerConfig.RequestUri
  
  //ch, err := amqpConnection.conn.Channel()
  worker := NewFcgiWorker(channel, workerConfig.Host, fcgiTimeout, fcgiParams)

  dispatcher := NewDispatcher(consumer.delivery, worker)
  dispatcher.Run()
  
  route := &Route{
    config:config, 
    workerConfig: workerConfig, 
    channel:channel,
    consumer:consumer,
    dispatcher:dispatcher,
  }
  
  return route, nil
  
}


