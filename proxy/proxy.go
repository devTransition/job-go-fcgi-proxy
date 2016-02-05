package proxy

import (
  "fmt"
  "log"
  "strings"
  "time"
)

type ProxyInstance struct {
  config *Configuration
  amqpConnection *AmqpConnection
  consumer *Consumer
  dispatcher *Dispatcher
}

func (p *ProxyInstance) Shutdown() error {
  
  // will shutdown ProxyInstance
  
  log.Printf("ProxyInstance[%q]: Start shutdown ...", p.config.InstanceName)
  
  defer log.Printf("ProxyInstance[%q]: shutdown OK", p.config.InstanceName)

  if err := p.consumer.Shutdown(); err != nil {
    return fmt.Errorf("ProxyInstance[%q]: Error during AMQP consume shutdown: %s", p.config.InstanceName, err)
  }

  if err := p.dispatcher.Shutdown(); err != nil {
    return fmt.Errorf("ProxyInstance[%q]: Error during Dispatcher shutdown: %s", p.config.InstanceName, err)
  }

  if err := p.amqpConnection.Shutdown(); err != nil {
    return fmt.Errorf("ProxyInstance[%q]: Error during AMQP connection shutdown: %s", p.config.InstanceName, err)
  }

  return nil
}

func Create(config *Configuration) (*ProxyInstance, error) {
  
  uri := "amqp://" + config.AmqpUser + ":" + config.AmqpPassword+ "@" + config.AmqpHost
  
  amqpConnection, err := NewAmqpConnection(uri, config.AmqpPrefetchCount)
  
  if err != nil {
    return nil, err;
  }
  
  
  consumer, err := NewConsumer(amqpConnection.channel, config.AmqpQueue, config.InstanceName)
  
  if err != nil {
    return nil, err;
  }
  
  fcgiTimeout := time.Duration(config.FcgiTimeout)*time.Second
  fcgiHostAddr, fcgiHostPort := strings.Split(config.FcgiHost, ":")[0], strings.Split(config.FcgiHost, ":")[1]
  
  fcgiParams := make(map[string]string)
  
  fcgiParams["SERVER_PROTOCOL"] = config.FcgiServerProtocol
  
  fcgiParams["SERVER_ADDR"] = fcgiHostAddr
  fcgiParams["SERVER_PORT"] = fcgiHostPort
  fcgiParams["SERVER_NAME"] = config.InstanceName+".job-go-fcgi-proxy.local"
  
  fcgiParams["REMOTE_ADDR"] = "127.0.0.1"
  fcgiParams["REMOTE_PORT"] = fcgiHostPort
  
  
  fcgiParams["SCRIPT_NAME"] = config.FcgiScriptName
  fcgiParams["SCRIPT_FILENAME"] = config.FcgiScriptFilename
  fcgiParams["REQUEST_URI"] = config.FcgiRequestUri
  
  worker := NewFcgiWorker(amqpConnection.channel, config.FcgiHost, fcgiTimeout, fcgiParams)

  dispatcher := NewDispatcher(consumer.delivery, worker)
  dispatcher.Run()
  
  proxy := &ProxyInstance{config, amqpConnection, consumer, dispatcher}
  
  return proxy, nil
  
}