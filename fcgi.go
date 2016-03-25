package main

import (
  "log"
  "os"
  "os/signal"
  "syscall"
  "sync"
  "time"
  "github.com/codegangsta/cli"
  "github.com/devTransition/job-go-fcgi-proxy/proxy"
  "fmt"
)

func main() {
  
  app := cli.NewApp()
  app.Name = "amqp-fcgi"
  app.Usage = ""
  app.Flags = []cli.Flag{
    cli.StringFlag{Name: "config", Value:"", Usage: "config filename without extension, JSON, TOML, YAML and HCL supported"},
    cli.StringFlag{Name: "instance-name", Value:"instance-name", Usage: "unique instance name (default: instance-name)"},
    cli.StringFlag{Name: "amqp-host", Value:"127.0.0.1", Usage: "hostname (default: 127.0.0.1)"},
    cli.StringFlag{Name: "amqp-user", Value:"guest", Usage: "username (default: guest)"},
    cli.StringFlag{Name: "amqp-password", Value:"guest", Usage: "password (default: guest)"},
    cli.StringFlag{Name: "amqp-queue", Value:"rpc_queue", Usage: "name of queue (default: rpc_queue)"},
    cli.IntFlag{Name: "amqp-prefetch-count", Value: 0, Usage: "AMQP prefetch count (default: 0) "},
    cli.StringFlag{Name: "fcgi-host", Value:"127.0.0.1:9000", Usage: "hostname (default: 127.0.0.1:9000)"},
    cli.IntFlag{Name: "fcgi-timeout", Value: 5, Usage: "Fcgi connection timeout in seconds (default: 5)"},
    cli.StringFlag{Name: "fcgi-server-protocol", Value:"HTTP/1.1", Usage: "SERVER_PROTOCOL (default: HTTP/1.1)"},
    cli.StringFlag{Name: "fcgi-script-name", Value:"/core/cgi.php", Usage: "SCRIPT_NAME (default: /core/cgi.php)"},
    cli.StringFlag{Name: "fcgi-script-filename", Value:"/data/www.secucore/core/cgi.php", Usage: "SCRIPT_FILENAME (default: /data/www.secucore/core/cgi.php)"},
    cli.StringFlag{Name: "fcgi-request-uri", Value:"/core/cgi.php/job/process", Usage: "REQUEST_URI (default: /data/www.secucore/core/cgi.php)"},
    //cli.StringFlag{Name: "ctag", Value:"simple-consumer", Usage: "unique tag for consumer (default: simple-consumer)"},
    cli.IntFlag{Name: "lifetime", Value: 0, Usage: "Number of seconds (default: 0, forever)"},
    // TODO add some -debug param to enable console log
  }
  app.Action = func(c *cli.Context) {
    runApp(c)
  }

  app.Run(os.Args)
  
}

func runApp(c *cli.Context) {

  lifetime := c.Int("lifetime")
  
  if lifetime == 0 {
    println("Running forever...")
  } else {
    println(fmt.Sprintf("Running ... Will exit after %d sec", lifetime))
  }
  
  configSource := c.String("config")
  
  serviceConfig := proxy.ServiceConfig{}
  
  if configSource == "" {
    
    proxy.FillServiceConfigFromCli(&serviceConfig, c)
    
  } else {
    
    err := proxy.FillServiceConfigFromFile(&serviceConfig, configSource)
    if err != nil {
      log.Fatalf("%s", err)
    }
    
  }
  
  //log.Print(serviceConfig)
  
  if len(serviceConfig.Routes) == 0 {
    log.Fatalln("No configuration provided")
    //log.Print(proxyConfigs)
  }
  
  brokers := map[proxy.BrokerConfig]*proxy.AmqpConnection{}
  routes := map[proxy.RouteConfig]*proxy.Route{}
  
  var err error
  for _, routeConfig := range serviceConfig.Routes {
    
    brokerConfig := serviceConfig.Brokers[routeConfig.Broker]
    broker, hasBroker := brokers[brokerConfig]
    if !hasBroker {
      broker, err = proxy.NewAmqpConnection(&brokerConfig)
      if err != nil {
        break
      }
      brokers[brokerConfig] = broker
    }
    
    workerConfig := serviceConfig.Workers[routeConfig.Worker]
    
    route, _err := proxy.CreateRoute(broker, &routeConfig, &workerConfig)
    if _err != nil {
      err = _err
      break
    }
    routes[routeConfig] = route
  }
  
  log.Printf("%v", brokers)
  log.Printf("%v", routes)
  
  if err != nil {
    log.Fatalf("%s", err)
  }
  
  //log.Fatalln("Debug stop")
  
  // Shutdown on sigterm
  sigs := make(chan os.Signal, 1)
  signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
  
  go func(routes *map[proxy.RouteConfig]*proxy.Route, brokers *map[proxy.BrokerConfig]*proxy.AmqpConnection) {
    sig := <-sigs
    log.Printf("Got shutdown signal %v", sig)
    shutdown(routes, brokers)
    os.Exit(0)
	}(&routes, &brokers)
  
  var wg sync.WaitGroup
  
  wg.Add(1);

  go func() {
    waitShutdown(lifetime)
    wg.Done()
  }()

  wg.Wait()
  
  shutdown(&routes, &brokers)
  
}

func shutdown(routes *map[proxy.RouteConfig]*proxy.Route, brokers *map[proxy.BrokerConfig]*proxy.AmqpConnection) {
  
  var err error
  
  err = shutdownRoutes(routes)
  
  if err != nil {
    log.Fatalf("%s", err)
  }
  
  err = shutdownBrokers(brokers)
  
  if err != nil {
    log.Fatalf("%s", err)
  }
  
  println("Shutdown success")
  
}

func shutdownRoutes(routes *map[proxy.RouteConfig]*proxy.Route) (err error) {
  
  for _, route := range *routes {
    
    if err = route.Shutdown(); err != nil {
      err = fmt.Errorf("Shutdown error: %s", err)
    }
    
  }
  
  return err
  
}

func shutdownBrokers(brokers *map[proxy.BrokerConfig]*proxy.AmqpConnection) (err error) {
  
  for _, broker := range *brokers {
    
    if err = broker.Shutdown(); err != nil {
      err = fmt.Errorf("Shutdown error: %s", err)
    }
    
  }
  
  return err
  
}

func waitShutdown(lifetime int) {
  
  if lifetime > 0 {
    time.Sleep(time.Second * time.Duration(lifetime))
  } else {
    
    for {
      time.Sleep(time.Second)
    }
    
  }
  
}