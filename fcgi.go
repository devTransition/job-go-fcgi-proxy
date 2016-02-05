package main

import (
  "log"
  "os"
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
  
  configFile := c.String("config")
  
  proxyConfigs := []proxy.Configuration{}
  
  if configFile == "" {
    
    config := proxy.Configuration{
      InstanceName:c.String("instance-name"),
      
      AmqpHost:c.String("amqp-host"),
      AmqpPassword:c.String("amqp-password"),
      AmqpPrefetchCount:c.Int("amqp-prefetch-count"),
      AmqpQueue:c.String("amqp-queue"),
      AmqpUser:c.String("amqp-user"),
      
      FcgiHost:c.String("fcgi-host"),
      FcgiRequestUri:c.String("fcgi-request-uri"),
      FcgiScriptFilename:c.String("fcgi-script-filename"),
      FcgiScriptName:c.String("fcgi-script-name"),
      FcgiServerProtocol:c.String("fcgi-server-protocol"),
      FcgiTimeout:c.Int("fcgi-timeout"),
    }
    
    proxyConfigs = append(proxyConfigs, config)
    //log.Print(proxyConfigs)
    
  } else {
    
    // TODO load configuration from file
    
  }
  
  if len(proxyConfigs) == 0 {
    log.Fatalln("No configuration provided")
    //log.Print(proxyConfigs)
  }
  
  proxies := []proxy.ProxyInstance{}
  
  var err error
  for _, config := range proxyConfigs {
    
    log.Print(config)
    proxy, _err := proxy.Create(&config)
    
    if _err != nil {
      err = _err
      break
    } else {
      proxies = append(proxies, *proxy)
    }
    
  }
  
  //log.Print(proxies)
  
  if err != nil {
    log.Fatalf("%s", err)
  }
  
  var wg sync.WaitGroup
  
  wg.Add(1);

  go func() {
    waitShutdown(lifetime)
    wg.Done()
  }()

  wg.Wait()
  
  for _, proxy := range proxies {
    
    if err := proxy.Shutdown(); err != nil {
      log.Fatalf("Error during Proxy shutdown: %s", err)
    }
    
  }
  
  println("Shutdown success")
  
}

func waitShutdown(lifetime int) {
  
  // TODO add option here to exit on signal
  
  if lifetime > 0 {
    time.Sleep(time.Second * time.Duration(lifetime))
  } else {
    
    for {
      time.Sleep(time.Second)
    }
    
  }
  
}