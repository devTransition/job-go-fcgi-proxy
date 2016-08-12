package main

import (
	"fmt"
	"github.com/codegangsta/cli"
	"github.com/devTransition/job-go-fcgi-proxy/fcgiwork"
	"github.com/devTransition/job-go-fcgi-proxy/proxy"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

var mutex sync.Mutex

func main() {

	app := cli.NewApp()
	app.Name = "amqp-fcgi"
	app.Usage = ""
	app.Flags = []cli.Flag{
		cli.StringFlag{Name: "config", Value: "", Usage: "config filename without extension, JSON, TOML, YAML and HCL supported"},
		cli.StringFlag{Name: "instance-name", Value: "instance-name", Usage: "unique instance name (default: instance-name)"},
		cli.StringFlag{Name: "amqp-host", Value: "127.0.0.1", Usage: "hostname (default: 127.0.0.1)"},
		cli.StringFlag{Name: "amqp-user", Value: "guest", Usage: "username (default: guest)"},
		cli.StringFlag{Name: "amqp-password", Value: "guest", Usage: "password (default: guest)"},
		cli.StringFlag{Name: "amqp-queue", Value: "rpc_queue", Usage: "name of queue (default: rpc_queue)"},
		cli.IntFlag{Name: "amqp-prefetch-count", Value: 0, Usage: "AMQP prefetch count (default: 0) "},
		cli.StringFlag{Name: "fcgi-host", Value: "127.0.0.1:9000", Usage: "hostname (default: 127.0.0.1:9000)"},
		cli.IntFlag{Name: "fcgi-timeout", Value: 5, Usage: "Fcgi connection timeout in seconds (default: 5)"},
		cli.StringFlag{Name: "fcgi-server-protocol", Value: "HTTP/1.1", Usage: "SERVER_PROTOCOL (default: HTTP/1.1)"},
		cli.StringFlag{Name: "fcgi-script-name", Value: "/core/cgi.php", Usage: "SCRIPT_NAME (default: /core/cgi.php)"},
		cli.StringFlag{Name: "fcgi-script-filename", Value: "/data/www.secucore/core/cgi.php", Usage: "SCRIPT_FILENAME (default: /data/www.secucore/core/cgi.php)"},
		cli.StringFlag{Name: "fcgi-request-uri", Value: "/core/cgi.php/job/process", Usage: "REQUEST_URI (default: /data/www.secucore/core/cgi.php)"},
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

		fcgiwork.FillServiceConfigFromCli(&serviceConfig, c)

	} else {

		err := fcgiwork.FillServiceConfigFromFile(&serviceConfig, configSource)
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
	routes := map[proxy.BrokerConfig]map[proxy.RouteConfig]*proxy.Route{}

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

		brokerRoutes, hasBrokerRoutes := routes[brokerConfig]

		if !hasBrokerRoutes {
			brokerRoutes = map[proxy.RouteConfig]*proxy.Route{}
			routes[brokerConfig] = brokerRoutes
		}

		brokerRoutes[routeConfig] = nil

	}

	// Note: "closing" here is unexpected termination (connection loss), "shutdown" is the result of graceful exit

	for brokerConfig := range routes {

		broker := brokers[brokerConfig]
		//log.Printf("Broker: %v", broker)

		// subscribe for broker connection open
		op := broker.NotifyOpen(make(chan *proxy.AmqpConnection))

		go func() {

			for range op {

				// re-create routes every time when broker connection is opened

				mutex.Lock()

				brokerRoutes := routes[brokerConfig]

				for routeConfig := range brokerRoutes {

					route := brokerRoutes[routeConfig]

					// remove old route from map
					// brokerRoutes[routeConfig] = nil

					// create route
					workerConfig := serviceConfig.Workers[routeConfig.Worker]
					route, err := proxy.CreateRoute(broker, &routeConfig, workerConfig)

					if err != nil {
						// TODO handle route creating errors
						log.Printf("%s", err)
					}

					// save new route to map
					brokerRoutes[routeConfig] = route

				}

				log.Printf("Broker routes created: %v", broker)

				mutex.Unlock()

				for routeConfig := range brokerRoutes {

					route := brokerRoutes[routeConfig]
					if route != nil {

						route.WaitClosed()

					}

				}

				broker.Open()

			}

			log.Printf("Brokers: %v", brokers)
			// should finish when op channel closed

		}()

		broker.Open()

	}

	log.Printf("Brokers: %v", brokers)
	log.Printf("Routes: %v", routes)

	if err != nil {
		log.Fatalf("%s", err)
	}

	waitShutdown := make(chan error)

	// Shutdown on sigterm
	sysigs := make(chan os.Signal, 1)
	signal.Notify(sysigs, syscall.SIGINT, syscall.SIGTERM)

	go func(sigs chan os.Signal, done chan error) {
		sig := <-sigs
		log.Printf("Got shutdown signal %v", sig)
		done <- nil
	}(sysigs, waitShutdown)

	go func(done chan error) {
		lifetimeShutdown(lifetime)
		log.Printf("Shutdown on lifetime timeout: %v sec", lifetime)
		done <- nil
	}(waitShutdown)

	// wait for shutdown
	<-waitShutdown

	mutex.Lock()
	shutdown(&routes, &brokers)
	mutex.Unlock()

}

func shutdown(routes *map[proxy.BrokerConfig]map[proxy.RouteConfig]*proxy.Route, brokers *map[proxy.BrokerConfig]*proxy.AmqpConnection) {

	var err error

	err = shutdownRoutes(routes)

	if err != nil {
		log.Fatalf("%s", err)
	}

	err = shutdownBrokers(brokers)

	if err != nil {
		log.Fatalf("%s", err)
	}

	log.Println("Shutdown success")

	// TODO debug
	// time.Sleep(time.Second * 10)
}

func shutdownRoutes(routes *map[proxy.BrokerConfig]map[proxy.RouteConfig]*proxy.Route) (err error) {

	for _, brokerRoutes := range *routes {

		for _, route := range brokerRoutes {

			if route != nil {

				route.Shutdown()

			}

		}

	}

	return err

}

func shutdownBrokers(brokers *map[proxy.BrokerConfig]*proxy.AmqpConnection) (err error) {

	for _, broker := range *brokers {

		if err = broker.Shutdown(); err != nil {
			err = fmt.Errorf("Shutdown error: %s", err)
			break
		}

	}

	return err

}

func lifetimeShutdown(lifetime int) {

	if lifetime > 0 {
		time.Sleep(time.Second * time.Duration(lifetime))
	} else {

		for {
			time.Sleep(time.Second)
		}

	}

}
