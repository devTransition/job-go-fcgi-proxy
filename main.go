package main

import (
	"fmt"
	"github.com/codegangsta/cli"
	"github.com/devTransition/job-go-fcgi-proxy/fcgiwork"
	"log"
	"os"
	"github.com/devTransition/job-go-fcgi-proxy/proxy"
)


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

	serviceConfig := &proxy.ServiceConfig{}

	if configSource == "" {

		fcgiwork.FillServiceConfigFromCli(serviceConfig, c)

	} else {

		err := fcgiwork.FillServiceConfigFromFile(serviceConfig, configSource)
		if err != nil {
			log.Fatalf("%s", err)
		}

	}

	//log.Print(serviceConfig)

	if len(serviceConfig.Routes) == 0 {
		log.Fatalln("No configuration provided")
		//log.Print(proxyConfigs)
	}

	proxy.Run(serviceConfig, lifetime)

}
