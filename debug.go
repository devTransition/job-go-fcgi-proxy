package main

import (
	"time"
	"syscall"
	"os"
	"os/signal"
	"log"
	"github.com/codegangsta/cli"
)

func main() {
	
	app := cli.NewApp()
	
	app.Action = func(c *cli.Context) {
		runApp(c)
	}

	app.Run(os.Args)
	
}

func runApp(c *cli.Context) {
	
	waitShutdown := make(chan error)
	
	sysigs := make(chan os.Signal, 1)
	signal.Notify(sysigs, syscall.SIGINT, syscall.SIGTERM)
	
	go func(sigs chan os.Signal, done chan error) {
		sig := <-sigs
		log.Printf("Got shutdown signal %v", sig)
		done <- nil
	}(sysigs, waitShutdown)
	
	go func(done chan error) {
		lifetimeShutdown(5)
		log.Printf("Shutdown on lifetime timeout: %v sec", 5)
		done <- nil
	}(waitShutdown)
	
	<- waitShutdown
	
	log.Printf("Shutdown finished")
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
