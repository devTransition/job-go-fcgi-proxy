package main

import (
	"github.com/codegangsta/cli"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {

	app := cli.NewApp()

	app.Action = func(c *cli.Context) {
		runApp(c)
	}

	app.Run(os.Args)

}

func runApp(c *cli.Context) {

	log.Printf("start waitSmth")

	var waitSmth chan int
	waitSmth = make(chan int, 1)

	go func() {
		waitSmth <- 10
		//log.Printf("pushed %v", 10)
	}()

	go func() {
		waitSmth <- 20
		//log.Printf("pushed %v", 20)

		time.Sleep(time.Second * 10)
		close(waitSmth)

	}()

	/*
		var r int
		for ; r == 0; r = <- waitSmth {

			if r > 0 {
				log.Printf("waitSmth, %v", r)
			}

		}
	*/

	for range waitSmth {

		log.Printf("waitSmth")

	}

	/*
		go func() {

			log.Printf("start waitSmth1")

			var waitSmth <- chan int
			waitSmth = make(chan int, 1)

			for ; ; <- waitSmth {

				log.Printf("waitSmth1")

			}
		}()
	*/

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

	<-waitShutdown

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
