package proxy

import (
	"github.com/streadway/amqp"
	"log"
	"sync"
)

type Dispatcher struct {
	jobQueue <-chan amqp.Delivery
	worker   Worker
	done     chan error
}

func NewDispatcher(jobQueue <-chan amqp.Delivery, worker Worker) *Dispatcher {
	return &Dispatcher{
		jobQueue: jobQueue,
		worker:   worker,
		done:     make(chan error),
	}
}

func (d *Dispatcher) Run() {
	go d.dispatch()
}

func (d *Dispatcher) dispatch() {

	var wg sync.WaitGroup

	for job := range d.jobQueue {

		//log.Printf("DeliveryTag: %v", job.DeliveryTag)
		wg.Add(1)

		go func(job amqp.Delivery) {
			d.worker.work(job)
			wg.Done()
		}(job)

	}

	wg.Wait()
	d.done <- nil

}

func (d *Dispatcher) Shutdown() error {

	defer log.Printf("Dispatcher shutdown OK")

	return <-d.done

}

type Worker interface {
	work(amqp.Delivery) error
}
