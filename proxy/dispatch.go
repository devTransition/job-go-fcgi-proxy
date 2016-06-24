package proxy

import (
	"encoding/json"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"sync"
)

type Dispatcher struct {
	jobQueue     <-chan amqp.Delivery
	worker       Worker
	replyChannel *amqp.Channel
	confirms     <-chan amqp.Confirmation
	done         chan error
}

func NewDispatcher(jobQueue <-chan amqp.Delivery, worker Worker, replyChannel *amqp.Channel) *Dispatcher {
	return &Dispatcher{
		jobQueue:     jobQueue,
		worker:       worker,
		replyChannel: replyChannel,
		confirms:     replyChannel.NotifyPublish(make(chan amqp.Confirmation, 1)),
		done:         make(chan error),
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

			res, reply, err := d.worker.work(&job)

			if err != nil {

				if reply {
					// publish error as reply
					d.publishReplyError(&job, err)
				}

				// TODO do we need to log errors here or publish to separate queue?
				job.Nack(false, false)

			} else {

				if reply {
					// publish result as reply
					d.publishReply(&job, res)
				}

				job.Ack(false)

			}

			wg.Done()

		}(job)

	}

	wg.Wait()

	log.Printf("Dispatcher: Job queue closed")
	d.done <- nil

	//defer log.Printf("Dispatcher finished. Job queue closed 4")

}

func (d *Dispatcher) publishReplyError(delivery *amqp.Delivery, err error) {

	log.Println(fmt.Sprintf("Dispatcher: %s", err))
	body := NewErrorMessage(fmt.Sprintf("Dispatcher: %s", err))
	//log.Printf("bodyJson: %q", body);
	bodyJson, _ := json.Marshal(body)
	//log.Printf("bodyJson: %q", bodyJson);

	d.publishReply(delivery, bodyJson)

}

func (d *Dispatcher) publishReply(delivery *amqp.Delivery, body []byte) {

	msg := amqp.Publishing{
		CorrelationId: delivery.CorrelationId,
		ContentType:   "application/x-json",
		Body:          body,
	}

	var confirmed amqp.Confirmation

	// TODO debug
	log.Printf("Publishing reply %q", msg.Body)

	d.replyChannel.Publish("", delivery.ReplyTo, false, false, msg)

	if confirmed = <-d.confirms; confirmed.Ack {
		log.Printf("Reply: published to AMQP, %q, %d", delivery.CorrelationId, confirmed.DeliveryTag)
	} else {
		log.Printf("Reply, not published to AMQP, %q, %d", delivery.CorrelationId, confirmed.DeliveryTag)
	}

	//log.Printf("%q", w.amqpChannel)
	//log.Printf("--------------------------")

	//time.Sleep(time.Second*10)

	//log.Printf("[%v] acking %q", delivery.DeliveryTag, delivery.CorrelationId)

}

func (d *Dispatcher) Shutdown() error {

	defer log.Printf("Dispatcher shutdown OK")

	return <-d.done

}

type Worker interface {
	work(*amqp.Delivery) ([]byte, bool, error)
}
