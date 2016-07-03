package proxy

import (
	"encoding/json"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"sync"
)

type Dispatcher struct {
	jobQueue       <-chan amqp.Delivery
	worker         Worker
	replyChannel   *amqp.Channel
	confirms       <-chan amqp.Confirmation
	mutex          sync.Mutex
	finishNotifies []chan error
	noNotify       bool // true when we will never notify again
}

func NewDispatcher(jobQueue <-chan amqp.Delivery, worker Worker, replyChannel *amqp.Channel) *Dispatcher {
	return &Dispatcher{
		jobQueue:     jobQueue,
		worker:       worker,
		replyChannel: replyChannel,
		confirms:     replyChannel.NotifyPublish(make(chan amqp.Confirmation, 1)),
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

	d.finish()
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

func (d *Dispatcher) NotifyFinished(ch chan error) chan error {

	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.noNotify {
		close(ch)
	} else {
		d.finishNotifies = append(d.finishNotifies, ch)
	}

	return ch
}

func (d *Dispatcher) finish() {

	d.mutex.Lock()
	defer d.mutex.Unlock()

	log.Printf("Dispatcher: finishing")
	defer log.Printf("Dispatcher finish OK")

	for _, ch := range d.finishNotifies {
		ch <- nil
	}

	for _, ch := range d.finishNotifies {
		close(ch)
	}

	d.noNotify = true

}

type Worker interface {
	work(*amqp.Delivery) ([]byte, bool, error)
}
