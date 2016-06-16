package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/codegangsta/cli"
	"github.com/streadway/amqp"
	"github.com/tomasen/fcgi_client"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

func main() {
	app := cli.NewApp()
	app.Name = "amqp-fcgi"
	app.Usage = ""
	app.Flags = []cli.Flag{
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

	testSmth()

	println("Running...")

	var wg sync.WaitGroup

	uri := "amqp://" + c.String("amqp-user") + ":" + c.String("amqp-password") + "@" + c.String("amqp-host")
	queue := c.String("amqp-queue")

	// TODO need to be generated UUID
	ctag := c.String("instance-name")
	prefetchCount := c.Int("amqp-prefetch-count")
	fcgiHost := c.String("fcgi-host")
	fcgiTimeout := time.Duration(c.Int("fcgi-timeout")) * time.Second
	lifetime := c.Int("lifetime")

	fcgiHostAddr, fcgiHostPort := strings.Split(fcgiHost, ":")[0], strings.Split(fcgiHost, ":")[1]

	fcgiParams := make(map[string]string)

	fcgiParams["SERVER_PROTOCOL"] = c.String("fcgi-server-protocol")

	fcgiParams["SERVER_ADDR"] = fcgiHostAddr
	fcgiParams["SERVER_PORT"] = fcgiHostPort
	fcgiParams["SERVER_NAME"] = c.String("instance-name") + ".job-go-fcgi-proxy.local"

	fcgiParams["REMOTE_ADDR"] = "127.0.0.1"
	fcgiParams["REMOTE_PORT"] = fcgiHostPort

	//log.Printf("%q %q", fcgiHostAddr, fcgiHostPort);

	fcgiParams["SCRIPT_NAME"] = c.String("fcgi-script-name")
	fcgiParams["SCRIPT_FILENAME"] = c.String("fcgi-script-filename")
	fcgiParams["REQUEST_URI"] = c.String("fcgi-request-uri")

	amqpConnection, err := NewAmqpConnection(uri, prefetchCount)
	if err != nil {
		log.Fatalf("%s", err)
	}

	consumer, err := NewConsumer(amqpConnection.channel, queue, ctag)

	if err != nil {
		log.Fatalf("%s", err)
	}

	worker := NewFcgiWorker(amqpConnection.channel, fcgiHost, fcgiTimeout, fcgiParams)

	dispatcher := NewDispatcher(consumer.delivery, worker)
	dispatcher.Run()

	wg.Add(1)

	go func() {
		waitShutdown(lifetime)
		wg.Done()
	}()

	wg.Wait()

	log.Printf("Start shutdown ...")

	if err := consumer.Shutdown(); err != nil {
		log.Fatalf("error during AMQP consume shutdown: %s", err)
	}

	if err := dispatcher.Shutdown(); err != nil {
		log.Fatalf("error during Dispatcher shutdown: %s", err)
	}

	if err := amqpConnection.Shutdown(); err != nil {
		log.Fatalf("error during AMQP connection shutdown: %s", err)
	}

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

/*
{
	"status": "error",
	"error": "ProductInternalException",
	"error_details": "Proxy: Backend not availble",
	"error_user": "Es ist ein interner Fehler aufgetreten",
	"code": 0
}
*/

type ErrorMessage struct {
	Status       string `json:"status"`
	Error        string `json:"error"`
	ErrorDetails string `json:"error_details"`
	ErrorUser    string `json:"error_user"`
	Code         int    `json:"code"`
}

func NewErrorMessage(details string) *ErrorMessage {
	return &ErrorMessage{Status: "error", Error: "ProductInternalException", ErrorDetails: details}
}

type AmqpConnection struct {
	conn    *amqp.Connection
	channel *amqp.Channel
}

func NewAmqpConnection(uri string, prefetchCount int) (*AmqpConnection, error) {

	c := &AmqpConnection{
		conn:    nil,
		channel: nil,
	}

	var err error

	log.Printf("dialing %q", uri)
	c.conn, err = amqp.Dial(uri)

	if err != nil {
		return nil, fmt.Errorf("Dial: %s", err)
	}

	go func() {
		fmt.Printf("closing: %s", <-c.conn.NotifyClose(make(chan *amqp.Error)))
	}()

	log.Printf("got Connection, getting Channel")
	c.channel, err = c.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("Channel: %s", err)
	}

	err = c.channel.Qos(
		prefetchCount, // prefetch count
		0,             // prefetch size
		false,         // global
	)

	if err != nil {
		return nil, fmt.Errorf("Failed to set QoS")
	}

	if err := c.channel.Confirm(false); err != nil {
		return nil, fmt.Errorf("Channel could not be put into confirm mode: %s", err)
	}

	return c, nil

}

func (c *AmqpConnection) Shutdown() error {

	defer log.Printf("AMQP Connection shutdown OK")

	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %s", err)
	}

	return nil

}

type Consumer struct {
	channel  *amqp.Channel
	tag      string
	delivery <-chan amqp.Delivery
}

func NewConsumer(channel *amqp.Channel, queue string, ctag string) (*Consumer, error) {

	c := &Consumer{
		channel: channel,
		tag:     ctag,
	}

	var err error

	log.Printf("got Channel, starting Consume (consumer tag %q)", c.tag)
	c.delivery, err = c.channel.Consume(
		queue, // name
		c.tag, // consumerTag,
		false, // noAck
		false, // exclusive
		false, // noLocal
		false, // noWait
		nil,   // arguments
	)

	if err != nil {
		return nil, fmt.Errorf("Queue Consume: %s", err)
	}

	return c, nil

}

/*
func consume(delivery <-chan amqp.Delivery, jobQueue chan<- amqp.Delivery, done chan<- error) {

  for d := range delivery {

    jobQueue <- d
    //d.Ack(false)

  }

  done <- nil

}
*/

func (c *Consumer) Shutdown() error {
	// will close() the deliveries channel

	defer log.Printf("AMQP consumer [%q] shutdown OK", c.tag)

	if err := c.channel.Cancel(c.tag, true); err != nil {
		return fmt.Errorf("Consumer cancel failed: %s", err)
	}

	return nil
}

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

type FcgiWorker struct {
	amqpChannel *amqp.Channel
	fcgiHost    string
	fcgiTimeout time.Duration
	fcgiParams  map[string]string
	confirms    <-chan amqp.Confirmation
}

func NewFcgiWorker(amqpChannel *amqp.Channel, fcgiHost string, fcgiTimeout time.Duration, fcgiParams map[string]string) *FcgiWorker {

	return &FcgiWorker{
		amqpChannel: amqpChannel,
		fcgiHost:    fcgiHost,
		fcgiTimeout: fcgiTimeout,
		fcgiParams:  fcgiParams,
		confirms:    amqpChannel.NotifyPublish(make(chan amqp.Confirmation, 1)),
	}

}

func (w *FcgiWorker) work(delivery amqp.Delivery) error {

	skipReply := delivery.CorrelationId == "" || delivery.ReplyTo == ""

	/*
	  log.Printf(
	    "tag: %v, CorrelationId: %q, ReplyTo: %q, skipReply: %t",
	    delivery.DeliveryTag,
	    delivery.CorrelationId,
	    delivery.ReplyTo,
	    skipReply,
	  )
	*/

	// check for valid input from amqp
	var deliveryJson map[string]interface{}
	err := json.Unmarshal(delivery.Body, &deliveryJson)

	if err != nil {

		err = fmt.Errorf("Amqp message body not valid: %s", string(delivery.Body))

		if skipReply {
			// don't publish error when reply skipped
			log.Println(err)
			delivery.Nack(false, false)
			return nil
		}

		return w.publishReplyError(&delivery, err)

	}

	// TODO ? handle errors when fcgi.max_children reached, looks like don't need it because of fcgi internal queue

	fcgi, err := fcgiclient.DialTimeout("tcp", w.fcgiHost, w.fcgiTimeout)

	if err != nil {

		err = fmt.Errorf("FCGI connection failed: %s", err)

		if skipReply {
			// don't publish error when reply skipped
			log.Println(err)
			delivery.Nack(false, false)
			return nil
		}

		return w.publishReplyError(&delivery, err)
	}

	defer fcgi.Close()

	fcgiParams := make(map[string]string)

	// copy fcgi params from options
	for k, v := range w.fcgiParams {
		fcgiParams[k] = v
	}

	//log.Printf("fcgiParams: %q", fcgiParams)

	// TODO error if routingKey is not set

	/*
	 * {
	 *    routing_key: delivery.RoutingKey,
	 *    app_id: delivery.AppId,
	 *    body: {json part of delivery.Body}
	 * }
	 */

	body := make(map[string]interface{})
	body["routing_key"] = delivery.RoutingKey
	body["app_id"] = delivery.AppId
	body["body"] = deliveryJson
	bodyJson, err := json.Marshal(body)

	rd := bytes.NewReader(bodyJson)
	resp, err := fcgi.Post(fcgiParams, "application/x-json", rd, rd.Len())

	if err != nil {
		// get this error when fcgi script doesn't exist
		//log.Println(err.Error() == "malformed MIME header line: Primary script unknown")

		err = fmt.Errorf("FCGI script failed: %s", err)

		if skipReply {
			// don't publish error when reply skipped
			log.Println(err)
			delivery.Nack(false, false)
			return nil
		}

		return w.publishReplyError(&delivery, err)

	}

	defer resp.Body.Close()

	content, err := ioutil.ReadAll(resp.Body)

	if err != nil {

		err = fmt.Errorf("FCGI error: %s", err)

		if skipReply {
			// don't publish error when reply skipped
			log.Println(err)
			delivery.Nack(false, false)
			return nil
		}

		return w.publishReplyError(&delivery, err)

	}

	var response map[string]interface{}
	err = json.Unmarshal(content, &response)

	if err != nil {

		err = fmt.Errorf("FCGI response not valid: %s", string(content))

		if skipReply {
			// don't publish error when reply skipped
			log.Println(err)
			delivery.Nack(false, false)
			return nil
		}

		return w.publishReplyError(&delivery, err)

	}

	//log.Printf("%q %q", string(content), resp.Header["Content-type"])
	//string(content)

	//log.Printf("%q", string(content))

	/*
	  for k, v := range resp.Header {
	    log.Printf("%q\t:\t%q", k, v)
	  }
	*/

	// func (me *Channel) Publish(exchange, key string, mandatory, immediate bool, msg Publishing) error

	if skipReply {
		// skip reply, just get success result from fcgi and ack the message
		// TODO do we need to log errors here or publish to separate queue?
		delivery.Ack(false)
		return nil
	}

	// proceed with publishing result from fcgi to amqp reply

	return w.publishReply(&delivery, content, true)

}

func (w *FcgiWorker) publishReplyError(delivery *amqp.Delivery, err error) error {

	log.Println(fmt.Sprintf("Proxy: %s", err))
	body := NewErrorMessage(fmt.Sprintf("Proxy: %s", err))
	//log.Printf("bodyJson: %q", body);
	bodyJson, _ := json.Marshal(body)
	//log.Printf("bodyJson: %q", bodyJson);

	return w.publishReply(delivery, bodyJson, false)
}

func (w *FcgiWorker) publishReply(delivery *amqp.Delivery, body []byte, ack bool) error {

	msg := amqp.Publishing{
		CorrelationId: delivery.CorrelationId,
		ContentType:   "application/x-json",
		Body:          body,
	}

	var confirmed amqp.Confirmation

	//log.Printf("Publishing %q", delivery.CorrelationId)
	//confirms := w.amqpChannel.NotifyPublish(make(chan amqp.Confirmation, 1))

	w.amqpChannel.Publish("", delivery.ReplyTo, false, false, msg)

	if confirmed = <-w.confirms; confirmed.Ack {
		//log.Printf("Published to AMQP, %q, %d", delivery.CorrelationId, confirmed.DeliveryTag)
	} else {
		//log.Printf("Not published to AMQP, %q, %d", delivery.CorrelationId, confirmed.DeliveryTag)
	}

	//log.Printf("%q", w.amqpChannel)
	//log.Printf("--------------------------")

	//time.Sleep(time.Second*10)

	//log.Printf("[%v] acking %q", delivery.DeliveryTag, delivery.CorrelationId)

	if ack {
		delivery.Ack(false)
	} else {
		delivery.Nack(false, false)
	}

	return nil

}

func testSmth() {

}
