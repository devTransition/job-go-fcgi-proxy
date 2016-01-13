package main

import (
  "bytes"
  "log"
  "time"
  "sync"
  "os"
  "fmt"
  "io/ioutil"
  "github.com/codegangsta/cli"
  "github.com/streadway/amqp"
  "github.com/tomasen/fcgi_client"
)

func main() {
  app := cli.NewApp()
  app.Name = "amqp-fcgi"
  app.Usage = ""
  app.Flags = []cli.Flag{
    cli.StringFlag{Name: "amqp-host", Value:"127.0.0.1", Usage: "hostname (default: 127.0.0.1)"},
    cli.StringFlag{Name: "amqp-user", Value:"guest", Usage: "username (default: guest)"},
    cli.StringFlag{Name: "amqp-password", Value:"guest", Usage: "password (default: guest)"},
    cli.StringFlag{Name: "amqp-queue", Value:"rpc_queue", Usage: "name of queue (default: rpc_queue)"},
    cli.IntFlag{Name: "prefetch-count", Value: 0, Usage: "AMQP prefetch count (default: 0) "},
    cli.StringFlag{Name: "fcgi-host", Value:"127.0.0.1:9000", Usage: "hostname (default: 127.0.0.1:9000)"},
    //cli.StringFlag{Name: "ctag", Value:"simple-consumer", Usage: "unique tag for consumer (default: simple-consumer)"},
    cli.IntFlag{Name: "lifetime", Value: 0, Usage: "Number of seconds (default: 0, forever)"},
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

  uri := "amqp://" + c.String("amqp-user") + ":" + c.String("amqp-password")+ "@" + c.String("amqp-host")
  queue := c.String("amqp-queue")

  // TODO need to be generated UUID
  ctag := "simple-consumer"
  prefetchCount := c.Int("prefetch-count")
  fcgiHost := c.String("fcgi-host")
  lifetime := c.Int("lifetime")
  
  amqpConnection, err := NewAmqpConnection(uri, prefetchCount)
  if err != nil {
    log.Fatalf("%s", err)
  }

  consumer, err := NewConsumer(amqpConnection.channel, queue, ctag)

  if err != nil {
    log.Fatalf("%s", err)
  }

  worker := NewFcgiWorker(amqpConnection.channel, fcgiHost)

  dispatcher := NewDispatcher(consumer.delivery, worker)
  dispatcher.Run()

  wg.Add(1);

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

type AmqpConnection struct {
  conn    *amqp.Connection
  channel *amqp.Channel
}

func NewAmqpConnection(uri string, prefetchCount int) (*AmqpConnection, error) {

  c := &AmqpConnection{
    conn: nil,
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
    0, // prefetch size
    false, // global
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
    nil, // arguments
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
    worker: worker,
    done:    make(chan error),
  }
}

func (d *Dispatcher) Run() {
  go d.dispatch()
}

func (d *Dispatcher) dispatch() {

  var wg sync.WaitGroup

  for job := range d.jobQueue {

    log.Printf("DeliveryTag: %v", job.DeliveryTag)
    wg.Add(1)

    go func() {
      d.worker.work(job)
      wg.Done()
    }()

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
  confirms <-chan amqp.Confirmation
}

func NewFcgiWorker(amqpChannel *amqp.Channel, fcgiHost string) *FcgiWorker {

  return &FcgiWorker{
    amqpChannel: amqpChannel,
    fcgiHost: fcgiHost,
    confirms: amqpChannel.NotifyPublish(make(chan amqp.Confirmation, 1)),
  }

}

func (w *FcgiWorker) work(delivery amqp.Delivery) error {

  log.Printf(
    "CorrelationId: %q, ReplyTo: %q",
    delivery.CorrelationId,
    delivery.ReplyTo,
  )
  
  fcgi, err := fcgiclient.Dial("tcp", w.fcgiHost)
  
  if err != nil {
    fmt.Errorf("FCGI connection failed: %s", err)
    return nil
  }

  defer fcgi.Close()


  fcgiParams := make(map[string]string)
  fcgiParams["SERVER_PROTOCOL"] = "HTTP/1.1"
  fcgiParams["SCRIPT_NAME"] = "/app/process.php"
  fcgiParams["SCRIPT_FILENAME"] = "/app/process.php"

  rd := bytes.NewReader(delivery.Body)
  resp, err := fcgi.Post(fcgiParams, "application/x-json", rd, rd.Len())

  if err != nil {
    log.Println(err)
    return nil
  }

  defer resp.Body.Close()

  content, err := ioutil.ReadAll(resp.Body)

  //log.Printf("%q %q", string(content), resp.Header["Content-type"])
  //string(content)
  
  //log.Printf("%q", string(content))
  
  /*
  for k, v := range resp.Header {
    log.Printf("%q\t:\t%q", k, v)
  }
  */
  
  // func (me *Channel) Publish(exchange, key string, mandatory, immediate bool, msg Publishing) error
  msg := amqp.Publishing{
    CorrelationId: delivery.CorrelationId,
    ContentType:  "application/x-json",
    Body:         content,
  }
  
  var confirmed amqp.Confirmation
  
  log.Printf("Publishing %q", delivery.CorrelationId)
  //confirms := w.amqpChannel.NotifyPublish(make(chan amqp.Confirmation, 1))
  
  w.amqpChannel.Publish("", delivery.ReplyTo, false, false, msg)
  
  if delivery.CorrelationId == "a9b34bb8-f2e4-4602-b393-1f8a79885bf7" {
    //log.Printf("Pause %q", delivery.CorrelationId)
    //time.Sleep(time.Second * 10)
  }
  
  if confirmed = <-w.confirms; confirmed.Ack {
    log.Printf("Published to AMQP, %q, %d", delivery.CorrelationId, confirmed.DeliveryTag)
  } else {
    log.Printf("Not published to AMQP, %q, %d", delivery.CorrelationId, confirmed.DeliveryTag)
  }
  
  //log.Printf("%q", w.amqpChannel)
  log.Printf("--------------------------")
  
  //time.Sleep(time.Second*10)
  
  delivery.Ack(false)
  
  return nil

}

func testSmth() {
  
}