package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/streadway/amqp"
	"github.com/tomasen/fcgi_client"
	"io/ioutil"
	"log"
	"time"
)

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
