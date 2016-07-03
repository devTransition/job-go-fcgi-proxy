package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/streadway/amqp"
	"github.com/tomasen/fcgi_client"
	"io/ioutil"
	"log"
	"strings"
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
	fcgiHost    string
	fcgiTimeout time.Duration
	fcgiParams  *map[string]string
}

func NewFcgiWorker(fcgiHost string, fcgiTimeout time.Duration, fcgiParams *map[string]string) *FcgiWorker {

	return &FcgiWorker{
		fcgiHost:    fcgiHost,
		fcgiTimeout: fcgiTimeout,
		fcgiParams:  fcgiParams,
	}

}

func (w *FcgiWorker) work(delivery *amqp.Delivery) (result []byte, reply bool, err error) {

	reply = delivery.CorrelationId != "" && delivery.ReplyTo != ""

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
	err = json.Unmarshal(delivery.Body, &deliveryJson)

	if err != nil {

		err = fmt.Errorf("Amqp message body not valid: %s, %s", string(delivery.Body), err)
		return

	}

	// TODO ? handle errors when fcgi.max_children reached, looks like don't need it because of fcgi internal queue

	fcgi, err := fcgiclient.DialTimeout("tcp", w.fcgiHost, w.fcgiTimeout)

	if err != nil {

		err = fmt.Errorf("FCGI connection failed: %s", err)
		return
	}

	defer fcgi.Close()

	fcgiParams := make(map[string]string)

	// copy fcgi params from options
	for k, v := range *w.fcgiParams {
		fcgiParams[k] = v
	}

	//log.Printf("fcgiParams: %q", fcgiParams)

	// TODO error if routingKey is not set?

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

	// TODO debug
	log.Printf("fcgi body: %q", body)

	rd := bytes.NewReader(bodyJson)
	resp, err := fcgi.Post(fcgiParams, "application/x-json", rd, rd.Len())

	if resp != nil {
		defer resp.Body.Close()
	}

	if err != nil {
		// get this error when fcgi script doesn't exist
		//log.Println(err.Error() == "malformed MIME header line: Primary script unknown")
		err = fmt.Errorf("FCGI script failed: %s", err)
		return

	}

	result, err = ioutil.ReadAll(resp.Body)

	if err != nil {

		err = fmt.Errorf("FCGI error: %s", err)
		return

	}

	var response map[string]interface{}
	err = json.Unmarshal(result, &response)

	if err != nil {

		err = fmt.Errorf("FCGI response not valid: %s, %s", string(result), err)
		return

	}

	//log.Printf("%q %q", string(content), resp.Header["Content-type"])
	//log.Printf("%q", string(content))

	/*
	  for k, v := range resp.Header {
	    log.Printf("%q\t:\t%q", k, v)
	  }
	*/

	return

}

func CreateFcgiParams(config *RouteConfig, workerConfig *WorkerConfig) *map[string]string {

	fcgiHostAddr, fcgiHostPort := strings.Split(workerConfig.Host, ":")[0], strings.Split(workerConfig.Host, ":")[1]

	fcgiParams := make(map[string]string)

	fcgiParams["SERVER_PROTOCOL"] = workerConfig.ServerProtocol

	fcgiParams["SERVER_ADDR"] = fcgiHostAddr
	fcgiParams["SERVER_PORT"] = fcgiHostPort
	fcgiParams["SERVER_NAME"] = config.Name + ".job-go-fcgi-proxy.local"

	fcgiParams["REMOTE_ADDR"] = "127.0.0.1"
	fcgiParams["REMOTE_PORT"] = fcgiHostPort

	fcgiParams["SCRIPT_NAME"] = workerConfig.ScriptName
	fcgiParams["SCRIPT_FILENAME"] = workerConfig.ScriptFilename
	fcgiParams["REQUEST_URI"] = workerConfig.RequestUri

	return &fcgiParams
}
