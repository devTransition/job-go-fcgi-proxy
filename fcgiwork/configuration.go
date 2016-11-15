package fcgiwork

import (
	"fmt"
	"log"

	"github.com/codegangsta/cli"
	"github.com/devTransition/job-go-fcgi-proxy/proxy"
	"github.com/spf13/viper"
	//"reflect"
)

/*
func (s *Configuration) FromMap(m map[string]interface{}) error {
	for k, v := range m {
		err := util.SetField(s, k, v)
		if err != nil {
			return err
		}
	}
	return nil
}
*/

func FillServiceConfigFromCli(config *proxy.ServiceConfig, c *cli.Context) {

	brokerConfig := proxy.BrokerConfig{
		Id:       "broker",
		Type:     "rabbitmq",
		Host:     c.String("amqp-host"),
		User:     c.String("amqp-user"),
		Password: c.String("amqp-password"),
	}

	brokers := map[string]proxy.BrokerConfig{}
	brokers[brokerConfig.Id] = brokerConfig
	config.Brokers = brokers

	workerConfig := FcgiWorkerConfig{
		Id:             "worker",
		Type:           "fastcgi",
		Host:           c.String("fcgi-host"),
		Timeout:        c.Int("fcgi-timeout"),
		ServerProtocol: c.String("fcgi-server-protocol"),
		ScriptName:     c.String("fcgi-script-name"),
		ScriptFilename: c.String("fcgi-script-filename"),
		RequestUri:     c.String("fcgi-request-uri"),
	}

	workers := map[string]proxy.WorkerConfig{}
	workers[workerConfig.GetId()] = &workerConfig
	config.Workers = workers

	routeConfig := proxy.RouteConfig{
		Name:          c.String("instance-name"),
		Broker:        brokerConfig.Id,
		Worker:        workerConfig.GetId(),
		Queue:         c.String("amqp-queue"),
		PrefetchCount: c.Int("amqp-prefetch-count"),
	}

	config.Routes = append(config.Routes, routeConfig)

}

func FillServiceConfigFromFile(config *proxy.ServiceConfig, filename string) error {

	viper.SetConfigName(filename)
	viper.AddConfigPath(".") // look in the working dir

	err := viper.ReadInConfig()

	if err != nil {
		return fmt.Errorf("Error when loading config: %s", err)
	}

	//log.Println(viper.Get("Routes"))

	_brokers := []proxy.BrokerConfig{}
	err = viper.UnmarshalKey("Brokers", &_brokers)

	if err != nil {
		return fmt.Errorf("Error when parsing Brokers: %v", err)
	}

	brokers := map[string]proxy.BrokerConfig{}
	for _, broker := range _brokers {
		brokers[broker.Id] = broker
	}

	log.Println("")
	config.Brokers = brokers

	_workers := []FcgiWorkerConfig{}
	err = viper.UnmarshalKey("Workers", &_workers)

	if err != nil {
		return fmt.Errorf("Error when parsing Workers: %v", err)
	}

	workers := map[string]proxy.WorkerConfig{}
	for _, worker := range _workers {
		workers[worker.GetId()] = &worker
	}

	config.Workers = workers

	err = viper.UnmarshalKey("Routes", &config.Routes)

	if err != nil {
		return fmt.Errorf("Error when parsing Routes: %v", err)
	}

	for _, route := range config.Routes {

		_, hasBroker := brokers[route.Broker]
		_, hasWorker := workers[route.Worker]

		if !hasWorker || !hasBroker {
			return fmt.Errorf("Config for route %s is not valid", route.Name)
		}

	}

	return nil

}
