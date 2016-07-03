package proxy

import (
	"fmt"
	"github.com/codegangsta/cli"
	"github.com/spf13/viper"
	"log"
	//"github.com/devTransition/job-go-fcgi-proxy/util"
	//"reflect"
)

type Configuration struct {
	InstanceName string

	AmqpHost          string
	AmqpUser          string
	AmqpPassword      string
	AmqpQueue         string
	AmqpPrefetchCount uint

	FcgiHost           string
	FcgiTimeout        uint
	FcgiServerProtocol string
	FcgiScriptName     string
	FcgiScriptFilename string
	FcgiRequestUri     string
}

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

type ServiceConfig struct {
	Brokers map[string]BrokerConfig
	Workers map[string]WorkerConfig
	Routes  []RouteConfig
}

type BrokerConfig struct {
	Id       string
	Type     string
	Host     string
	User     string
	Password string
}

type WorkerConfig struct {
	Id             string
	Type           string
	Host           string
	Timeout        int
	ServerProtocol string
	ScriptName     string
	ScriptFilename string
	RequestUri     string
}

type RouteConfig struct {
	Name          string
	Broker        string
	Worker        string
	Queue         string
	PrefetchCount int
}

func FillServiceConfigFromCli(config *ServiceConfig, c *cli.Context) {

	brokerConfig := BrokerConfig{
		Id:       "broker",
		Type:     "rabbitmq",
		Host:     c.String("amqp-host"),
		User:     c.String("amqp-user"),
		Password: c.String("amqp-password"),
	}

	brokers := map[string]BrokerConfig{}
	brokers[brokerConfig.Id] = brokerConfig
	config.Brokers = brokers

	workerConfig := WorkerConfig{
		Id:             "worker",
		Type:           "fastcgi",
		Host:           c.String("fcgi-host"),
		Timeout:        c.Int("fcgi-timeout"),
		ServerProtocol: c.String("fcgi-server-protocol"),
		ScriptName:     c.String("fcgi-script-name"),
		ScriptFilename: c.String("fcgi-script-filename"),
		RequestUri:     c.String("fcgi-request-uri"),
	}

	workers := map[string]WorkerConfig{}
	workers[workerConfig.Id] = workerConfig
	config.Workers = workers

	routeConfig := RouteConfig{
		Name:          c.String("instance-name"),
		Broker:        brokerConfig.Id,
		Worker:        workerConfig.Id,
		Queue:         c.String("amqp-queue"),
		PrefetchCount: c.Int("amqp-prefetch-count"),
	}

	config.Routes = append(config.Routes, routeConfig)

}

func FillServiceConfigFromFile(config *ServiceConfig, filename string) error {

	viper.SetConfigName(filename)
	viper.AddConfigPath(".") // look in the working dir

	err := viper.ReadInConfig()

	if err != nil {
		return fmt.Errorf("Error when loading config: %s", err)
	}

	//log.Println(viper.Get("Routes"))

	_brokers := []BrokerConfig{}
	err = viper.UnmarshalKey("Brokers", &_brokers)

	if err != nil {
		return fmt.Errorf("Error when parsing Brokers: %v", err)
	}

	brokers := map[string]BrokerConfig{}
	for _, broker := range _brokers {
		brokers[broker.Id] = broker
	}

	log.Println("")
	config.Brokers = brokers

	_workers := []WorkerConfig{}
	err = viper.UnmarshalKey("Workers", &_workers)

	if err != nil {
		return fmt.Errorf("Error when parsing Workers: %v", err)
	}

	workers := map[string]WorkerConfig{}
	for _, worker := range _workers {
		workers[worker.Id] = worker
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
