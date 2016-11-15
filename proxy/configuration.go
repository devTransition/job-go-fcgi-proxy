package proxy

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

type WorkerConfig interface {
	GetId() string
	GetType() string
	CreateWorker(*RouteConfig) Worker
}

type RouteConfig struct {
	Name          string
	Broker        string
	Worker        string
	Queue         string
	PrefetchCount int
}
