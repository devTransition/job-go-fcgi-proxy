package proxy

type Configuration struct {
  InstanceName       string

  AmqpHost           string
  AmqpUser           string
  AmqpPassword       string
  AmqpQueue          string
  AmqpPrefetchCount  int

  FcgiHost           string
  FcgiTimeout        int
  FcgiServerProtocol string
  FcgiScriptName     string
  FcgiScriptFilename string
  FcgiRequestUri     string
}
