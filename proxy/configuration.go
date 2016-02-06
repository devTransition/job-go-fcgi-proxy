package proxy

import (
  "github.com/spf13/viper"
  "fmt"
  //"log"
  //"github.com/devTransition/job-go-fcgi-proxy/util"
  //"reflect"
)

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

func FillConfigFromFile(config *[]Configuration, filename string) (error) {
  
  viper.SetConfigName(filename)
  viper.AddConfigPath(".") // look in the working dir
  
  err := viper.ReadInConfig()
  
  if err != nil {
    return fmt.Errorf("Error when loading config: %s", err)
  }
  
  //log.Println(viper.Get("Routes"))
  
  //cc := []Configuration{}
  
  err = viper.UnmarshalKey("Routes", config)
  
  if err != nil {
    return fmt.Errorf("Error when parsing config: %v", err)
  }
  
  return nil
  
} 