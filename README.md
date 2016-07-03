# Go amqp-fcgi proxy 

Service connects to amqp server and consumes messages from specified queue to run php workers using FastCGI connection.

Sample yaml configuration (json supported too):

```yaml
# descibe amqp connections, if no routes using broker, the connection won't be created
Brokers:
 -
     Id: "main"
     Type: "rabbitmq"
     Host: "192.168.99.100"
     User: "guest"
     Password: "guest"
     

# describe php workers
Workers:
 -
     Id: "mainworker"
     Type: "fastcgi"
     Host: "192.168.99.100:9000"
     Timeout: 5
     ServerProtocol: "HTTP/1.1"
     ScriptName: "/app/process.php"
     ScriptFilename: "/app/process.php"
     RequestUri: "/app/process.php/job/process"

# descibe the routes, define amqp connection with queue to consume from and php worker to process messages 
Routes:
 -
     Name: "rpc-main" # NOTE! domain-like unique name, used as SERVER_NAME fcgi parameter 
     Broker: "main"
     Worker: "mainworker"
     Queue: "rpc_queue"
     PrefetchCount: 5
```

Notes: 
  
  - If amqp delivery contains `correlationId` and `replyTo` fields, the result/error recieved from php worker will be published to defined queue.
  - Ampq delivery is acked/nacked after worker job has been finished, result if any have been recieved and reply if defined have been sent
  - Scenario when reply have been published and connection is reset-by-peer before ack/nack is possible. Delivery will be returned to the queue. 


## run RabbitMQ

    docker run -d --hostname my-rabbit --name some-rabbit -p 15672:15672 -p 5672:5672 rabbitmq:3-management

## configure queue and add some messages
See [job-benchmark-tools](https://github.com/devTransition/job-benchmark-tools)

    node benchmark.js -h 192.168.99.100 -t 1900 -D 10..3000 -S 1111..30000 -w 1 -c 10 -l 10

## run FastCGI server

    docker run -d --name php-rabbit-test -p 9000:9000 -v /path/to/dir/with/php-script:/app php:fpm

## install and configure
Project is configured to use [glide package manager](https://github.com/Masterminds/glide), when running commands add `export GO15VENDOREXPERIMENT=1;`
    
    glide install; glide rebuild
    
## run proxy

### single-route config:
    
    go run main.go -amqp-host 192.168.99.100 -fcgi-host 192.168.99.100:9000 -lifetime=0 -fcgi-script-filename="/app/process.php" -amqp-prefetch-count 100

### use config from file to run multiple routes:
Supported config formats: JSON, TOML, YAML and HCL, see [https://github.com/spf13/viper](https://github.com/spf13/viper) for more
    
    go run main.go -config="config-json"
    
## TO-DO
https://github.com/pquerna/ffjson

Create proxy docker container with https://github.com/progrium/entrykit

## FastCGI docs
https://easyengine.io/tutorials/php/directly-connect-php-fpm/

