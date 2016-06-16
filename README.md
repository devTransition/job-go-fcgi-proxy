# Go amqp-fcgi proxy 
    
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

## FastCGI docs
https://easyengine.io/tutorials/php/directly-connect-php-fpm/

