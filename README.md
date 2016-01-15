# Job processer demo tools

## run FastCGI server

    docker run -d --name php-rabbit-test -p 9000:9000 -v /path/to/dir/with/php-script:/app php:fpm

