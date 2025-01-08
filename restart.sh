#!/usr/bin/env sh

pid=`cat /data/proj/apiserver/logs/nginx.pid`
if [ -z $pid ]; then
    echo "nginx is not running"
else
    echo "nginx is running"
    kill -9 $pid
    echo kill -9 $pid
    sleep 1
    echo "nginx is killed"
fi

nginx -p /data/proj/apiserver -c /etc/nginx/nginx.conf