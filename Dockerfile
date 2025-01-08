#基于哪个镜像
FROM openresty/openresty:1.19.3.1-0-alpine

RUN apk add --no-cache perl-dev curl gcc g++ make autoconf automake libtool tar
RUN mkdir -p /data/proj/apiserver
WORKDIR /data/proj/apiserver
COPY libmaxminddb-1.1.2.tar.gz /data/proj/apiserver/
RUN ls -la /data/proj/apiserver/
RUN tar -xzvf /data/proj/apiserver/libmaxminddb-1.1.2.tar.gz -C /data/proj/apiserver/
RUN ls -la /data/proj/apiserver/libmaxminddb-1.1.2

# 构建和安装 libmaxminddb
RUN cd /data/proj/apiserver/libmaxminddb-1.1.2 && ./configure && make && make install

RUN opm get anjia0532/lua-resty-maxminddb

EXPOSE 8899
