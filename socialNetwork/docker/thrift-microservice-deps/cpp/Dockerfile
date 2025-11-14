FROM ubuntu:16.04

ARG LIB_MONGOC_VERSION=1.15.0
ARG LIB_THRIFT_VERSION=0.12.0
ARG LIB_JSON_VERSION=3.6.1
ARG LIB_JAEGER_VERSION=0.4.2
ARG LIB_YAML_VERSION=0.6.2
ARG LIB_OPENTRACING_VERSION=1.5.1
ARG LIB_CPP_JWT_VERSION=1.1.1
ARG LIB_CPP_REDIS_VERSION=4.3.1
ARG LIB_AMQP_CPP_VERSION=4.1.4
ARG LIB_SIMPLEAMQPCLIENT_VERSION=2.4.0
ARG LIB_HIREDIS_VERSION=1.0.0
ARG LIB_REDIS_PLUS_PLUS_VERSION=1.2.3

ARG BUILD_DEPS="ca-certificates g++ cmake wget git libmemcached-dev automake bison flex libboost-all-dev libevent-dev libssl-dev libtool make pkg-config librabbitmq-dev python3-dev python3-pip python3-setuptools python3-wheel"

RUN apt-get update \
  && apt-get install -y ${BUILD_DEPS} --no-install-recommends \
  # Install mongo-c-driver
  && cd /tmp \
  && wget https://github.com/mongodb/mongo-c-driver/releases/download/1.15.0/mongo-c-driver-1.15.0.tar.gz \
  && tar -zxf mongo-c-driver-1.15.0.tar.gz \
  && cd mongo-c-driver-1.15.0 \
  && mkdir -p cmake-build \
  && cd cmake-build \
  && cmake -DCMAKE_BUILD_TYPE=Release -DENABLE_TESTS=0 -DENABLE_EXAMPLES=0 .. \
  && make -j$(nproc) \
  && make install \
  && cd /tmp \
  # Install lib-thrift
  && wget -O thrift-0.12.0.tar.gz https://github.com/apache/thrift/archive/v0.12.0.tar.gz \
  && tar -zxf thrift-0.12.0.tar.gz \
  && cd thrift-0.12.0 \
  && mkdir -p cmake-build \
  && cd cmake-build \
  && cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=0 .. \
  && make -j$(nproc) \
  && make install \
  && cd /tmp \
  # Install /nlohmann/json
  && wget -O json-3.6.1.tar.gz https://github.com/nlohmann/json/archive/v3.6.1.tar.gz \
  && tar -zxf json-3.6.1.tar.gz \
  && cd json-3.6.1 \
  && mkdir -p cmake-build \
  && cd cmake-build \
  && cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=0 .. \
  && make -j$(nproc) \
  && make install \
  && cd /tmp \
  # Install yaml-cpp
  && wget -O yaml-cpp-0.6.2.tar.gz https://github.com/jbeder/yaml-cpp/archive/yaml-cpp-0.6.2.tar.gz \
  && tar -zxf yaml-cpp-0.6.2.tar.gz \
  && cd yaml-cpp-yaml-cpp-0.6.2 \
  && mkdir -p cmake-build \
  && cd cmake-build \
  && cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-fPIC" -DYAML_CPP_BUILD_TESTS=0 .. \
  && make -j$(nproc) \
  && make install \
  && cd /tmp \
  # Install opentracing-cpp
  && wget -O opentracing-cpp-1.5.1.tar.gz https://github.com/opentracing/opentracing-cpp/archive/v1.5.1.tar.gz \
  && tar -zxf opentracing-cpp-1.5.1.tar.gz \
  && cd opentracing-cpp-1.5.1 \
  && mkdir -p cmake-build \
  && cd cmake-build \
  && cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-fPIC" -DBUILD_TESTING=0 .. \
  && make -j$(nproc) \
  && make install \
  && cd /tmp \
  # Install jaeger-client-cpp
  && wget -O jaeger-client-cpp-0.4.2.tar.gz https://github.com/jaegertracing/jaeger-client-cpp/archive/v0.4.2.tar.gz \
  && tar -zxf jaeger-client-cpp-0.4.2.tar.gz \
  && cd jaeger-client-cpp-0.4.2 \
  && mkdir -p cmake-build \
  && cd cmake-build \
  && cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-fPIC" -DHUNTER_ENABLED=0 -DBUILD_TESTING=0 -DJAEGERTRACING_WITH_YAML_CPP=1 -DJAEGERTRACING_BUILD_EXAMPLES=0 .. \
  && make -j$(nproc) \
  && make install \
  && cd /tmp \
  # Install jwt
  && wget -O cpp-jwt-1.1.1.tar.gz https://github.com/arun11299/cpp-jwt/archive/v1.1.1.tar.gz \
  && tar -zxf cpp-jwt-1.1.1.tar.gz \
  && cd cpp-jwt-1.1.1 \
  && cp -R include/jwt /usr/local/include \
  # use the dependency in /usr/local/include instead of in jwt/json
  && rm -rf /usr/local/include/jwt/json \
  && sed -i 's/\#include \"jwt\/json\/json.hpp\"/\#include \<nlohmann\/json\.hpp\>/g' /usr/local/include/jwt/jwt.hpp \
  # Install cpp_redis
  && cd /tmp \
  && git clone https://github.com/cpp-redis/cpp_redis.git \
  && cd cpp_redis && git checkout ${LIB_CPP_REDIS_VERSION} \
  && git submodule init && git submodule update \
  && mkdir cmake-build && cd cmake-build \
  && cmake .. -DCMAKE_BUILD_TYPE=Release \
  && make -j$(nproc) \
  && make install \
  # Install AMQP-CPP
  && cd /tmp \
  && git clone https://github.com/CopernicaMarketingSoftware/AMQP-CPP.git \
  && cd AMQP-CPP && git checkout v${LIB_AMQP_CPP_VERSION} \
  && mkdir cmake-build && cd cmake-build \
  && cmake .. -DCMAKE_BUILD_TYPE=Release -DAMQP-CPP_BUILD_SHARED=on -DAMQP-CPP_LINUX_TCP=on \
  && make -j$(nproc) && make install \
  # Install SimpleAmqpClient
  && cd /tmp \
  && git clone https://github.com/alanxz/SimpleAmqpClient.git \
  && cd SimpleAmqpClient \
  && git checkout v${LIB_SIMPLEAMQPCLIENT_VERSION} \
  && mkdir cmake-build && cd cmake-build \
  && cmake .. -DCMAKE_BUILD_TYPE=Release \
  && make -j$(nproc) \
  && make install \
  # Install Hiredis
  && cd /tmp \
  && git clone https://github.com/redis/hiredis.git \
  && cd hiredis \
  && git checkout v${LIB_HIREDIS_VERSION} \
  && make -j$(nproc) USE_SSL=1 \
  && make USE_SSL=1 install \
  # Install Redis plus plus
  && cd /tmp \
  && git clone https://github.com/sewenew/redis-plus-plus.git \
  && cd redis-plus-plus \
  && git checkout ${LIB_REDIS_PLUS_PLUS_VERSION} \
  && sed -i '/Transaction transaction/i\\    ShardsPool* get_shards_pool(){\n        return &_pool;\n    }\n' \
     src/sw/redis++/redis_cluster.h \
  && cmake -DREDIS_PLUS_PLUS_USE_TLS=ON . \
  && make -j$(nproc) \
  && make install \
  # Install pyyaml
  && pip3 install PyYAML \
  # Clean up
  && cd /tmp \
  && rm -rf \
    mongo-c-driver-1.15.0.tar.gz \
    mongo-c-driver-1.15.0 \
    thrift-0.12.0.tar.gz \
    thrift-0.12.0 \
    json-3.6.1.tar.gz \
    json-3.6.1 \
    jaeger-client-cpp-0.4.2.tar.gz \
    jaeger-client-cpp-0.4.2 \
    yaml-cpp-0.6.2.tar.gz \
    yaml-cpp-yaml-cpp-0.6.2 \
    opentracing-cpp-1.5.1.tar.gz \
    opentracing-cpp-1.5.1 \
    cpp-jwt-1.1.1.tar.gz \
    cpp-jwt-1.1.1 \
    cpp_redis \
    AMQP-CPP \
    SimpleAmqpClient

ENV LD_LIBRARY_PATH /usr/local/lib:${LD_LIBRARY_PATH}
RUN ldconfig
