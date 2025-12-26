# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
FROM --platform=linux/amd64 maven:3.9-eclipse-temurin-11-alpine

RUN apk add --no-cache --upgrade bash curl jq git

# Install thrift 0.13.0 from source (compiler only, skip library)
RUN apk add --no-cache libstdc++ \
    && apk add --no-cache --virtual .build-deps \
        autoconf \
        automake \
        bison \
        boost-dev \
        flex \
        g++ \
        libevent-dev \
        libtool \
        make \
        openssl-dev \
        zlib-dev \
    && curl -sSL https://archive.apache.org/dist/thrift/0.13.0/thrift-0.13.0.tar.gz | tar xz \
    && cd thrift-0.13.0 \
    && ./configure --prefix=/usr/local \
        --without-python \
        --without-py3 \
        --without-nodejs \
        --without-go \
        --without-ruby \
        --without-php \
        --without-erlang \
        --without-perl \
        --without-lua \
        --without-rs \
        --without-swift \
        --without-dotnetcore \
        --without-haskell \
        --without-d \
        --without-c_glib \
    && make -C compiler/cpp -j$(nproc) \
    && make -C compiler/cpp install \
    && cd .. \
    && rm -rf thrift-0.13.0 \
    && apk del .build-deps
