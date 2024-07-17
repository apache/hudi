#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.

FROM scratch
MAINTAINER Hoodie
USER root

ARG TARGETARCH

# Default to UTF-8 file.encoding
ENV LANG C.UTF-8


FROM arm64v8/zookeeper:3.4.12 AS platform-arm64

FROM bitnami/zookeeper:3.4.12-r68 AS platform-amd64

FROM platform-${TARGETARCH} AS final


