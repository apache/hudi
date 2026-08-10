#!/bin/bash

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

printf "====== INSTALL THRIFT START ======\n"
brew install thrift@0.13.0
printf "====== INSTALL THRIFT END ======\n"
printf "====== COMPILE THRIFT SOURCE FILE START ======\n"
PARENT_PATH=$(dirname "$PWD")
mkdir -p ${PARENT_PATH}/target/generated-sources
/usr/local/bin/thrift -o ${PARENT_PATH}/target/generated-sources --gen java ${PARENT_PATH}/src/main/thrift/hudi-metaserver.thrift
printf "====== COMPILE THRIFT SOURCE FILE END ======\n"