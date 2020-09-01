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

SCRIPT_PATH=$(cd `dirname $0`; pwd)
# set up root directory
WS_ROOT=`dirname $SCRIPT_PATH`

echo "Preparing Presto server binary file."
PRESTO_VERSION=0.217
PRESTO_BINARY_FILE_NAME=presto-server-${PRESTO_VERSION}.tar.gz
PRESTO_BINARY_CACHE_FILE_PATH=$SCRIPT_PATH/binarycache/$PRESTO_BINARY_FILE_NAME

if [ -f $PRESTO_BINARY_CACHE_FILE_PATH ]; then
  echo "The binary file $PRESTO_BINARY_FILE_NAME has been cached in the binary cache directory!"
else
  echo "The binary file $PRESTO_BINARY_FILE_NAME did not exist in the binary cache directory, try to download."
  wget https://repo1.maven.org/maven2/com/facebook/presto/presto-server/${PRESTO_VERSION}/${PRESTO_BINARY_FILE_NAME} -O ${PRESTO_BINARY_CACHE_FILE_PATH}
fi
