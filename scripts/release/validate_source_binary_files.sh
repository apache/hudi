#!/bin/bash

#
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
#

# fail immediately
set -o errexit
set -o nounset

echo "Checking for binary files in the source files"
numBinaryFiles=`find . -iname '*' | xargs -I {} file -I {} | grep -va directory | grep -v "release/" | grep -v "/src/test/" | grep -va 'application/json' | grep -va 'text/' | grep -va 'application/xml' | grep -va 'application/json' | wc -l | sed -e s'/ //g'`

if [ "$numBinaryFiles" -gt "0" ]; then
  echo -e "There were non-text files in source release. [ERROR]\n Please check below\n"
  find . -iname '*' | xargs -I {} file -I {} | grep -va directory | grep -v "release/release_guide" | grep -v "/src/test/" | grep -va 'application/json' | grep -va 'text/' |  grep -va 'application/xml'
  exit 1
fi
echo -e "\t\tNo Binary Files in the source files? - [OK]\n"
