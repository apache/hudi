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
/bin/bash --version
shopt -s globstar
echo "Jacoco CLI jar: $1"
echo "Hudi source directory: $2"
retry_count=0
while [[ $retry_count -lt 3 ]]; do
  ls -l $2/**/jacoco-agent/**/*.exec
  java -jar $1 merge $2/**/jacoco-agent/**/*.exec --destfile merged-jacoco.exec
  exit_status=$?

  if [[ $exit_status -eq 0 ]]; then
    echo "Jacoco merge succeeded on attempt $((retry_count + 1))"
    exit 0
  fi

  retry_count=$((retry_count + 1))
  sleep 10
done
