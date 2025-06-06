#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
## Create the require jars for the demo and copy them into a directory we'll mount in our notebook container

CURRENT_DIR="$( cd "$(dirname "$0")" ; pwd -P )"
XTABLE_HOME="$( cd "$(dirname "$CURRENT_DIR")" ; pwd -P )"
cd $XTABLE_HOME

mvn install -am -pl xtable-core -DskipTests -T 2
mkdir -p demo-s3/jars
cp xtable-hudi-support/xtable-hudi-support-utils/target/xtable-hudi-support-utils-0.1.0-SNAPSHOT.jar demo-s3/jars
cp xtable-api/target/xtable-api-0.1.0-SNAPSHOT.jar demo-s3/jars
cp xtable-core/target/xtable-core-0.1.0-SNAPSHOT.jar demo-s3/jars

cd demo-s3
docker-compose up
