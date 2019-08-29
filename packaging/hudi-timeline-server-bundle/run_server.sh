#!/usr/bin/env bash
################################################################################
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
################################################################################

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
#Ensure we pick the right jar even for hive11 builds
HOODIE_JAR=`ls -c $DIR/target/hudi-timeline-*.jar | grep -v test | head -1`

if [ -z "$HADOOP_CONF_DIR" ]; then
  echo "setting hadoop conf dir"
  HADOOP_CONF_DIR="/etc/hadoop/conf"
fi

OTHER_JARS=`ls -1 $DIR/target/lib/*jar | grep -v '*avro*-1.' | tr '\n' ':'`
echo "Running command : java -cp $DIR/target/:${HADOOP_CONF_DIR}:$HOODIE_JAR:$OTHER_JARS org.apache.hudi.timeline.service.TimelineService $@"
java -Xmx4G -cp $DIR/target/test-classes/:${HADOOP_CONF_DIR}:$HOODIE_JAR:$OTHER_JARS org.apache.hudi.timeline.service.TimelineService "$@"
