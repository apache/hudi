
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

echo "Copying spark default config and setting up configs"
cp /var/hoodie/ws/docker/demo/config/spark-defaults.conf $SPARK_CONF_DIR/.
cp /var/hoodie/ws/docker/demo/config/log4j2.properties $SPARK_CONF_DIR/.
hadoop fs -mkdir -p /var/demo/
hadoop fs -mkdir -p /tmp/spark-events
hadoop fs -copyFromLocal  -f /var/hoodie/ws/docker/demo/config /var/demo/.
chmod +x /var/hoodie/ws/hudi-sync/hudi-hive-sync/run_sync_tool.sh
