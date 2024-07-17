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

#first time use the below command and also enable experimental under docker engine
#docker buildx create --name mybuilder --use
docker buildx use mybuilder

#docker buildx build base --platform linux/arm64,linux/amd64 -t apachehudi/hudi-hadoop_3.3.5-base:latest --push
docker buildx build namenode --platform linux/arm64,linux/amd64 -t apachehudi/hudi-hadoop_3.3.5-namenode:latest --push
docker buildx build datanode --platform linux/arm64,linux/amd64 -t apachehudi/hudi-hadoop_3.3.5-datanode:latest --push
docker buildx build historyserver --platform linux/arm64,linux/amd64 -t apachehudi/hudi-hadoop_3.3.5-history:latest --push

docker buildx build hive_base --platform linux/arm64,linux/amd64 -t apachehudi/hudi-hadoop_3.3.5-hive_2.3.3:latest --push

docker buildx build spark_base --platform linux/arm64,linux/amd64 -t apachehudi/hudi-hadoop_3.3.5-hive_2.3.3-sparkbase_3.5.1:latest --push
docker buildx build sparkmaster --platform linux/arm64,linux/amd64 -t apachehudi/hudi-hadoop_3.3.5-hive_2.3.3-sparkmaster_3.5.1:latest --push
docker buildx build sparkadhoc --platform linux/arm64,linux/amd64 -t apachehudi/hudi-hadoop_3.3.5-hive_2.3.3-sparkadhoc_3.5.1:latest --push
docker buildx build sparkworker --platform linux/arm64,linux/amd64 -t apachehudi/hudi-hadoop_3.3.5-hive_2.3.3-sparkworker_3.5.1:latest --push


docker buildx build prestobase --platform linux/arm64,linux/amd64 -t apachehudi/hudi-hadoop_3.3.5-prestobase_0.288:latest --push

docker buildx build base_java11 --platform linux/arm64,linux/amd64 -t apachehudi/hudi-hadoop_3.3.5-base-java11:latest --push
docker buildx build trinobase --platform linux/arm64,linux/amd64 -t apachehudi/hudi-hadoop_3.3.5-trinobase_368:latest --push
docker buildx build trinocoordinator --platform linux/arm64,linux/amd64 -t apachehudi/hudi-hadoop_3.3.5-trinocoordinator_368:latest --push
docker buildx build trinoworker --platform linux/arm64,linux/amd64 -t apachehudi/hudi-hadoop_3.3.5-trinoworker_368:latest --push

docker buildx build hive_metastore_postgresql --platform linux/arm64,linux/amd64 -t apachehudi/hive-metastore-postgresql:2.3.0 --push
docker buildx build zookeeper --platform linux/arm64,linux/amd64 -t apachehudi/zookeeper:3.4.12 --push
