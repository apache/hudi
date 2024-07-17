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

PLATFORM_TYPE_ARG=$(uname -m)

docker build base -t apachehudi/hudi-hadoop_3.3.5-base:latest --build-arg PLATFORM_TYPE=$PLATFORM_TYPE_ARG --push
docker build namenode -t apachehudi/hudi-hadoop_3.3.5-namenode:latest --build-arg PLATFORM_TYPE=$PLATFORM_TYPE_ARG --push
docker build datanode -t apachehudi/hudi-hadoop_3.3.5-datanode:latest --build-arg PLATFORM_TYPE=$PLATFORM_TYPE_ARG --push
docker build historyserver -t apachehudi/hudi-hadoop_3.3.5-history:latest --build-arg PLATFORM_TYPE=$PLATFORM_TYPE_ARG --push

docker build hive_base -t apachehudi/hudi-hadoop_3.3.5-hive_2.3.3:latest --build-arg PLATFORM_TYPE=$PLATFORM_TYPE_ARG --push

docker build spark_base -t apachehudi/hudi-hadoop_3.3.5-hive_2.3.3-sparkbase_3.5.1:latest --build-arg PLATFORM_TYPE=$PLATFORM_TYPE_ARG --push
docker build sparkmaster -t apachehudi/hudi-hadoop_3.3.5-hive_2.3.3-sparkmaster_3.5.1:latest --build-arg PLATFORM_TYPE=$PLATFORM_TYPE_ARG --push
docker build sparkadhoc -t apachehudi/hudi-hadoop_3.3.5-hive_2.3.3-sparkadhoc_3.5.1:latest --build-arg PLATFORM_TYPE=$PLATFORM_TYPE_ARG --push
docker build sparkworker -t apachehudi/hudi-hadoop_3.3.5-hive_2.3.3-sparkworker_3.5.1:latest --build-arg PLATFORM_TYPE=$PLATFORM_TYPE_ARG --push


docker build prestobase -t apachehudi/hudi-hadoop_3.3.5-prestobase_0.288:latest --build-arg PLATFORM_TYPE=$PLATFORM_TYPE_ARG --push

docker build base_java11 -t apachehudi/hudi-hadoop_3.3.5-base-java11:latest --build-arg PLATFORM_TYPE=$PLATFORM_TYPE_ARG --push
docker build trinobase -t apachehudi/hudi-hadoop_3.3.5-trinobase_368:latest --build-arg PLATFORM_TYPE=$PLATFORM_TYPE_ARG --push
docker build trinocoordinator -t apachehudi/hudi-hadoop_3.3.5-trinocoordinator_368:latest --build-arg PLATFORM_TYPE=$PLATFORM_TYPE_ARG --push
docker build trinoworker -t apachehudi/hudi-hadoop_3.3.5-trinoworker_368:latest --build-arg PLATFORM_TYPE=$PLATFORM_TYPE_ARG --push