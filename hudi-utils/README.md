<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

mvn clean
mvn install

java -cp target/hudi-utils-1.0-SNAPSHOT-jar-with-dependencies.jar:$HOME/.m2/repository/org/apache/hudi/hudi-utilities-bundle_2.11/0.10.0-SNAPSHOT/hudi-utilities-bundle_2.11-0.10.0-SNAPSHOT.jar:$HOME/.m2/repository/org/apache/hudi/hudi-spark-bundle_2.11/0.10.0-SNAPSHOT/hudi-spark-bundle_2.11-0.10.0-SNAPSHOT.jar:$HOME/.m2/repository/org/apache/hudi/hudi-flink-bundle_2.11/0.10.0-SNAPSHOT/hudi-flink-bundle_2.11-0.10.0-SNAPSHOT.jar org.apache.hudi.utils.HoodieConfigDocGenerator

cp /tmp/configurations.md $HUDI-DIR/website/docs/configurations.md 
