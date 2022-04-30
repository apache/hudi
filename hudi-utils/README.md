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

Execute these from hudi-utils dir <br/>
Ensure you have hudi artifacts from latest master installed <br/> 
If not, execute `mvn install -DskipTests` in your hudi repo  <br/>

```shell
mvn clean
mvn install
```
Set the appropriate SNAPSHOT version and execute the below commands
```shell
VERSION=0.12.0

JARS=(
"$HOME/.m2/repository/org/apache/hudi/hudi-utilities-bundle_2.11/$VERSION-SNAPSHOT/hudi-utilities-bundle_2.11-$VERSION-SNAPSHOT.jar"
"$HOME/.m2/repository/org/apache/hudi/hudi-spark2.4-bundle_2.11/$VERSION-SNAPSHOT/hudi-spark2.4-bundle_2.11-$VERSION-SNAPSHOT.jar"
"$HOME/.m2/repository/org/apache/hudi/hudi-flink1.14-bundle_2.11/$VERSION-SNAPSHOT/hudi-flink1.14-bundle_2.11-$VERSION-SNAPSHOT.jar"
"$HOME/.m2/repository/org/apache/hudi/hudi-kafka-connect-bundle/$VERSION-SNAPSHOT/hudi-kafka-connect-bundle-$VERSION-SNAPSHOT.jar"
"$HOME/.m2/repository/org/apache/hudi/hudi-datahub-sync-bundle/$VERSION-SNAPSHOT/hudi-datahub-sync-bundle-$VERSION-SNAPSHOT.jar"
"$HOME/.m2/repository/org/apache/hudi/hudi-gcp-bundle/$VERSION-SNAPSHOT/hudi-gcp-bundle-$VERSION-SNAPSHOT.jar"
)

printf -v CLASSPATH ':%s' "${JARS[@]}"
echo "CLASSPATH=$CLASSPATH"

java -cp target/hudi-utils-1.0-SNAPSHOT-jar-with-dependencies.jar$CLASSPATH \
org.apache.hudi.utils.HoodieConfigDocGenerator

cp /tmp/configurations.md ../website/docs/configurations.md
```

Once complete, please put up a patch with latest configurations.