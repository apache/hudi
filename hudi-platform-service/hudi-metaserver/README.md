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

# How to compile

There are two ways, if there is a thrift binary in the local, please see the first one.
If docker is more convenient for you, please see the other way.

### Way1: Compile with thrift binary

Firstly, make sure `/usr/local/bin/thrift` exists, and its version is the same with the one declared in the pom.xml.

Then compile the module with maven options `-Pthrift`.

### Way2: Compile with docker

Firstly, make sure there is a docker in the local.

Secondly run the script `sh hudi-platform-service/hudi-metaserver/bin/compile_thrift_by_docker.sh`.

Then the source code of thrift file generates and it is under the `target/generated-sources/thrift/gen-java`.
```shell
├── gen-java
│   └── org
│       └── apache
│           └── hudi
│               └── metaserver
│                   └── thrift
│                       ├── AlreadyExistException.java
│                       ├── FieldSchema.java
│                       ├── ...
```

After source code generates, just compile the module with common maven commands(exclude `mvn clean`).


# How to run a job with Hudi Metaserver

### Start Hudi Metaserver
1. modify the `hikariPool.properties` and config the mysql address. For example,
```text
jdbcUrl=jdbc:mysql://localhost:3306
dataSource.user=root
dataSource.password=password
```
2. start the server

   make sure `hudi-metaserver-${project.version}.jar` is under the directory,
```shell
sh start_hudi_metaserver.sh
```

### Write client configurations
```shell
hoodie.database.name=default
hoodie.table.name=test
hoodie.base.path=${path}
hoodie.metaserver.enabled=true
hoodie.metadata.enabled=false
hoodie.metaserver.uris=thrift://${serverIP}:9090
```

# How to test

### Run a unit test with Hudi Metaserver
Add the configurations mentioned in the `Write client configurations` part, and
set `hoodie.metaserver.uris=""`.

The meta server runs as an embedded one with h2 database that performs like mysql running locally.

