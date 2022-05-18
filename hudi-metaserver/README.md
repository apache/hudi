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
If you update the `hudi-metaserver.thrift`, you have to recompile it to bring the new thrift file into effect. 

Firstly, make sure `/usr/local/bin/thrift` exists.
Then compile the module with maven options `-Pthrift`.
Finally, the source code of thrift file generates and it is under the `src/main/thrift/gen-java`.
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
└── hudi-metaserver.thrift

```

# How to run a job with Hudi meta server

### Start Hudi meta server
1. modify the `hikariPool.properties` and config the mysql address. For example,
```text
jdbcUrl=jdbc:mysql://localhost:3306
dataSource.user=root
dataSource.password=password
```
2. start the server

   make sure `hudi-metaserver-${project.version}.jar` is under the directory,
```shell
sh start-hudi-metaserver.sh
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

### Run a unit test with hudi meta server
Add the configurations mentioned in the `Write client configurations` part, and
set `hoodie.metaserver.uris=""`.

The meta server runs as an embedded one with h2 database that performs like mysql running locally.



