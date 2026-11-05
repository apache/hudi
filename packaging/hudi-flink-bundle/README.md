<!--
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
-->

# `hudi-flink-bundle` module

This module packages the Hudi Flink bundle for reading and writing Hudi tables with Flink.

## Lance dependencies

The Hudi Flink bundle does not include Lance runtime dependencies. To use Lance with Hudi,
ensure that the Flink runtime includes the following dependencies and their required transitive
dependencies. Use versions compatible with the dependencies of your Hudi version; see its
[root POM](../../pom.xml) for the Lance and Arrow versions.

- `org.lance:lance-core`
- `org.apache.arrow:arrow-vector`
- `org.apache.arrow:arrow-format`
- `org.apache.arrow:arrow-memory-core`
- `org.apache.arrow:arrow-memory-netty`
- `org.apache.arrow:arrow-memory-netty-buffer-patch`
- `org.apache.arrow:arrow-c-data`
- `org.questdb:jar-jni`
- `com.google.flatbuffers:flatbuffers-java`
- `io.netty:netty-buffer`
- `io.netty:netty-common`
