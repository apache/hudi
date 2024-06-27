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

When new config classes are added, or existing ones are moved to a separate module, please add the corresponding bundle 
for configurations of that module to the script [`generate_configs.sh`](generate_config.sh) to be picked up.
Set the appropriate version (either a snapshot version like `0.14.0-SNAPSHOT` or a public release version
like `0.13.0`) in the script [`generate_configs.sh`](generate_config.sh) and run it to automatically generate the
configuration documentation.

Once complete, please put up a patch with the latest configurations.