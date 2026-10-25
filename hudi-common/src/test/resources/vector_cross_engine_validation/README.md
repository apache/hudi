
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

# Generation of test files

Spark3.5 is used to generate these test files.

Table schema:
* MOR table
```sql
CREATE TABLE vector_table_mor (
    id BIGINT,
    name STRING,
    embedding1 VECTOR(128) COMMENT 'document float embedding',
    embedding2 VECTOR(128, DOUBLE) COMMENT 'document double embedding',
    embedding3 VECTOR(128, INT8) COMMENT 'document INT8 embedding',
    ts BIGINT
) USING hudi
LOCATION '/tmp/hudi_vector_table_mor'
TBLPROPERTIES (
    primaryKey = 'id',
    preCombineField = 'ts',
    type = 'mor',
    hoodie.index.type = 'INMEMORY'
);
```

* COW table
```sql
CREATE TABLE vector_table_cow (
    id BIGINT,
    name STRING,
    embedding1 VECTOR(128) COMMENT 'document float embedding',
    embedding2 VECTOR(128, DOUBLE) COMMENT 'document double embedding',
    embedding3 VECTOR(128, INT8) COMMENT 'document INT8 embedding',
    ts BIGINT
) USING hudi
LOCATION '/tmp/hudi_vector_table_mor'
TBLPROPERTIES (
    primaryKey = 'id',
    preCombineField = 'ts',
    type = 'cow'
);
```

The shell commands used to generate this are:

```shell
cd /path/to/test/files/
zip -r $TABLE_DIR_NAME.zip $TABLE_DIR_NAME
```
