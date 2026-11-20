/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

SET 'table.dml-sync' = 'true';

CREATE TABLE t1
(
    uuid        VARCHAR(20) PRIMARY KEY NOT ENFORCED,
    name        VARCHAR(10),
    age         INT,
    ts          TIMESTAMP(3),
    `partition` VARCHAR(20)
) PARTITIONED BY (`partition`)
WITH (
  'connector' = 'hudi',
  'table.type' = 'MERGE_ON_READ',
  'metadata.enabled' = 'false', -- avoid classloader issue, class HFile can not be found
  'path' = '/tmp/hudi-flink-bundle-test'
);

-- insert data using values
INSERT INTO t1
VALUES ('id1', 'Danny', 23, TIMESTAMP '1970-01-01 00:00:01', 'par1'),
       ('id8', 'Han', 56, TIMESTAMP '1970-01-01 00:00:08', 'par4');
