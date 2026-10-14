/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '60000 ms';

CREATE TABLE `orders_hudi` (
  `id` STRING NOT NULL,
  `name` STRING,
  `event_ts` TIMESTAMP(3) NOT NULL,
  `partition_date` DATE NOT NULL,
  PRIMARY KEY (`id`) NOT ENFORCED
)
PARTITIONED BY (`partition_date`)
WITH (
  'connector' = 'hudi',
  'path' = 'file:///tmp/hudi-architect-orders',
  'table.type' = 'COPY_ON_WRITE',
  'write.operation' = 'insert',
  'write.insert.cluster' = 'false'
);

INSERT INTO `orders_hudi` (
  `id`,
  `name`,
  `event_ts`,
  `partition_date`
)
SELECT
  `id`,
  `name`,
  `event_ts`,
  `partition_date`
FROM `orders_source`;
