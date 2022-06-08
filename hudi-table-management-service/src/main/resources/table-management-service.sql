--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

CREATE TABLE if not exists `instance`
(
    `id`               bigint unsigned NOT NULL AUTO_INCREMENT COMMENT 'primary key',
    `db_name`          varchar(128) NOT NULL COMMENT 'db name',
    `table_name`       varchar(128) NOT NULL COMMENT 'table name',
    `base_path`        varchar(128) NOT NULL COMMENT 'base path',
    `execution_engine` varchar(128) NOT NULL COMMENT 'execution engine',
    `owner`            varchar(128) NOT NULL COMMENT 'owner',
    `cluster`          varchar(128) NOT NULL COMMENT 'cluster',
    `queue`            varchar(128) NOT NULL COMMENT 'queue',
    `resource`         varchar(128) NOT NULL COMMENT 'resource',
    `parallelism`      varchar(128) NOT NULL COMMENT 'parallelism',
    `auto_clean`       int          NOT NULL DEFAULT '0' COMMENT 'auto_clean',
    `instant`          varchar(128) NOT NULL COMMENT 'instant',
    `action`           int          NOT NULL COMMENT 'action',
    `status`           int          NOT NULL COMMENT 'status',
    `run_times`        int          NOT NULL DEFAULT '0' COMMENT 'run times',
    `application_id`   varchar(128)          DEFAULT NULL COMMENT 'application id',
    `dorado_job_id`    varchar(128)          DEFAULT NULL COMMENT 'job id',
    `schedule_time`    timestamp NULL DEFAULT NULL COMMENT 'schedule time',
    `create_time`      timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
    `update_time`      timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'update time',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uniq_table_instant` (`db_name`,`table_name`,`instant`),
    KEY  `idx_status` (`status`),
    KEY  `idx_update_time_status` (`update_time`,`status`)
) COMMENT='Table Management Service instance';

