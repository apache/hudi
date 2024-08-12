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
 *
 */

{{
    config(
        materialized='incremental',
        file_format='hudi'
    )
}}

with new_changes as (
    select
        GET_JSON_OBJECT(after, '$.user_id') AS user_id,
        COALESCE(GET_JSON_OBJECT(before, '$.city'), 'Nil') AS old_city,
        GET_JSON_OBJECT(after, '$.city') AS new_city,
        ts_ms as process_ts

    from hudi_table_changes('hudi_examples_dbt.profiles', 'cdc',
        from_unixtime(unix_timestamp() - 3600 * 24, 'yyyyMMddHHmmss'))

    {% if is_incremental() %}
        where ts_ms > (select max(process_ts) from {{ this }})
    {% endif %}
)
select user_id, old_city, new_city, process_ts
from new_changes
