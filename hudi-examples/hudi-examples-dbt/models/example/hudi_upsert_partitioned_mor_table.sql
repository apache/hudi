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

/*
 Example of an upsert for a partitioned merge on read table with incremental materialization using merge strategy.
 */
{{ config(
    materialized='incremental',
    file_format='hudi',
    incremental_strategy='merge',
    options={
        'type': 'mor',
        'primaryKey': 'id',
        'precombineKey': 'ts',
    },
    unique_key='id',
    partition_by='datestr',
    pre_hook=["set spark.sql.datetime.java8API.enabled=false;"],
   )
}}

select id, name, current_timestamp() as ts, current_date as datestr
from {{ ref('hudi_upsert_table') }}