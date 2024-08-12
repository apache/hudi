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
        file_format='hudi',
        incremental_strategy='insert_overwrite'
    )
}}

with source_data as (

    select '101' as user_id, 'A' as city, unix_timestamp() as updated_at
    union all
    select '102' as user_id, 'B' as city, unix_timestamp() as updated_at
    union all
    select '103' as user_id, 'C' as city, unix_timestamp() as updated_at

)

select *
from source_data
