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
        incremental_strategy='merge',
        merge_update_columns = ['city', 'updated_at'],
        unique_key='user_id',
        file_format='hudi',
        options={
            'type': 'cow',
            'primaryKey': 'user_id',
            'orderingFields': 'updated_at',
            'hoodie.table.cdc.enabled': 'true',
            'hoodie.table.cdc.supplemental.logging.mode': 'DATA_BEFORE_AFTER'
        }
    )
}}

with new_updates as (
    select user_id, city, updated_at from {{ ref('raw_updates') }}

    {% if is_incremental() %}
        where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

select
    user_id, city, updated_at
from new_updates

