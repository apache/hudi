/*
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
 */

package org.apache.hudi.common.table.cdc;

/**
 * Here define four cdc file types. The different cdc file type will decide which file will be
 * used to extract the change data, and how to do this.
 *
 * CDC_LOG_FILE:
 *   For this type, there must be a real cdc log file from which we get the whole/part change data.
 *   when `hoodie.table.cdc.supplemental.logging.mode` is 'cdc_data_before_after', it keeps all the fields about the
 *   change data, including `op`, `ts_ms`, `before` and `after`. So read it and return directly,
 *   no more other files need to be loaded.
 *   when `hoodie.table.cdc.supplemental.logging.mode` is 'cdc_data_before', it keeps the `op`, the key and the
 *   `before` of the changing record. When `op` is equal to 'i' or 'u', need to get the current record from the
 *   current base/log file as `after`.
 *   when `hoodie.table.cdc.supplemental.logging.mode` is 'op_key', it just keeps the `op` and the key of
 *   the changing record. When `op` is equal to 'i', `before` is null and get the current record
 *   from the current base/log file as `after`. When `op` is equal to 'u', get the previous
 *   record from the previous file slice as `before`, and get the current record from the
 *   current base/log file as `after`. When `op` is equal to 'd', get the previous record from
 *   the previous file slice as `before`, and `after` is null.
 *
 * ADD_BASE_FILE:
 *   For this type, there must be a base file at the current instant. All the records from this
 *   file is new-coming, so we can load this, mark all the records with `i`, and treat them as
 *   the value of `after`. The value of `before` for each record is null.
 *
 * REMOVE_BASE_FILE:
 *   For this type, there must be an empty file at the current instant, but a non-empty base file
 *   at the previous instant. First we find this base file that has the same file group and belongs
 *   to the previous instant. Then load this, mark all the records with `d`, and treat them as
 *   the value of `before`. The value of `after` for each record is null.
 *
 * MOR_LOG_FILE:
 *   For this type, a normal log file of mor table will be used. First we need to load the previous
 *   file slice(including the base file and other log files in the same file group). Then for each
 *   record from the log file, get the key of this, and execute the following steps:
 *     1) if the record is deleted,
 *       a) if there is a record with the same key in the data loaded, `op` is 'd', 'before' is the
 *          record from the data loaded, `after` is null;
 *       b) if there is not a record with the same key in the data loaded, just skip.
 *     2) the record is not deleted,
 *       a) if there is a record with the same key in the data loaded, `op` is 'u', 'before' is the
 *          record from the data loaded, `after` is the current record;
 *       b) if there is not a record with the same key in the data loaded, `op` is 'i', 'before' is
 *          null, `after` is the current record;
 *
 * REPLACED_FILE_GROUP:
 *   For this type, it must be a replacecommit, like INSERT_OVERWRITE and DROP_PARTITION. It drops
 *   a whole file group. First we find this file group. Then load this, mark all the records with
 *   `d`, and treat them as the value of `before`. The value of `after` for each record is null.
 */
public enum CDCFileTypeEnum {

  CDC_LOG_FILE,
  ADD_BASE_FILE,
  REMOVE_BASE_FILE,
  MOR_LOG_FILE,
  REPLACED_FILE_GROUP;

}
