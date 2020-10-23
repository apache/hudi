/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi

import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload

/**
 * Options supported for writing hoodie tables.
 * TODO: This file is partially copied from org.apache.hudi.DataSourceWriteOptions.
 * Should be removed if Spark 2.x support is dropped.
 */
object DataSourceWriteOptionsForSpark2 {
  /**
   * The table type for the underlying data, for this write.
   * Note that this can't change across writes.
   *
   * Default: COPY_ON_WRITE
   */
  val TABLE_TYPE_OPT_KEY = "hoodie.datasource.write.table.type"
  val COW_TABLE_TYPE_OPT_VAL = HoodieTableType.COPY_ON_WRITE.name
  val MOR_TABLE_TYPE_OPT_VAL = HoodieTableType.MERGE_ON_READ.name
  val DEFAULT_TABLE_TYPE_OPT_VAL = COW_TABLE_TYPE_OPT_VAL

  /**
   * Payload class used. Override this, if you like to roll your own merge logic, when upserting/inserting.
   * This will render any value set for `PRECOMBINE_FIELD_OPT_VAL` in-effective
   */
  val PAYLOAD_CLASS_OPT_KEY = "hoodie.datasource.write.payload.class"
  val DEFAULT_PAYLOAD_OPT_VAL = classOf[OverwriteWithLatestAvroPayload].getName

  /**
   * Flag to indicate whether to drop duplicates upon insert.
   * By default insert will accept duplicates, to gain extra performance.
   */
  val INSERT_DROP_DUPS_OPT_KEY = "hoodie.datasource.write.insert.drop.duplicates"
  val DEFAULT_INSERT_DROP_DUPS_OPT_VAL = "false"

  // Async Compaction - Enabled by default for MOR
  val ASYNC_COMPACT_ENABLE_OPT_KEY = "hoodie.datasource.compaction.async.enable"
  val DEFAULT_ASYNC_COMPACT_ENABLE_OPT_VAL = "true"
}
