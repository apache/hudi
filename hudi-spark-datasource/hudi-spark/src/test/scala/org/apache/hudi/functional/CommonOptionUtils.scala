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

package org.apache.hudi.functional

import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, DefaultSparkRecordMerger}
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.config.HoodieWriteConfig

object CommonOptionUtils {

  val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    "hoodie.bulkinsert.shuffle.parallelism" -> "2",
    "hoodie.delete.shuffle.parallelism" -> "1",
    HoodieTableConfig.PARTITION_METAFILE_USE_BASE_FORMAT.key() -> "true",
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
    HoodieTableConfig.ORDERING_FIELDS.key -> "timestamp",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
    HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key -> "1"
  )
  val sparkOpts = Map(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key -> classOf[DefaultSparkRecordMerger].getName)

  def getWriterReaderOpts(recordType: HoodieRecordType,
                          opt: Map[String, String] = commonOpts):
  (Map[String, String], Map[String, String]) = {

    recordType match {
      case HoodieRecordType.SPARK => (opt ++ sparkOpts, sparkOpts)
      case _ => (opt, Map.empty)
    }
  }
}
