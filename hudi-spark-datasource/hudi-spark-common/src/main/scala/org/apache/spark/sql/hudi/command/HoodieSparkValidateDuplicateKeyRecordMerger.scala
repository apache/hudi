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

package org.apache.spark.sql.hudi.command

import org.apache.hudi.{DefaultSparkRecordMerger, HoodieSparkRecordMerger}
import org.apache.hudi.common.config.TypedProperties
import org.apache.hudi.common.model.{HoodieRecord, HoodieRecordMerger, OperationModeAwareness}
import org.apache.hudi.common.util.{collection, HoodieRecordUtils, Option => HOption}
import org.apache.hudi.exception.HoodieDuplicateKeyException

import org.apache.avro.Schema
import org.apache.spark.sql.hudi.command.HoodieSparkValidateDuplicateKeyRecordMerger.STRATEGY_ID

/**
 * Validate the duplicate key for insert statement without enable the INSERT_DROP_DUPS_OPT
 * config.
 * @see org.apache.spark.sql.hudi.command.ValidateDuplicateKeyPayload
 */
class HoodieSparkValidateDuplicateKeyRecordMerger extends HoodieSparkRecordMerger with OperationModeAwareness {

  override def merge(older: HoodieRecord[_], oldSchema: Schema, newer: HoodieRecord[_], newSchema: Schema, props: TypedProperties): collection.Pair[HoodieRecord[_], Schema] = {
    val key = older.getRecordKey(oldSchema, HoodieRecord.RECORD_KEY_METADATA_FIELD)
    throw new HoodieDuplicateKeyException(key)
  }

  override def asPreCombiningMode(): HoodieRecordMerger = {
    HoodieRecordUtils.loadRecordMerger(classOf[DefaultSparkRecordMerger].getName)
  }

  /**
   * The kind of merging strategy this recordMerger belongs to. An UUID represents merging strategy.
   */
  override def getMergingStrategy: String = STRATEGY_ID
}

object HoodieSparkValidateDuplicateKeyRecordMerger {
  final val STRATEGY_ID = "fb092649-0fdc-4c14-9113-acde3034a6c4"
}
