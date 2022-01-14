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

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, IndexedRecord}
import org.apache.hudi.common.model.{DefaultHoodieRecordPayload, HoodieRecord}
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.exception.HoodieDuplicateKeyException


import java.util.Properties

/**
 * Validate the duplicate key for insert statement without enable the INSERT_DROP_DUPS_OPT
 * config.
 */
class ValidateDuplicateKeyPayload(record: GenericRecord, orderingVal: Comparable[_])
  extends DefaultHoodieRecordPayload(record, orderingVal) {

  def this(record: HOption[GenericRecord]) {
    this(if (record.isPresent) record.get else null, 0)
  }

  override def combineAndGetUpdateValue(currentValue: IndexedRecord,
                                        schema: Schema, properties: Properties): HOption[IndexedRecord] = {
    val key = currentValue.asInstanceOf[GenericRecord].get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString
    throw new HoodieDuplicateKeyException(key)
  }
}
