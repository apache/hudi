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

import org.apache.hudi.common.config.TypedProperties
import org.apache.hudi.common.model.HoodieRecord.{PARTITION_PATH_META_FIELD_ORD, RECORD_KEY_META_FIELD_ORD}
import org.apache.hudi.common.util.StringUtils

import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String


/**
 * NOTE TO USERS: YOU SHOULD NOT SET THIS AS YOUR KEYGENERATOR
 *
 * Keygenerator that is meant to be used internally for the spark sql merge into command
 * It will attempt to get the partition path and recordkey from the metafields, but will
 * fallback to the sql keygenerator if the meta field is not populated
 *
 */
class MergeIntoKeyGenerator(props: TypedProperties) extends SqlKeyGenerator(props) {

  override def getRecordKey(record: GenericRecord): String = {
    val recordKey = record.get(RECORD_KEY_META_FIELD_ORD)
    if (recordKey != null) {
      recordKey.toString
    } else {
      super.getRecordKey(record)
    }
  }

  override def getRecordKey(row: Row): String = {
    val recordKey = row.getString(RECORD_KEY_META_FIELD_ORD)
    if (!StringUtils.isNullOrEmpty(recordKey)) {
      recordKey
    } else {
      super.getRecordKey(row)
    }
  }

  override def getRecordKey(internalRow: InternalRow, schema: StructType): UTF8String = {
    val recordKey = internalRow.getUTF8String(RECORD_KEY_META_FIELD_ORD)
    if (recordKey != null) {
      recordKey
    } else {
      super.getRecordKey(internalRow, schema)
    }
  }

  override def getPartitionPath(record: GenericRecord): String = {
    val partitionPath = record.get(PARTITION_PATH_META_FIELD_ORD)
    if (partitionPath != null) {
      partitionPath.toString
    } else {
      super.getPartitionPath(record)
    }
  }

  override def getPartitionPath(row: Row): String = {
    val partitionPath = row.getString(PARTITION_PATH_META_FIELD_ORD)
    if (!StringUtils.isNullOrEmpty(partitionPath)) {
      partitionPath
    } else {
      super.getPartitionPath(row)
    }
  }

  override def getPartitionPath(internalRow: InternalRow, schema: StructType): UTF8String = {
    val partitionPath = internalRow.getUTF8String(PARTITION_PATH_META_FIELD_ORD)
    if (partitionPath != null) {
      partitionPath
    } else {
      super.getPartitionPath(internalRow, schema)
    }
  }

}
