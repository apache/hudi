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

package org.apache.spark.sql.hudi.common

import org.apache.hudi.common.config.TypedProperties
import org.apache.hudi.keygen.{BuiltinKeyGenerator, ComplexAvroKeyGenerator, KeyGenUtils}
import org.apache.hudi.keygen.constant.KeyGeneratorOptions

import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

import java.util.Arrays

import scala.jdk.CollectionConverters._

/**
 * Key generator that converts partition values to slash-separated partition paths.
 *
 * This generator is useful when you have partition columns like:
 * - datestr: "yyyy-mm-dd" format
 * - country, state, city: regular string values
 *
 * And you want to create partition paths like: yyyy/mm/dd/country/state/city
 *
 * The first partition field (typically a date) will have its hyphens replaced with slashes.
 * All partition fields are then combined with "/" as the separator.
 */
class MockSlashKeyGenerator(props: TypedProperties) extends BuiltinKeyGenerator(props) {

  private val complexAvroKeyGenerator: ComplexAvroKeyGenerator = new ComplexAvroKeyGenerator(props)

  this.recordKeyFields = KeyGenUtils.getRecordKeyFields(props)
  this.partitionPathFields = props.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key())
    .split(",")
    .map(_.trim)
    .filter(_.nonEmpty)
    .toList
    .asJava

  override def getRecordKey(record: GenericRecord): String = {
    complexAvroKeyGenerator.getRecordKey(record)
  }

  override def getPartitionPath(record: GenericRecord): String = {
    complexAvroKeyGenerator.getPartitionPath(record).replace('-', '/')
  }

  override def getRecordKey(row: Row): String = {
    tryInitRowAccessor(row.schema)
    combineRecordKey(getRecordKeyFieldNames, Arrays.asList(rowAccessor.getRecordKeyParts(row): _*))
  }

  override def getRecordKey(internalRow: InternalRow, schema: StructType): UTF8String = {
    tryInitRowAccessor(schema)
    combineRecordKeyUnsafe(getRecordKeyFieldNames, Arrays.asList(rowAccessor.getRecordKeyParts(internalRow): _*))
  }

  override def getPartitionPath(row: Row): String = {
    tryInitRowAccessor(row.schema)
    val partitionValues = rowAccessor.getRecordPartitionPathValues(row)
    formatPartitionPath(partitionValues)
  }

  override def getPartitionPath(row: InternalRow, schema: StructType): UTF8String = {
    tryInitRowAccessor(schema)
    val partitionValues = rowAccessor.getRecordPartitionPathValues(row)
    UTF8String.fromString(formatPartitionPath(partitionValues))
  }

  /**
   * Formats the partition path by:
   * 1. Converting the first partition value (date) from "yyyy-mm-dd" to "yyyy/mm/dd"
   * 2. Combining all partition values with "/" separator
   *
   * @param partitionValues Array of partition field values
   * @return Formatted partition path like "yyyy/mm/dd/country/state/city"
   */
  private def formatPartitionPath(partitionValues: Array[Object]): String = {
    if (partitionValues == null || partitionValues.length == 0) {
      ""
    } else {
      val partitionPath = new StringBuilder()

      for (i <- partitionValues.indices) {
        if (i > 0) {
          partitionPath.append("/")
        }

        var value = getPartitionValue(partitionValues(i))

        // For the first partition field (typically the date), replace hyphens with slashes
        if (i == 0 && value.contains("-")) {
          value = value.replace("-", "/")
        }

        partitionPath.append(value)
      }

      partitionPath.toString()
    }
  }

  /**
   * Extracts the string value from a partition field value object.
   *
   * @param value The partition field value
   * @return String representation of the value
   */
  private def getPartitionValue(value: Object): String = {
    value match {
      case null => ""
      case utf8: UTF8String => utf8.toString
      case _ => String.valueOf(value)
    }
  }
}
