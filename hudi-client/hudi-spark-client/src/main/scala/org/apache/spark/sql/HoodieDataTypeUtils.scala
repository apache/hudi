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

package org.apache.spark.sql

import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hudi.AvroConversionUtils
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.common.config.{HoodieStorageConfig, TypedProperties}
import org.apache.parquet.avro.AvroWriteSupport
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

object HoodieDataTypeUtils {
  private val log = LoggerFactory.getLogger(getClass)

  /**
   * Parses provided [[jsonSchema]] into [[StructType]].
   *
   * Throws [[RuntimeException]] in case it's unable to parse it as such.
   */
  def parseStructTypeFromJson(jsonSchema: String): StructType =
    StructType.fromString(jsonSchema)

  def canUseRowWriter(schema: Schema, conf: Configuration): Boolean = {
    false
//    if (conf.getBoolean(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, true)) {
//      // if we can write lists with the old list structure, we can use row writer regardless of decimal precision
//      true
//    } else if (!HoodieAvroUtils.hasSmallPrecisionDecimalField(schema)) {
//      true
//    } else {
//      // small precision decimals require the legacy write mode but lists and maps require the new write mode when
//      // WRITE_OLD_LIST_STRUCTURE is false so we can only use row writer if one is present and the other is not
//      if (HoodieAvroUtils.hasListOrMapField(schema)) {
//        log.warn("Cannot use row writer due to presence of list or map with a small precision decimal field")
//        false
//      } else {
//        true
//      }
//    }
  }

  /**
   * Checks whether default value (false) of "hoodie.parquet.writelegacyformat.enabled" should be
   * overridden in case:
   *
   * <ul>
   * <li>Property has not been explicitly set by the writer</li>
   * <li>Data schema contains {@code DecimalType} that would be affected by it</li>
   * </ul>
   *
   * If both of the aforementioned conditions are true, will override the default value of the config
   * (by essentially setting the value) to make sure that the produced Parquet data files could be
   * read by {@code AvroParquetReader}
   *
   * @param properties properties specified by the writer
   * @param schema     schema of the dataset being written
   */
  def tryOverrideParquetWriteLegacyFormatProperty(properties: java.util.Map[String, String], schema: StructType): Unit = {
    tryOverrideParquetWriteLegacyFormatProperty(Left(properties),
      AvroConversionUtils.convertStructTypeToAvroSchema(schema, "struct", "hoodie.source"))
  }

  def tryOverrideParquetWriteLegacyFormatProperty(properties: TypedProperties, schema: Schema): Unit = {
    tryOverrideParquetWriteLegacyFormatProperty(Right(properties), schema)
  }

  private def tryOverrideParquetWriteLegacyFormatProperty(properties: Either[java.util.Map[String, String], TypedProperties], schema: Schema): Unit = {
    val legacyFormatEnabledProp = properties match {
      case Left(map) => map.get(HoodieStorageConfig.PARQUET_WRITE_LEGACY_FORMAT_ENABLED.key)
      case Right(typedProps) => typedProps.get(HoodieStorageConfig.PARQUET_WRITE_LEGACY_FORMAT_ENABLED.key)
    }
    if (legacyFormatEnabledProp == null && HoodieAvroUtils.hasSmallPrecisionDecimalField(schema)) {
      // ParquetWriteSupport writes DecimalType to parquet as INT32/INT64 when the scale of decimalType
      // is less than {@code Decimal.MAX_LONG_DIGITS}, but {@code AvroParquetReader} which is used by
      // {@code HoodieParquetReader} does not support DecimalType encoded as INT32/INT64 as.
      //
      // To work this problem around we're checking whether
      //    - Schema contains any decimals that could be encoded as INT32/INT64
      //    - {@code HoodieStorageConfig.PARQUET_WRITE_LEGACY_FORMAT_ENABLED} has not been explicitly
      //    set by the writer
      //
      // If both of these conditions are true, then we override the default value of {@code
      // HoodieStorageConfig.PARQUET_WRITE_LEGACY_FORMAT_ENABLED} and set it to "true"
      log.warn("Small Decimal Type found in the persisted schema, reverting default value of 'hoodie.parquet.writelegacyformat.enabled' to true")
      properties match {
        case Left(map) => map.put(HoodieStorageConfig.PARQUET_WRITE_LEGACY_FORMAT_ENABLED.key, "true")
        case Right(typedProps) => typedProps.put(HoodieStorageConfig.PARQUET_WRITE_LEGACY_FORMAT_ENABLED.key, "true")
      }
    }
  }
}
