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

package org.apache.spark.sql.execution.datasources

import org.apache.hudi.HoodieConversionUtils.toScalaOption
import org.apache.hudi.common.config.TimestampKeyGeneratorConfig
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.util.{ConfigUtils, DateTimeUtils, StringUtils}
import org.apache.hudi.keygen.{CustomAvroKeyGenerator, CustomKeyGenerator, TimestampBasedAvroKeyGenerator, TimestampBasedKeyGenerator}
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.util.JavaScalaConverters

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils.{DEFAULT_PARTITION_NAME, unescapePathName}
import org.apache.spark.sql.catalyst.expressions.{Cast, Literal}
import org.apache.spark.sql.types.{AnsiIntervalType, AnyTimestampType, BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, NullType, ShortType, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.joda.time.DateTimeZone
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

import java.lang.{Double => JDouble, Long => JLong}
import java.math.{BigDecimal => JBigDecimal}
import java.time.Instant
import java.util.TimeZone

import scala.util.Try

object BetterParsePartitionUtil extends Logging {

  def parsePartition(partitionPath: String,
                     partitionSchema: StructType,
                     tableConfig: HoodieTableConfig,
                     timeZone: TimeZone): Array[Object] = {
    val keyGenerator = tableConfig.getKeyGeneratorClassName
    lazy val isTimestampKeygen = keyGenerator.equals(classOf[TimestampBasedKeyGenerator].getCanonicalName) ||
      keyGenerator.equals(classOf[TimestampBasedAvroKeyGenerator].getCanonicalName)
    lazy val isCustomKeygen = keyGenerator.equals(classOf[CustomKeyGenerator].getCanonicalName) ||
      keyGenerator.equals(classOf[CustomAvroKeyGenerator].getCanonicalName)
    lazy val timestampFieldsOpt = toScalaOption(CustomAvroKeyGenerator.getTimestampFields(tableConfig)).map(JavaScalaConverters.convertJavaListToScalaList)

    if (keyGenerator != null && (isTimestampKeygen || (isCustomKeygen && timestampFieldsOpt.isDefined))) {
      doParsePartition(partitionPath, partitionSchema, tableConfig, timeZone, timestampFieldsOpt)
    } else {
      doParsePartition(partitionPath, partitionSchema, tableConfig, timeZone, None)
    }
  }

  def doParsePartition(partitionPath: String,
                       partitionSchema: StructType,
                       tableConfig: HoodieTableConfig,
                       timeZone: TimeZone,
                       timestampFields: Option[List[String]]): Array[Object] = {
    if (partitionSchema.isEmpty) {
      Array.empty
    } else {
      val partitionFragments = partitionPath.split(StoragePath.SEPARATOR)
      if (partitionFragments.length != partitionSchema.length) {
        // at least 1 field has extra slashes
        if (partitionSchema.length == 1) {
          parseSingleFieldWithSlashes(partitionPath, partitionSchema, tableConfig, timeZone, timestampFields)
        } else {
          parseMultipleFieldsWithExtraSlashes(partitionPath, partitionSchema, tableConfig, timeZone, timestampFields, partitionFragments)
        }
      } else {
        parseMultipleFields(partitionSchema, tableConfig, timeZone, timestampFields, partitionFragments)
      }
    }
  }

  def parseSingleFieldWithSlashes(partitionPath: String,
                                  partitionSchema: StructType,
                                  tableConfig: HoodieTableConfig,
                                  timeZone: TimeZone,
                                  timestampFields: Option[List[String]]): Array[Object] = {
    val rawFieldVal = getRawFieldVal(partitionPath, partitionSchema.head.name)
    if (timestampFields.isDefined) {
      Array(parseTimestampKeygenField(rawFieldVal, tableConfig, timeZone).asInstanceOf[Object])
    } else {
      // If the partition column size is not equal to the partition fragment size
      // and the partition column size is 1, we map the whole partition path
      // to the partition column which can benefit from the partition prune.
      Array(UTF8String.fromString(rawFieldVal))
    }
  }

  def parseMultipleFieldsWithExtraSlashes(partitionPath: String,
                                          partitionSchema: StructType,
                                          tableConfig: HoodieTableConfig,
                                          timeZone: TimeZone,
                                          timestampFields: Option[List[String]],
                                          partitionFragments: Array[String]): Array[Object] = {
    val prefix = s"${partitionSchema.head.name}="
    if (partitionPath.startsWith(prefix)) {
      // hive style partitioning, so it is easy to figure out where each raw field starts and ends
      val rawFieldVals = splitHiveSlashPartitions(partitionFragments, partitionSchema.length)
      partitionSchema.fields.zip(rawFieldVals).map(col => {
        if (timestampFields.isDefined && timestampFields.get.contains(col._1.name)) {
          parseTimestampKeygenField(col._2, tableConfig, timeZone).asInstanceOf[Object]
        } else if (col._2.contains(StoragePath.SEPARATOR_CHAR)) {
          UTF8String.fromString(col._2)
        } else {
          castPartValueToDesiredType(col._1.dataType, col._2, timeZone).asInstanceOf[Object]
        }
      })
    } else if (timestampFields.isDefined) {
      // we have timestamp keygen configs saved to tableconfig, so we can figure out which slashes are
      // part of the field and which slashes separate fields
      val outputFormat = ConfigUtils.getStringWithAltKeys(tableConfig.getProps,
        TimestampKeyGeneratorConfig.TIMESTAMP_OUTPUT_DATE_FORMAT)
      val nSlashes = outputFormat.count(_ == StoragePath.SEPARATOR_CHAR)
      val rawFieldVals = Array.ofDim[String](partitionSchema.length)
      var index = 0
      partitionSchema.zipWithIndex.foreach(field => {
        if (timestampFields.get.contains(field._1.name)) {
          val nextIndex = index + nSlashes + 1
          rawFieldVals(field._2) = partitionFragments.slice(index, nextIndex).mkString(StoragePath.SEPARATOR)
          index = nextIndex
        } else {
          rawFieldVals(field._2) = partitionFragments(index)
          index += 1
        }
      })
      partitionSchema.fields.zip(rawFieldVals).map(col => {
        if (timestampFields.get.contains(col._1.name)) {
          parseTimestampKeygenField(col._2, tableConfig, timeZone).asInstanceOf[Object]
        } else {
          castPartValueToDesiredType(col._1.dataType, col._2, timeZone).asInstanceOf[Object]
        }
      })
    } else {
      // If the partition column size is not equal to the partition fragments size
      // and the partition column size > 1, and we don't have the keygen configs,
      // we do not know how to map the partition
      // fragments to the partition columns and therefore return an empty tuple. We don't
      // fail outright so that in some cases we can fallback to reading the table as non-partitioned
      // one
      logWarning(s"Failed to parse partition values: found partition fragments" +
        s" (${partitionFragments.mkString(",")}) are not aligned with expected partition columns" +
        s" (${partitionSchema.mkString(",")})")
      Array.empty
    }
  }


  def parseMultipleFields(partitionSchema: StructType,
                          tableConfig: HoodieTableConfig,
                          timeZone: TimeZone,
                          timestampFields: Option[List[String]],
                          partitionFragments: Array[String]): Array[Object] = {
    partitionSchema.fields.zip(partitionFragments).map(col => {
      val rawFieldVal = getRawFieldVal(col._2, col._1.name)
      if (timestampFields.isDefined && timestampFields.get.contains(col._1.name)) {
        parseTimestampKeygenField(rawFieldVal, tableConfig, timeZone).asInstanceOf[Object]
      } else {
        castPartValueToDesiredType(col._1.dataType, rawFieldVal, timeZone).asInstanceOf[Object]
      }
    })
  }

  def getRawFieldVal(field: String, name: String): String = {
    val equalSignIndex = field.indexOf('=')
    lazy val isHivePartition = unescapePathName(field.take(equalSignIndex)).equalsIgnoreCase(name)
    if (equalSignIndex == -1 || !isHivePartition) {
      field
    } else {
      field.drop(equalSignIndex + 1)
    }
  }

  def splitHiveSlashPartitions(partitionFragments: Array[String], nPartitions: Int): Array[String] = {
    val partitionVals = Array.ofDim[String](nPartitions)
    var index = 0
    var first = true
    for (fragment <- partitionFragments) {
      if (fragment.contains("=")) {
        if (first) {
          first = false
        } else {
          index += 1
        }
        partitionVals(index) = fragment.substring(fragment.indexOf("=") + 1)

      } else {
        partitionVals(index) += StoragePath.SEPARATOR + fragment
      }
    }
    partitionVals
  }

    def parseTimestampKeygenField(rawFieldVal: String,
                                  tableConfig: HoodieTableConfig,
                                  timeZone: TimeZone): Any = {
      val outputFormat = ConfigUtils.getStringWithAltKeys(tableConfig.getProps,
        TimestampKeyGeneratorConfig.TIMESTAMP_OUTPUT_DATE_FORMAT)
      if (StringUtils.isNullOrEmpty(outputFormat)) {
        UTF8String.fromString(rawFieldVal)
      } else {
        val formatter = DateTimeFormat.forPattern(outputFormat)
          .withZone(DateTimeZone.forTimeZone(timeZone))
        getMicrosFromGeneratedTimestamp(formatter, rawFieldVal)
      }
    }

  def getMicrosFromGeneratedTimestamp(formatter: DateTimeFormatter, timestamp: String): Long = {
    DateTimeUtils.instantToMicros(Instant.ofEpochMilli(formatter.parseDateTime(timestamp).getMillis))
  }

    def castPartValueToDesiredType(
                                    desiredType: DataType,
                                    value: String,
                                    timeZone: TimeZone): Any = desiredType match {
      case _ if value == DEFAULT_PARTITION_NAME => null
      case NullType => null
      case StringType => UTF8String.fromString(unescapePathName(value))
      case ByteType => Integer.parseInt(value).toByte
      case ShortType => Integer.parseInt(value).toShort
      case IntegerType => Integer.parseInt(value)
      case LongType => JLong.parseLong(value)
      case FloatType => JDouble.parseDouble(value).toFloat
      case DoubleType => JDouble.parseDouble(value)
      case _: DecimalType => Literal(new JBigDecimal(value)).value
      case DateType =>
        Cast(Literal(value), DateType, Some(timeZone.toZoneId.getId)).eval()
      // Timestamp types
      case dt if AnyTimestampType.acceptsType(dt) =>
        Try {
          Cast(Literal(unescapePathName(value)), dt, Some(timeZone.toZoneId.getId)).eval()
        }.getOrElse {
          Cast(Cast(Literal(value), DateType, Some(timeZone.toZoneId.getId)), dt).eval()
        }
      case it: AnsiIntervalType =>
        Cast(Literal(unescapePathName(value)), it).eval()
      case BinaryType => value.getBytes()
      case BooleanType => value.toBoolean
      case dt => throw new IllegalArgumentException(s"Unexpected type $dt")
    }

}
