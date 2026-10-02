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

package org.apache.hudi.dataframe

import org.apache.hudi.common.config.{TimestampKeyGeneratorConfig, TypedProperties}
import org.apache.hudi.common.util.PartitionPathEncodeUtils
import org.apache.hudi.exception.HoodieException

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{coalesce, col, concat, concat_ws, lit, udf, when}
import org.apache.spark.sql.types.StringType

import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

/**
 * Key generation as Catalyst column expressions, mirroring the built-in key generators' record
 * key and partition path formats without instantiating them per row: the whole prepare stage
 * stays a projection the optimizer can see.
 */
object KeyGenExpressions {

  private val NULL_RECORDKEY_PLACEHOLDER = "__null__"
  private val EMPTY_RECORDKEY_PLACEHOLDER = "__empty__"

  sealed trait KeyGenKind
  case object SimpleKind extends KeyGenKind
  case object ComplexKind extends KeyGenKind
  case object TimestampKind extends KeyGenKind
  case object CustomKind extends KeyGenKind
  case object NonPartitionedKind extends KeyGenKind

  def kindOf(keyGenClass: String): KeyGenKind = {
    keyGenClass.substring(keyGenClass.lastIndexOf('.') + 1) match {
      case "SimpleKeyGenerator" | "SimpleAvroKeyGenerator" => SimpleKind
      case "ComplexKeyGenerator" | "ComplexAvroKeyGenerator" => ComplexKind
      case "TimestampBasedKeyGenerator" | "TimestampBasedAvroKeyGenerator" => TimestampKind
      case "CustomKeyGenerator" | "CustomAvroKeyGenerator" => CustomKind
      case "NonpartitionedKeyGenerator" | "NonpartitionedAvroKeyGenerator" => NonPartitionedKind
      case other =>
        throw new HoodieException(s"Key generator $other is not supported by the DataFrame write path yet")
    }
  }

  /** Record key column: the bare value for a single simple key, `f1:v1,f2:v2` otherwise. */
  def recordKeyExpr(kind: KeyGenKind, recordKeyFields: Seq[String]): Column = {
    kind match {
      case ComplexKind => complexKeyExpr(recordKeyFields)
      case _ if recordKeyFields.size == 1 => col(recordKeyFields.head).cast(StringType)
      case _ => complexKeyExpr(recordKeyFields)
    }
  }

  private def complexKeyExpr(fields: Seq[String]): Column = {
    concat_ws(",", fields.map { f =>
      concat(lit(f + ":"),
        when(col(f).isNull, lit(NULL_RECORDKEY_PLACEHOLDER))
          .when(col(f).cast(StringType) === "", lit(EMPTY_RECORDKEY_PLACEHOLDER))
          .otherwise(col(f).cast(StringType)))
    }: _*)
  }

  /**
   * Partition path column. Custom keygen partition fields carry a `field:type` suffix selecting
   * simple or timestamp handling per field, mirroring CustomKeyGenerator.
   */
  def partitionPathExpr(kind: KeyGenKind,
                        partitionFields: Seq[String],
                        props: TypedProperties,
                        hiveStyle: Boolean,
                        urlEncode: Boolean): Column = {
    kind match {
      case NonPartitionedKind => lit("")
      case _ if partitionFields.isEmpty => lit("")
      case TimestampKind =>
        partitionFieldExpr(partitionFields.head, timestamp = true, props, hiveStyle, urlEncode)
      case CustomKind =>
        concat_ws("/", partitionFields.map { spec =>
          val parts = spec.split(":")
          val timestamp = parts.length > 1 && parts(1).equalsIgnoreCase("timestamp")
          partitionFieldExpr(parts(0), timestamp, props, hiveStyle, urlEncode)
        }: _*)
      case _ =>
        concat_ws("/", partitionFields.map(f =>
          partitionFieldExpr(f, timestamp = false, props, hiveStyle, urlEncode)): _*)
    }
  }

  private def partitionFieldExpr(field: String,
                                 timestamp: Boolean,
                                 props: TypedProperties,
                                 hiveStyle: Boolean,
                                 urlEncode: Boolean): Column = {
    val raw = col(field).cast(StringType)
    val value = if (timestamp) {
      val conf = TimestampFormat.fromProps(props)
      val convert = udf((v: String) => TimestampFormat.toPartition(conf, v))
      convert(raw)
    } else {
      when(raw.isNull || raw === "", lit(PartitionPathEncodeUtils.DEFAULT_PARTITION_PATH))
        .otherwise(raw)
    }
    val encoded = if (urlEncode) {
      val escape = udf((v: String) => PartitionPathEncodeUtils.escapePathName(v))
      escape(value)
    } else {
      value
    }
    if (hiveStyle) concat(lit(field + "="), coalesce(encoded, lit(PartitionPathEncodeUtils.DEFAULT_PARTITION_PATH))) else encoded
  }
}

/** Serializable timestamp-keygen settings plus the per-executor conversion used by the UDF. */
case class TimestampFormat(timestampType: String,
                           inputFormats: Seq[String],
                           outputFormat: String,
                           inputTimezone: String,
                           outputTimezone: String,
                           scalarUnit: String) extends Serializable

object TimestampFormat {
  private val formatterCache = new ConcurrentHashMap[String, DateTimeFormatter]()

  def fromProps(props: TypedProperties): TimestampFormat = {
    def get(key: org.apache.hudi.common.config.ConfigProperty[String], fallback: String): String = {
      val v = props.getString(key.key(), if (key.hasDefaultValue) key.defaultValue() else fallback)
      if (v == null) fallback else v
    }
    val generalTz = get(TimestampKeyGeneratorConfig.TIMESTAMP_TIMEZONE_FORMAT, "UTC")
    TimestampFormat(
      props.getString(TimestampKeyGeneratorConfig.TIMESTAMP_TYPE_FIELD.key(), "EPOCHMILLISECONDS"),
      get(TimestampKeyGeneratorConfig.TIMESTAMP_INPUT_DATE_FORMAT, "")
        .split(get(TimestampKeyGeneratorConfig.TIMESTAMP_INPUT_DATE_FORMAT_LIST_DELIMITER_REGEX, ","))
        .filter(_.nonEmpty).toSeq,
      get(TimestampKeyGeneratorConfig.TIMESTAMP_OUTPUT_DATE_FORMAT, "yyyyMMdd"),
      get(TimestampKeyGeneratorConfig.TIMESTAMP_INPUT_TIMEZONE_FORMAT, generalTz),
      get(TimestampKeyGeneratorConfig.TIMESTAMP_OUTPUT_TIMEZONE_FORMAT, generalTz),
      props.getString("hoodie.keygen.timebased.timestamp.scalar.time.unit", "SECONDS"))
  }

  def toPartition(conf: TimestampFormat, value: String): String = {
    if (value == null || value.isEmpty) {
      PartitionPathEncodeUtils.DEFAULT_PARTITION_PATH
    } else {
      val millis = conf.timestampType.toUpperCase match {
        case "EPOCHMILLISECONDS" => value.trim.toDouble.toLong
        case "UNIX_TIMESTAMP" => value.trim.toDouble.toLong * 1000L
        case "EPOCHMICROSECONDS" => value.trim.toDouble.toLong / 1000L
        case "SCALAR" =>
          TimeUnit.valueOf(conf.scalarUnit.toUpperCase).toMillis(value.trim.toDouble.toLong)
        case "DATE_STRING" =>
          val zone = zoneOf(conf.inputTimezone)
          conf.inputFormats.view.flatMap { pattern =>
            try {
              val fmt = new java.text.SimpleDateFormat(pattern)
              fmt.setTimeZone(java.util.TimeZone.getTimeZone(zone))
              Some(fmt.parse(value.trim).getTime)
            } catch {
              case _: Exception => None
            }
          }.headOption.getOrElse(
            throw new HoodieException(s"Cannot parse timestamp '$value' with formats ${conf.inputFormats}"))
        case other => throw new HoodieException(s"Timestamp type $other is not supported by the DataFrame write path yet")
      }
      formatter(conf.outputFormat, conf.outputTimezone).format(Instant.ofEpochMilli(millis))
    }
  }

  private def zoneOf(tz: String): ZoneId =
    if (tz == null || tz.isEmpty) ZoneId.of("UTC") else java.util.TimeZone.getTimeZone(tz).toZoneId

  private def formatter(pattern: String, tz: String): DateTimeFormatter = {
    formatterCache.computeIfAbsent(pattern + "@" + tz,
      _ => DateTimeFormatter.ofPattern(pattern).withZone(zoneOf(tz)))
  }
}
