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

package org.apache.spark.sql.execution.datasources

import org.apache.hadoop.fs.Path
import org.apache.hudi.common.util.PartitionPathEncodeUtils.DEFAULT_PARTITION_PATH
import org.apache.hudi.spark3.internal.ReflectUtil
import org.apache.hudi.util.JFunction
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils.unescapePathName
import org.apache.spark.sql.catalyst.expressions.{Cast, Literal}
import org.apache.spark.sql.catalyst.util.{DateFormatter, TimestampFormatter}
import org.apache.spark.sql.execution.datasources.PartitioningUtils.timestampPartitionPattern
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.lang.{Boolean => JBoolean, Double => JDouble, Long => JLong}
import java.math.{BigDecimal => JBigDecimal}
import java.time.ZoneId
import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.{Locale, TimeZone}
import scala.collection.convert.Wrappers.JConcurrentMapWrapper
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import scala.util.control.NonFatal

object Spark3ParsePartitionUtil extends SparkParsePartitionUtil {

  private val cache = JConcurrentMapWrapper(
    new ConcurrentHashMap[ZoneId, (DateFormatter, TimestampFormatter)](1))

  /**
   * The definition of PartitionValues has been changed by SPARK-34314 in Spark3.2.
   * To solve the compatibility between 3.1 and 3.2, copy some codes from PartitioningUtils in Spark3.2 here.
   * And this method will generate and return `InternalRow` directly instead of `PartitionValues`.
   */
  override def parsePartition(path: Path,
                              typeInference: Boolean,
                              basePaths: Set[Path],
                              userSpecifiedDataTypes: Map[String, DataType],
                              tz: TimeZone,
                              validatePartitionValues: Boolean = false): InternalRow = {
    val (dateFormatter, timestampFormatter) = cache.getOrElseUpdate(tz.toZoneId, {
      val dateFormatter = ReflectUtil.getDateFormatter(tz.toZoneId)
      val timestampFormatter = TimestampFormatter(timestampPartitionPattern, tz.toZoneId, isParsing = true)

      (dateFormatter, timestampFormatter)
    })

    val (partitionValues, _) = parsePartition(path, typeInference, basePaths, userSpecifiedDataTypes,
      validatePartitionValues, tz.toZoneId, dateFormatter, timestampFormatter)

    partitionValues.map {
      case PartitionValues(columnNames: Seq[String], typedValues: Seq[TypedPartValue]) =>
        val rowValues = columnNames.zip(typedValues).map { case (columnName, typedValue) =>
          try {
            castPartValueToDesiredType(typedValue.dataType, typedValue.value, tz.toZoneId)
          } catch {
            case NonFatal(_) =>
              if (validatePartitionValues) {
                throw new RuntimeException(s"Failed to cast value `${typedValue.value}` to " +
                  s"`${typedValue.dataType}` for partition column `$columnName`")
              } else null
          }
        }
        InternalRow.fromSeq(rowValues)
    }.getOrElse(InternalRow.empty)
  }

  case class TypedPartValue(value: String, dataType: DataType)

  case class PartitionValues(columnNames: Seq[String], typedValues: Seq[TypedPartValue])
  {
    require(columnNames.size == typedValues.size)
  }

  private def parsePartition(
      path: Path,
      typeInference: Boolean,
      basePaths: Set[Path],
      userSpecifiedDataTypes: Map[String, DataType],
      validatePartitionColumns: Boolean,
      zoneId: ZoneId,
      dateFormatter: DateFormatter,
      timestampFormatter: TimestampFormatter): (Option[PartitionValues], Option[Path]) = {

    val columns = ArrayBuffer.empty[(String, TypedPartValue)]
    // Old Hadoop versions don't have `Path.isRoot`
    var finished = path.getParent == null
    // currentPath is the current path that we will use to parse partition column value.
    var currentPath: Path = path

    while (!finished) {
      // Sometimes (e.g., when speculative task is enabled), temporary directories may be left
      // uncleaned. Here we simply ignore them.
      if (currentPath.getName.toLowerCase(Locale.ROOT) == "_temporary") {
        // scalastyle:off return
        return (None, None)
        // scalastyle:on return
      }

      if (basePaths.contains(currentPath)) {
        // If the currentPath is one of base paths. We should stop.
        finished = true
      } else {
        // Let's say currentPath is a path of "/table/a=1/", currentPath.getName will give us a=1.
        // Once we get the string, we try to parse it and find the partition column and value.
        val maybeColumn =
        parsePartitionColumn(currentPath.getName, typeInference, userSpecifiedDataTypes,
          validatePartitionColumns, zoneId, dateFormatter, timestampFormatter)
        maybeColumn.foreach(columns += _)

        // Now, we determine if we should stop.
        // When we hit any of the following cases, we will stop:
        //  - In this iteration, we could not parse the value of partition column and value,
        //    i.e. maybeColumn is None, and columns is not empty. At here we check if columns is
        //    empty to handle cases like /table/a=1/_temporary/something (we need to find a=1 in
        //    this case).
        //  - After we get the new currentPath, this new currentPath represent the top level dir
        //    i.e. currentPath.getParent == null. For the example of "/table/a=1/",
        //    the top level dir is "/table".
        finished =
          (maybeColumn.isEmpty && !columns.isEmpty) || currentPath.getParent == null

        if (!finished) {
          // For the above example, currentPath will be "/table/".
          currentPath = currentPath.getParent
        }
      }
    }

    if (columns.isEmpty) {
      (None, Some(path))
    } else {
      val (columnNames, values) = columns.reverse.unzip
      (Some(PartitionValues(columnNames, values)), Some(currentPath))
    }
  }

  private def parsePartitionColumn(
      columnSpec: String,
      typeInference: Boolean,
      userSpecifiedDataTypes: Map[String, DataType],
      validatePartitionColumns: Boolean,
      zoneId: ZoneId,
      dateFormatter: DateFormatter,
      timestampFormatter: TimestampFormatter): Option[(String, TypedPartValue)] = {
    val equalSignIndex = columnSpec.indexOf('=')
    if (equalSignIndex == -1) {
      None
    } else {
      val columnName = unescapePathName(columnSpec.take(equalSignIndex))
      assert(columnName.nonEmpty, s"Empty partition column name in '$columnSpec'")

      val rawColumnValue = columnSpec.drop(equalSignIndex + 1)
      assert(rawColumnValue.nonEmpty, s"Empty partition column value in '$columnSpec'")

      val dataType = if (userSpecifiedDataTypes.contains(columnName)) {
        // SPARK-26188: if user provides corresponding column schema, get the column value without
        //              inference, and then cast it as user specified data type.
        userSpecifiedDataTypes(columnName)
      } else {
        inferPartitionColumnValue(
          rawColumnValue,
          typeInference,
          zoneId,
          dateFormatter,
          timestampFormatter)
      }
      Some(columnName -> TypedPartValue(rawColumnValue, dataType))
    }
  }

  private def inferPartitionColumnValue(
      raw: String,
      typeInference: Boolean,
      zoneId: ZoneId,
      dateFormatter: DateFormatter,
      timestampFormatter: TimestampFormatter): DataType = {
    val decimalTry = Try {
      // `BigDecimal` conversion can fail when the `field` is not a form of number.
      val bigDecimal = new JBigDecimal(raw)
      // It reduces the cases for decimals by disallowing values having scale (e.g. `1.1`).
      require(bigDecimal.scale <= 0)
      // `DecimalType` conversion can fail when
      //   1. The precision is bigger than 38.
      //   2. scale is bigger than precision.
      fromDecimal(Decimal(bigDecimal))
    }

    val dateTry = Try {
      // try and parse the date, if no exception occurs this is a candidate to be resolved as
      // DateType
      dateFormatter.parse(raw)
      // SPARK-23436: Casting the string to date may still return null if a bad Date is provided.
      // This can happen since DateFormat.parse  may not use the entire text of the given string:
      // so if there are extra-characters after the date, it returns correctly.
      // We need to check that we can cast the raw string since we later can use Cast to get
      // the partition values with the right DataType (see
      // org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex.inferPartitioning)
      val dateValue = Cast(Literal(raw), DateType, Some(zoneId.getId)).eval()
      // Disallow DateType if the cast returned null
      require(dateValue != null)
      DateType
    }

    val timestampTry = Try {
      val unescapedRaw = unescapePathName(raw)
      // the inferred data type is consistent with the default timestamp type
      val timestampType = TimestampType
      // try and parse the date, if no exception occurs this is a candidate to be resolved as TimestampType
      timestampFormatter.parse(unescapedRaw)

      // SPARK-23436: see comment for date
      val timestampValue = Cast(Literal(unescapedRaw), timestampType, Some(zoneId.getId)).eval()
      // Disallow TimestampType if the cast returned null
      require(timestampValue != null)
      timestampType
    }

    if (typeInference) {
      // First tries integral types
      Try({ Integer.parseInt(raw); IntegerType })
        .orElse(Try { JLong.parseLong(raw); LongType })
        .orElse(decimalTry)
        // Then falls back to fractional types
        .orElse(Try { JDouble.parseDouble(raw); DoubleType })
        // Then falls back to date/timestamp types
        .orElse(timestampTry)
        .orElse(dateTry)
        // Then falls back to string
        .getOrElse {
          if (raw == DEFAULT_PARTITION_PATH) NullType else StringType
        }
    } else {
      if (raw == DEFAULT_PARTITION_PATH) NullType else StringType
    }
  }

  def castPartValueToDesiredType(
      desiredType: DataType,
      value: String,
      zoneId: ZoneId): Any = desiredType match {
    case _ if value == DEFAULT_PARTITION_PATH => null
    case NullType => null
    case BooleanType => JBoolean.parseBoolean(value)
    case StringType => UTF8String.fromString(unescapePathName(value))
    case IntegerType => Integer.parseInt(value)
    case LongType => JLong.parseLong(value)
    case DoubleType => JDouble.parseDouble(value)
    case _: DecimalType => Literal(new JBigDecimal(value)).value
    case DateType =>
      Cast(Literal(value), DateType, Some(zoneId.getId)).eval()
    // Timestamp types
    case dt: TimestampType =>
      Try {
        Cast(Literal(unescapePathName(value)), dt, Some(zoneId.getId)).eval()
      }.getOrElse {
        Cast(Cast(Literal(value), DateType, Some(zoneId.getId)), dt).eval()
      }
    case dt => throw new IllegalArgumentException(s"Unexpected type $dt")
  }

  private def fromDecimal(d: Decimal): DecimalType = DecimalType(d.precision, d.scale)
}
