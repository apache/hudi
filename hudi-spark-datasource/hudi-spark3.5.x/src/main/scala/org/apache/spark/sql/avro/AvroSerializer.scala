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

package org.apache.spark.sql.avro

import org.apache.avro.LogicalTypes
import org.apache.avro.Conversions.DecimalConversion
import org.apache.avro.generic.GenericData.{EnumSymbol, Fixed, Record}
import org.apache.avro.util.Utf8
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.schema.HoodieSchemaType
import org.apache.hudi.common.schema.HoodieSchemaType._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.avro.AvroSerializer.{createDateRebaseFuncInWrite, createTimestampRebaseFuncInWrite}
import org.apache.spark.sql.avro.AvroUtils.toFieldStr
import org.apache.spark.sql.avro.{HoodieSchemaHelper, HoodieMatchedField}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{SpecializedGetters, SpecificInternalRow}
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, RebaseDateTime}
import org.apache.spark.sql.execution.datasources.DataSourceUtils
import org.apache.spark.sql.internal.{LegacyBehaviorPolicy, SQLConf}
import org.apache.spark.sql.types._

import java.nio.ByteBuffer
import java.util.TimeZone
import scala.collection.JavaConverters._

/**
 * A serializer to serialize data in catalyst format to data in avro format.
 *
 * NOTE: This code is borrowed from Spark 3.3.0
 *       This code is borrowed, so that we can better control compatibility w/in Spark minor
 *       branches (3.2.x, 3.1.x, etc)
 *
 * NOTE: THIS IMPLEMENTATION HAS BEEN MODIFIED FROM ITS ORIGINAL VERSION WITH THE MODIFICATION
 *       BEING EXPLICITLY ANNOTATED INLINE. PLEASE MAKE SURE TO UNDERSTAND PROPERLY ALL THE
 *       MODIFICATIONS.
 *
 * PLEASE REFRAIN MAKING ANY CHANGES TO THIS CODE UNLESS ABSOLUTELY NECESSARY
 */
private[sql] class AvroSerializer(rootCatalystType: DataType,
                                  rootHoodieType: HoodieSchema,
                                  nullable: Boolean,
                                  positionalFieldMatch: Boolean,
                                  datetimeRebaseMode: LegacyBehaviorPolicy.Value) extends Logging {

  def this(rootCatalystType: DataType, rootHoodieType: HoodieSchema, nullable: Boolean) = {
    this(rootCatalystType, rootHoodieType, nullable, positionalFieldMatch = false,
      LegacyBehaviorPolicy.withName(SQLConf.get.getConf(SQLConf.AVRO_REBASE_MODE_IN_WRITE,
        LegacyBehaviorPolicy.CORRECTED.toString)))
  }

  def serialize(catalystData: Any): Any = {
    converter.apply(catalystData)
  }

  private val dateRebaseFunc = createDateRebaseFuncInWrite(
    datetimeRebaseMode, "Avro")

  private val timestampRebaseFunc = createTimestampRebaseFuncInWrite(
    datetimeRebaseMode, "Avro")

  private val converter: Any => Any = {
    val actualHoodieType = resolveNullableType(rootHoodieType, nullable)
    val baseConverter = try {
      rootCatalystType match {
        case st: StructType =>
          newStructConverter(st, actualHoodieType, Nil, Nil).asInstanceOf[Any => Any]
        case _ =>
          val tmpRow = new SpecificInternalRow(Seq(rootCatalystType))
          val converter = newConverter(rootCatalystType, actualHoodieType, Nil, Nil)
          (data: Any) =>
            tmpRow.update(0, data)
            converter.apply(tmpRow, 0)
      }
    } catch {
      case ise: IncompatibleSchemaException => throw new IncompatibleSchemaException(
        s"Cannot convert SQL type ${rootCatalystType.sql} to Avro type $rootHoodieType.", ise)
    }
    if (nullable) {
      (data: Any) =>
        if (data == null) {
          null
        } else {
          baseConverter.apply(data)
        }
    } else {
      baseConverter
    }
  }

  private type Converter = (SpecializedGetters, Int) => Any

  private lazy val decimalConversions = new DecimalConversion()

  private def newConverter(catalystType: DataType,
                           hoodieType: HoodieSchema,
                           catalystPath: Seq[String],
                           avroPath: Seq[String]): Converter = {
    val errorPrefix = s"Cannot convert SQL ${toFieldStr(catalystPath)} " +
      s"to Avro ${toFieldStr(avroPath)} because "
    (catalystType, hoodieType.getType) match {
      case (NullType, HoodieSchemaType.NULL) =>
        (getter, ordinal) => null
      case (BooleanType, HoodieSchemaType.BOOLEAN) =>
        (getter, ordinal) => getter.getBoolean(ordinal)
      case (ByteType, HoodieSchemaType.INT) =>
        (getter, ordinal) => getter.getByte(ordinal).toInt
      case (ShortType, HoodieSchemaType.INT) =>
        (getter, ordinal) => getter.getShort(ordinal).toInt
      case (IntegerType, HoodieSchemaType.INT) =>
        (getter, ordinal) => getter.getInt(ordinal)
      case (LongType, HoodieSchemaType.LONG) =>
        (getter, ordinal) => getter.getLong(ordinal)
      case (FloatType, HoodieSchemaType.FLOAT) =>
        (getter, ordinal) => getter.getFloat(ordinal)
      case (DoubleType, HoodieSchemaType.DOUBLE) =>
        (getter, ordinal) => getter.getDouble(ordinal)
      case (d: DecimalType, HoodieSchemaType.FIXED) =>
        hoodieType match {
          case decimal: HoodieSchema.Decimal =>
            (getter, ordinal) =>
              val decimalValue = getter.getDecimal(ordinal, d.precision, d.scale)
              decimalConversions.toFixed(decimalValue.toJavaBigDecimal,
                hoodieType.toAvroSchema, LogicalTypes.decimal(d.precision, d.scale))
          case _ =>
            throw new IncompatibleSchemaException(errorPrefix +
              s"FIXED type must be a decimal schema")
        }

      case (d: DecimalType, HoodieSchemaType.BYTES) =>
        hoodieType match {
          case decimal: HoodieSchema.Decimal =>
            (getter, ordinal) =>
              val decimalValue = getter.getDecimal(ordinal, d.precision, d.scale)
              decimalConversions.toBytes(decimalValue.toJavaBigDecimal,
                hoodieType.toAvroSchema, LogicalTypes.decimal(d.precision, d.scale))
          case _ =>
            throw new IncompatibleSchemaException(errorPrefix +
              s"BYTES type must be a decimal schema")
        }

      case (StringType, HoodieSchemaType.ENUM) =>
        val enumSymbols: Set[String] = hoodieType.getEnumSymbols.asScala.toSet
        (getter, ordinal) =>
          val data = getter.getUTF8String(ordinal).toString
          if (!enumSymbols.contains(data)) {
            throw new IncompatibleSchemaException(errorPrefix +
              s""""$data" cannot be written since it's not defined in enum """ +
              enumSymbols.mkString("\"", "\", \"", "\""))
          }
          new EnumSymbol(hoodieType.toAvroSchema, data)

      case (StringType, HoodieSchemaType.STRING) =>
        (getter, ordinal) => new Utf8(getter.getUTF8String(ordinal).getBytes)

      case (BinaryType, HoodieSchemaType.FIXED) =>
        val size = hoodieType.getFixedSize
        (getter, ordinal) =>
          val data: Array[Byte] = getter.getBinary(ordinal)
          if (data.length != size) {
            def len2str(len: Int): String = s"$len ${if (len > 1) "bytes" else "byte"}"

            throw new IncompatibleSchemaException(errorPrefix + len2str(data.length) +
              " of binary data cannot be written into FIXED type with size of " + len2str(size))
          }
          new Fixed(hoodieType.toAvroSchema, data)

      case (BinaryType, HoodieSchemaType.BYTES) =>
        (getter, ordinal) => ByteBuffer.wrap(getter.getBinary(ordinal))

      case (DateType, HoodieSchemaType.INT) =>
        (getter, ordinal) => dateRebaseFunc(getter.getInt(ordinal))

      case (TimestampType, HoodieSchemaType.LONG) =>
        hoodieType match {
          case ts: HoodieSchema.Timestamp if ts.isUtcAdjusted =>
            ts.getPrecision match {
              case HoodieSchema.TimePrecision.MILLIS => (getter, ordinal) =>
                DateTimeUtils.microsToMillis(timestampRebaseFunc(getter.getLong(ordinal)))
              case HoodieSchema.TimePrecision.MICROS => (getter, ordinal) =>
                timestampRebaseFunc(getter.getLong(ordinal))
            }
          case _ if hoodieType.getType == HoodieSchemaType.LONG =>
            // For backward compatibility, if the Avro type is Long and it is not logical type,
            // output the timestamp value as with millisecond precision.
            (getter, ordinal) =>
              DateTimeUtils.microsToMillis(timestampRebaseFunc(getter.getLong(ordinal)))
          case _ =>
            throw new IncompatibleSchemaException(errorPrefix +
              s"SQL type ${TimestampType.sql} cannot be converted to Avro type $hoodieType")
        }

      case (TimestampNTZType, HoodieSchemaType.LONG) =>
        hoodieType match {
          case ts: HoodieSchema.Timestamp if !ts.isUtcAdjusted =>
            ts.getPrecision match {
              case HoodieSchema.TimePrecision.MILLIS => (getter, ordinal) =>
                DateTimeUtils.microsToMillis(getter.getLong(ordinal))
              case HoodieSchema.TimePrecision.MICROS => (getter, ordinal) =>
                getter.getLong(ordinal)
            }
          case _ if hoodieType.getType == HoodieSchemaType.LONG =>
            // To keep consistent with TimestampType, if the Avro type is Long and it is not
            // logical type, output the TimestampNTZ as long value in millisecond precision.
            (getter, ordinal) =>
              DateTimeUtils.microsToMillis(getter.getLong(ordinal))
          case _ =>
            throw new IncompatibleSchemaException(errorPrefix +
              s"SQL type ${TimestampNTZType.sql} cannot be converted to Avro type $hoodieType")
        }

      case (ArrayType(et, containsNull), HoodieSchemaType.ARRAY) =>
        val elementConverter = newConverter(
          et, resolveNullableType(hoodieType.getElementType, containsNull),
          catalystPath :+ "element", avroPath :+ "element")
        (getter, ordinal) => {
          val arrayData = getter.getArray(ordinal)
          val len = arrayData.numElements()
          val result = new Array[Any](len)
          var i = 0
          while (i < len) {
            if (containsNull && arrayData.isNullAt(i)) {
              result(i) = null
            } else {
              result(i) = elementConverter(arrayData, i)
            }
            i += 1
          }
          // avro writer is expecting a Java Collection, so we convert it into
          // `ArrayList` backed by the specified array without data copying.
          java.util.Arrays.asList(result: _*)
        }

      case (st: StructType, HoodieSchemaType.RECORD) =>
        val structConverter = newStructConverter(st, hoodieType, catalystPath, avroPath)
        val numFields = st.length
        (getter, ordinal) => structConverter(getter.getStruct(ordinal, numFields))

      ////////////////////////////////////////////////////////////////////////////////////////////
      // Following section is amended to the original (Spark's) implementation
      // >>> BEGINS
      ////////////////////////////////////////////////////////////////////////////////////////////

      case (st: StructType, HoodieSchemaType.UNION) =>
        val unionConverter = newUnionConverter(st, hoodieType, catalystPath, avroPath)
        val numFields = st.length
        (getter, ordinal) => unionConverter(getter.getStruct(ordinal, numFields))

      ////////////////////////////////////////////////////////////////////////////////////////////
      // <<< ENDS
      ////////////////////////////////////////////////////////////////////////////////////////////

      case (MapType(kt, vt, valueContainsNull), HoodieSchemaType.MAP) if kt == StringType =>
        val valueConverter = newConverter(
          vt, resolveNullableType(hoodieType.getValueType, valueContainsNull),
          catalystPath :+ "value", avroPath :+ "value")
        (getter, ordinal) =>
          val mapData = getter.getMap(ordinal)
          val len = mapData.numElements()
          val result = new java.util.HashMap[String, Any](len)
          val keyArray = mapData.keyArray()
          val valueArray = mapData.valueArray()
          var i = 0
          while (i < len) {
            val key = keyArray.getUTF8String(i).toString
            if (valueContainsNull && valueArray.isNullAt(i)) {
              result.put(key, null)
            } else {
              result.put(key, valueConverter(valueArray, i))
            }
            i += 1
          }
          result

      case (_: YearMonthIntervalType, HoodieSchemaType.INT) =>
        (getter, ordinal) => getter.getInt(ordinal)

      case (_: DayTimeIntervalType, HoodieSchemaType.LONG) =>
        (getter, ordinal) => getter.getLong(ordinal)

      case _ =>
        throw new IncompatibleSchemaException(errorPrefix +
          s"schema is incompatible (sqlType = ${catalystType.sql}, hoodieType = $hoodieType)")
    }
  }

  private def newStructConverter(catalystStruct: StructType,
                                 hoodieStruct: HoodieSchema,
                                 catalystPath: Seq[String],
                                 avroPath: Seq[String]): InternalRow => Record = {

    val hoodieSchemaHelper = new HoodieSchemaHelper(
      hoodieStruct, catalystStruct, avroPath, catalystPath, positionalFieldMatch)

    hoodieSchemaHelper.validateNoExtraCatalystFields(ignoreNullable = false)
    hoodieSchemaHelper.validateNoExtraRequiredHoodieFields()

    val (avroIndices, fieldConverters) = hoodieSchemaHelper.matchedFields.map {
      case HoodieMatchedField(catalystField, _, hoodieField) =>
        val converter = newConverter(catalystField.dataType,
          resolveNullableType(hoodieField.schema(), catalystField.nullable),
          catalystPath :+ catalystField.name, avroPath :+ hoodieField.name())
        (hoodieField.pos(), converter)
    }.toArray.unzip

    val numFields = catalystStruct.length
    row: InternalRow =>
      val result = new Record(hoodieStruct.toAvroSchema)
      var i = 0
      while (i < numFields) {
        if (row.isNullAt(i)) {
          result.put(avroIndices(i), null)
        } else {
          result.put(avroIndices(i), fieldConverters(i).apply(row, i))
        }
        i += 1
      }
      result
  }

  ////////////////////////////////////////////////////////////////////////////////////////////
  // Following section is amended to the original (Spark's) implementation
  // >>> BEGINS
  ////////////////////////////////////////////////////////////////////////////////////////////

  private def newUnionConverter(catalystStruct: StructType,
                                hoodieUnion: HoodieSchema,
                                catalystPath: Seq[String],
                                avroPath: Seq[String]): InternalRow => Any = {
    if (hoodieUnion.getType != HoodieSchemaType.UNION || !canMapUnion(catalystStruct, hoodieUnion)) {
      throw new IncompatibleSchemaException(s"Cannot convert Catalyst type $catalystStruct to " +
        s"Avro type $hoodieUnion.")
    }
    val nullable = hoodieUnion.getTypes.size() > 0 && hoodieUnion.getTypes.get(0).getType == HoodieSchemaType.NULL
    val avroInnerTypes = if (nullable) {
      hoodieUnion.getTypes.asScala.tail
    } else {
      hoodieUnion.getTypes.asScala
    }
    val fieldConverters = catalystStruct.zip(avroInnerTypes).map {
      case (f1, f2) => newConverter(f1.dataType, f2, catalystPath, avroPath)
    }
    val numFields = catalystStruct.length
    (row: InternalRow) =>
      var i = 0
      var result: Any = null
      while (i < numFields) {
        if (!row.isNullAt(i)) {
          if (result != null) {
            throw new IncompatibleSchemaException(s"Cannot convert Catalyst record $catalystStruct to " +
              s"hoodie union $hoodieUnion. Record has more than one optional values set")
          }
          result = fieldConverters(i).apply(row, i)
        }
        i += 1
      }
      if (!nullable && result == null) {
        throw new IncompatibleSchemaException(s"Cannot convert Catalyst record $catalystStruct to " +
          s"Hoodie union $hoodieUnion. Record has no values set, while should have exactly one")
      }
      result
  }

  private def canMapUnion(catalystStruct: StructType, avroStruct: HoodieSchema): Boolean = {
    (avroStruct.getTypes.size() > 0 &&
      avroStruct.getTypes.get(0).getType == HoodieSchemaType.NULL &&
      avroStruct.getTypes.size() - 1 == catalystStruct.length) || avroStruct.getTypes.size() == catalystStruct.length
  }

  ////////////////////////////////////////////////////////////////////////////////////////////
  // <<< ENDS
  ////////////////////////////////////////////////////////////////////////////////////////////


  /**
   * Resolve a possibly nullable Avro Type.
   *
   * An Avro type is nullable when it is a [[UNION]] of two types: one null type and another
   * non-null type. This method will check the nullability of the input Avro type and return the
   * non-null type within when it is nullable. Otherwise it will return the input Avro type
   * unchanged. It will throw an [[UnsupportedAvroTypeException]] when the input Avro type is an
   * unsupported nullable type.
   *
   * It will also log a warning message if the nullability for Avro and catalyst types are
   * different.
   */
  private def resolveNullableType(avroType: HoodieSchema, nullable: Boolean): HoodieSchema = {
    val (avroNullable, resolvedAvroType) = resolveAvroType(avroType)
    warnNullabilityDifference(avroNullable, nullable)
    resolvedAvroType
  }

  /**
   * Check the nullability of the input Avro type and resolve it when it is nullable. The first
   * return value is a [[Boolean]] indicating if the input Avro type is nullable. The second
   * return value is the possibly resolved type.
   */
  private def resolveAvroType(avroType: HoodieSchema): (Boolean, HoodieSchema) = {
    if (avroType.getType == HoodieSchemaType.UNION) {
      val fields = avroType.getTypes.asScala
      val actualType = fields.filter(_.getType != HoodieSchemaType.NULL)
      if (fields.length == 2 && actualType.length == 1) {
        (true, actualType.head)
      } else {
        // This is just a normal union, not used to designate nullability
        (false, avroType)
      }
    } else {
      (false, avroType)
    }
  }

  /**
   * log a warning message if the nullability for Avro and catalyst types are different.
   */
  private def warnNullabilityDifference(avroNullable: Boolean, catalystNullable: Boolean): Unit = {
    if (avroNullable && !catalystNullable) {
      logWarning("Writing Avro files with nullable Avro schema and non-nullable catalyst schema.")
    }
    if (!avroNullable && catalystNullable) {
      logWarning("Writing Avro files with non-nullable Avro schema and nullable catalyst " +
        "schema will throw runtime exception if there is a record with null value.")
    }
  }
}

object AvroSerializer {

  // NOTE: Following methods have been renamed in Spark 3.2.1 [1] making [[AvroSerializer]] implementation
  //       (which relies on it) be only compatible with the exact same version of [[DataSourceUtils]].
  //       To make sure this implementation is compatible w/ all Spark versions w/in Spark 3.2.x branch,
  //       we're preemptively cloned those methods to make sure Hudi is compatible w/ Spark 3.2.0 as well as
  //       w/ Spark >= 3.2.1
  //
  // [1] https://github.com/apache/spark/pull/34978

  def createDateRebaseFuncInWrite(rebaseMode: LegacyBehaviorPolicy.Value,
                                  format: String): Int => Int = rebaseMode match {
    case LegacyBehaviorPolicy.EXCEPTION => days: Int =>
      if (days < RebaseDateTime.lastSwitchGregorianDay) {
        throw DataSourceUtils.newRebaseExceptionInWrite(format)
      }
      days
    case LegacyBehaviorPolicy.LEGACY => RebaseDateTime.rebaseGregorianToJulianDays
    case LegacyBehaviorPolicy.CORRECTED => identity[Int]
  }

  def createTimestampRebaseFuncInWrite(rebaseMode: LegacyBehaviorPolicy.Value,
                                       format: String): Long => Long = rebaseMode match {
    case LegacyBehaviorPolicy.EXCEPTION => micros: Long =>
      if (micros < RebaseDateTime.lastSwitchGregorianTs) {
        throw DataSourceUtils.newRebaseExceptionInWrite(format)
      }
      micros
    case LegacyBehaviorPolicy.LEGACY =>
      val timeZone = SQLConf.get.sessionLocalTimeZone
      RebaseDateTime.rebaseGregorianToJulianMicros(TimeZone.getTimeZone(timeZone), _)
    case LegacyBehaviorPolicy.CORRECTED => identity[Long]
  }

}
