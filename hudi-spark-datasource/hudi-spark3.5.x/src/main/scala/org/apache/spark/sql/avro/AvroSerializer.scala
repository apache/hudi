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

import org.apache.avro.Conversions.DecimalConversion
import org.apache.avro.LogicalTypes.{LocalTimestampMicros, LocalTimestampMillis, TimestampMicros, TimestampMillis}
import org.apache.avro.{LogicalTypes, Schema}
import org.apache.avro.Schema.Type
import org.apache.avro.Schema.Type._
import org.apache.avro.generic.GenericData.{EnumSymbol, Fixed, Record}
import org.apache.avro.util.Utf8
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.spark.internal.Logging
import org.apache.spark.sql.avro.AvroSerializer.{createDateRebaseFuncInWrite, createTimestampRebaseFuncInWrite}
import org.apache.spark.sql.avro.AvroUtils.{AvroMatchedField, toFieldStr}
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
                                  rootAvroType: HoodieSchema,
                                  nullable: Boolean,
                                  positionalFieldMatch: Boolean,
                                  datetimeRebaseMode: LegacyBehaviorPolicy.Value) extends Logging {

  def this(rootCatalystType: DataType, rootAvroType: HoodieSchema, nullable: Boolean) = {
    this(rootCatalystType, rootAvroType, nullable, positionalFieldMatch = false,
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
    val actualAvroType = resolveNullableType(rootAvroType.getAvroSchema, nullable)
    val baseConverter = try {
      rootCatalystType match {
        case st: StructType =>
          newStructConverter(st, actualAvroType, Nil, Nil).asInstanceOf[Any => Any]
        case _ =>
          val tmpRow = new SpecificInternalRow(Seq(rootCatalystType))
          val converter = newConverter(rootCatalystType, actualAvroType, Nil, Nil)
          (data: Any) =>
            tmpRow.update(0, data)
            converter.apply(tmpRow, 0)
      }
    } catch {
      case ise: IncompatibleSchemaException => throw new IncompatibleSchemaException(
        s"Cannot convert SQL type ${rootCatalystType.sql} to Avro type $rootAvroType.", ise)
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
                           avroType: Schema,
                           catalystPath: Seq[String],
                           avroPath: Seq[String]): Converter = {
    val errorPrefix = s"Cannot convert SQL ${toFieldStr(catalystPath)} " +
      s"to Avro ${toFieldStr(avroPath)} because "
    (catalystType, avroType.getType) match {
      case (NullType, NULL) =>
        (getter, ordinal) => null
      case (BooleanType, BOOLEAN) =>
        (getter, ordinal) => getter.getBoolean(ordinal)
      case (ByteType, INT) =>
        (getter, ordinal) => getter.getByte(ordinal).toInt
      case (ShortType, INT) =>
        (getter, ordinal) => getter.getShort(ordinal).toInt
      case (IntegerType, INT) =>
        (getter, ordinal) => getter.getInt(ordinal)
      case (LongType, LONG) =>
        (getter, ordinal) => getter.getLong(ordinal)
      case (FloatType, FLOAT) =>
        (getter, ordinal) => getter.getFloat(ordinal)
      case (DoubleType, DOUBLE) =>
        (getter, ordinal) => getter.getDouble(ordinal)
      case (d: DecimalType, FIXED)
        if avroType.getLogicalType == LogicalTypes.decimal(d.precision, d.scale) =>
        (getter, ordinal) =>
          val decimal = getter.getDecimal(ordinal, d.precision, d.scale)
          decimalConversions.toFixed(decimal.toJavaBigDecimal, avroType,
            LogicalTypes.decimal(d.precision, d.scale))

      case (d: DecimalType, BYTES)
        if avroType.getLogicalType == LogicalTypes.decimal(d.precision, d.scale) =>
        (getter, ordinal) =>
          val decimal = getter.getDecimal(ordinal, d.precision, d.scale)
          decimalConversions.toBytes(decimal.toJavaBigDecimal, avroType,
            LogicalTypes.decimal(d.precision, d.scale))

      case (StringType, ENUM) =>
        val enumSymbols: Set[String] = avroType.getEnumSymbols.asScala.toSet
        (getter, ordinal) =>
          val data = getter.getUTF8String(ordinal).toString
          if (!enumSymbols.contains(data)) {
            throw new IncompatibleSchemaException(errorPrefix +
              s""""$data" cannot be written since it's not defined in enum """ +
              enumSymbols.mkString("\"", "\", \"", "\""))
          }
          new EnumSymbol(avroType, data)

      case (StringType, STRING) =>
        (getter, ordinal) => new Utf8(getter.getUTF8String(ordinal).getBytes)

      case (BinaryType, FIXED) =>
        val size = avroType.getFixedSize
        (getter, ordinal) =>
          val data: Array[Byte] = getter.getBinary(ordinal)
          if (data.length != size) {
            def len2str(len: Int): String = s"$len ${if (len > 1) "bytes" else "byte"}"

            throw new IncompatibleSchemaException(errorPrefix + len2str(data.length) +
              " of binary data cannot be written into FIXED type with size of " + len2str(size))
          }
          new Fixed(avroType, data)

      case (BinaryType, BYTES) =>
        (getter, ordinal) => ByteBuffer.wrap(getter.getBinary(ordinal))

      case (DateType, INT) =>
        (getter, ordinal) => dateRebaseFunc(getter.getInt(ordinal))

      case (TimestampType, LONG) => avroType.getLogicalType match {
        // For backward compatibility, if the Avro type is Long and it is not logical type
        // (the `null` case), output the timestamp value as with millisecond precision.
        case null | _: TimestampMillis => (getter, ordinal) =>
          DateTimeUtils.microsToMillis(timestampRebaseFunc(getter.getLong(ordinal)))
        case _: TimestampMicros => (getter, ordinal) =>
          timestampRebaseFunc(getter.getLong(ordinal))
        case other => throw new IncompatibleSchemaException(errorPrefix +
          s"SQL type ${TimestampType.sql} cannot be converted to Avro logical type $other")
      }

      case (TimestampNTZType, LONG) => avroType.getLogicalType match {
        // To keep consistent with TimestampType, if the Avro type is Long and it is not
        // logical type (the `null` case), output the TimestampNTZ as long value
        // in millisecond precision.
        case null | _: LocalTimestampMillis => (getter, ordinal) =>
          DateTimeUtils.microsToMillis(getter.getLong(ordinal))
        case _: LocalTimestampMicros => (getter, ordinal) =>
          getter.getLong(ordinal)
        case other => throw new IncompatibleSchemaException(errorPrefix +
          s"SQL type ${TimestampNTZType.sql} cannot be converted to Avro logical type $other")
      }

      case (ArrayType(et, containsNull), ARRAY) =>
        val elementConverter = newConverter(
          et, resolveNullableType(avroType.getElementType, containsNull),
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

      case (st: StructType, RECORD) =>
        val structConverter = newStructConverter(st, avroType, catalystPath, avroPath)
        val numFields = st.length
        (getter, ordinal) => structConverter(getter.getStruct(ordinal, numFields))

      ////////////////////////////////////////////////////////////////////////////////////////////
      // Following section is amended to the original (Spark's) implementation
      // >>> BEGINS
      ////////////////////////////////////////////////////////////////////////////////////////////

      case (st: StructType, UNION) =>
        val unionConverter = newUnionConverter(st, avroType, catalystPath, avroPath)
        val numFields = st.length
        (getter, ordinal) => unionConverter(getter.getStruct(ordinal, numFields))

      ////////////////////////////////////////////////////////////////////////////////////////////
      // <<< ENDS
      ////////////////////////////////////////////////////////////////////////////////////////////

      case (MapType(kt, vt, valueContainsNull), MAP) if kt == StringType =>
        val valueConverter = newConverter(
          vt, resolveNullableType(avroType.getValueType, valueContainsNull),
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

      case (_: YearMonthIntervalType, INT) =>
        (getter, ordinal) => getter.getInt(ordinal)

      case (_: DayTimeIntervalType, LONG) =>
        (getter, ordinal) => getter.getLong(ordinal)

      case _ =>
        throw new IncompatibleSchemaException(errorPrefix +
          s"schema is incompatible (sqlType = ${catalystType.sql}, avroType = $avroType)")
    }
  }

  private def newStructConverter(catalystStruct: StructType,
                                 avroStruct: Schema,
                                 catalystPath: Seq[String],
                                 avroPath: Seq[String]): InternalRow => Record = {

    val avroSchemaHelper = new AvroUtils.AvroSchemaHelper(
      avroStruct, catalystStruct, avroPath, catalystPath, positionalFieldMatch)

    avroSchemaHelper.validateNoExtraCatalystFields(ignoreNullable = false)
    avroSchemaHelper.validateNoExtraRequiredAvroFields()

    val (avroIndices, fieldConverters) = avroSchemaHelper.matchedFields.map {
      case AvroMatchedField(catalystField, _, avroField) =>
        val converter = newConverter(catalystField.dataType,
          resolveNullableType(avroField.schema(), catalystField.nullable),
          catalystPath :+ catalystField.name, avroPath :+ avroField.name)
        (avroField.pos(), converter)
    }.toArray.unzip

    val numFields = catalystStruct.length
    row: InternalRow =>
      val result = new Record(avroStruct)
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
                                avroUnion: Schema,
                                catalystPath: Seq[String],
                                avroPath: Seq[String]): InternalRow => Any = {
    if (avroUnion.getType != UNION || !canMapUnion(catalystStruct, avroUnion)) {
      throw new IncompatibleSchemaException(s"Cannot convert Catalyst type $catalystStruct to " +
        s"Avro type $avroUnion.")
    }
    val nullable = avroUnion.getTypes.size() > 0 && avroUnion.getTypes.get(0).getType == Type.NULL
    val avroInnerTypes = if (nullable) {
      avroUnion.getTypes.asScala.tail
    } else {
      avroUnion.getTypes.asScala
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
              s"Avro union $avroUnion. Record has more than one optional values set")
          }
          result = fieldConverters(i).apply(row, i)
        }
        i += 1
      }
      if (!nullable && result == null) {
        throw new IncompatibleSchemaException(s"Cannot convert Catalyst record $catalystStruct to " +
          s"Avro union $avroUnion. Record has no values set, while should have exactly one")
      }
      result
  }

  private def canMapUnion(catalystStruct: StructType, avroStruct: Schema): Boolean = {
    (avroStruct.getTypes.size() > 0 &&
      avroStruct.getTypes.get(0).getType == Type.NULL &&
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
  private def resolveNullableType(avroType: Schema, nullable: Boolean): Schema = {
    val (avroNullable, resolvedAvroType) = resolveAvroType(avroType)
    warnNullabilityDifference(avroNullable, nullable)
    resolvedAvroType
  }

  /**
   * Check the nullability of the input Avro type and resolve it when it is nullable. The first
   * return value is a [[Boolean]] indicating if the input Avro type is nullable. The second
   * return value is the possibly resolved type.
   */
  private def resolveAvroType(avroType: Schema): (Boolean, Schema) = {
    if (avroType.getType == Type.UNION) {
      val fields = avroType.getTypes.asScala
      val actualType = fields.filter(_.getType != Type.NULL)
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
