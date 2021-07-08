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

package org.apache.spark.sql.hudi.command.payload

import java.math.BigDecimal
import java.nio.ByteBuffer

import org.apache.avro.Conversions.DecimalConversion
import org.apache.avro.LogicalTypes.{TimestampMicros, TimestampMillis}
import org.apache.avro.Schema.Type._
import org.apache.avro.generic.{GenericFixed, IndexedRecord}
import org.apache.avro.util.Utf8
import org.apache.avro.{LogicalTypes, Schema}
import org.apache.spark.sql.avro.{IncompatibleSchemaException, SchemaConverters}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._

/**
  * A sql typed record which will convert the avro field to sql typed value.
  * This is referred to the org.apache.spark.sql.avro.AvroDeserializer#newWriter in spark project.
  * @param record
  */
class SqlTypedRecord(val record: IndexedRecord) extends IndexedRecord {

  private lazy val decimalConversions = new DecimalConversion()
  private lazy val sqlType = SchemaConverters.toSqlType(getSchema).dataType.asInstanceOf[StructType]

  override def put(i: Int, v: Any): Unit = {
    record.put(i, v)
  }

  override def get(i: Int): AnyRef = {
    val value = record.get(i)
    val avroFieldType = getSchema.getFields.get(i).schema()
    val sqlFieldType = sqlType.fields(i).dataType
    if (value == null) {
      null
    } else {
      convert(avroFieldType, sqlFieldType, value)
    }
  }

  private def convert(avroFieldType: Schema, sqlFieldType: DataType, value: AnyRef): AnyRef = {
    (avroFieldType.getType, sqlFieldType) match {
      case (NULL, NullType) => null

      case (BOOLEAN, BooleanType) => value.asInstanceOf[Boolean].asInstanceOf[java.lang.Boolean]

      case (INT, IntegerType) => value.asInstanceOf[Int].asInstanceOf[java.lang.Integer]

      case (INT, DateType) => value.asInstanceOf[Int].asInstanceOf[java.lang.Integer]

      case (LONG, LongType) => value.asInstanceOf[Long].asInstanceOf[java.lang.Long]

      case (LONG, TimestampType) => avroFieldType.getLogicalType match {
        case _: TimestampMillis => (value.asInstanceOf[Long] * 1000).asInstanceOf[java.lang.Long]
        case _: TimestampMicros => value.asInstanceOf[Long].asInstanceOf[java.lang.Long]
        case null =>
          // For backward compatibility, if the Avro type is Long and it is not logical type,
          // the value is processed as timestamp type with millisecond precision.
          java.lang.Long.valueOf(value.asInstanceOf[Long] * 1000)
        case other => throw new IncompatibleSchemaException(
          s"Cannot convert Avro logical type ${other} to Catalyst Timestamp type.")
      }

      // Before we upgrade Avro to 1.8 for logical type support, spark-avro converts Long to Date.
      // For backward compatibility, we still keep this conversion.
      case (LONG, DateType) =>
        java.lang.Integer.valueOf((value.asInstanceOf[Long] / SqlTypedRecord.MILLIS_PER_DAY).toInt)

      case (FLOAT, FloatType) => value.asInstanceOf[Float].asInstanceOf[java.lang.Float]

      case (DOUBLE, DoubleType) => value.asInstanceOf[Double].asInstanceOf[java.lang.Double]

      case (STRING, StringType) => value match {
        case s: String => UTF8String.fromString(s)
        case s: Utf8 => UTF8String.fromString(s.toString)
        case o => throw new IllegalArgumentException(s"Cannot convert $o to StringType")
      }

      case (ENUM, StringType) => value.toString

      case (FIXED, BinaryType) => value.asInstanceOf[GenericFixed].bytes().clone()

      case (BYTES, BinaryType) => value match {
        case b: ByteBuffer =>
          val bytes = new Array[Byte](b.remaining)
          b.get(bytes)
          bytes
        case b: Array[Byte] => b
        case other => throw new RuntimeException(s"$other is not a valid avro binary.")
      }

      case (FIXED, d: DecimalType) =>
        val bigDecimal = decimalConversions.fromFixed(value.asInstanceOf[GenericFixed], avroFieldType,
          LogicalTypes.decimal(d.precision, d.scale))
        createDecimal(bigDecimal, d.precision, d.scale)

      case (BYTES, d: DecimalType) =>
        val bigDecimal = decimalConversions.fromBytes(value.asInstanceOf[ByteBuffer], avroFieldType,
          LogicalTypes.decimal(d.precision, d.scale))
        createDecimal(bigDecimal, d.precision, d.scale)

      case (RECORD, _: StructType) =>
        throw new IllegalArgumentException(s"UnSupport StructType yet")

      case (ARRAY, ArrayType(_, _)) =>
        throw new IllegalArgumentException(s"UnSupport ARRAY type yet")

      case (MAP, MapType(keyType, _, _)) if keyType == StringType =>
        throw new IllegalArgumentException(s"UnSupport MAP type yet")

      case (UNION, _) =>
        val allTypes = avroFieldType.getTypes.asScala
        val nonNullTypes = allTypes.filter(_.getType != NULL)
        if (nonNullTypes.nonEmpty) {
          if (nonNullTypes.length == 1) {
            convert(nonNullTypes.head, sqlFieldType, value)
          } else {
            nonNullTypes.map(_.getType) match {
              case Seq(a, b) if Set(a, b) == Set(INT, LONG) && sqlFieldType == LongType =>
                value match {
                  case null => null
                  case l: java.lang.Long => l
                  case i: java.lang.Integer => i.longValue().asInstanceOf[java.lang.Long]
                }

              case Seq(a, b) if Set(a, b) == Set(FLOAT, DOUBLE) && sqlFieldType == DoubleType =>
                value match {
                  case null => null
                  case d: java.lang.Double => d
                  case f: java.lang.Float => f.doubleValue().asInstanceOf[java.lang.Double]
                }

              case _ =>
                throw new IllegalArgumentException(s"UnSupport UNION type: ${sqlFieldType}")
            }
          }
        } else {
          null
        }
      case _ =>
        throw new IncompatibleSchemaException(
          s"Cannot convert Avro to catalyst because schema  " +
            s"is not compatible (avroType = $avroFieldType, sqlType = $sqlFieldType).\n")
    }
  }

  private def createDecimal(decimal: BigDecimal, precision: Int, scale: Int): Decimal = {
    if (precision <= Decimal.MAX_LONG_DIGITS) {
      // Constructs a `Decimal` with an unscaled `Long` value if possible.
      Decimal(decimal.unscaledValue().longValue(), precision, scale)
    } else {
      // Otherwise, resorts to an unscaled `BigInteger` instead.
      Decimal(decimal, precision, scale)
    }
  }

  override def getSchema: Schema = record.getSchema
}

object SqlTypedRecord {
  val MILLIS_PER_DAY = 24 * 60 * 60 * 1000L
}
