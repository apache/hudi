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

package org.apache.hudi

import org.apache.hudi.expression.{Expression, Literal, NameReference, Predicates}
import org.apache.hudi.internal.schema.{Type, Types}

import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}

import scala.collection.JavaConverters._

object SparkFilterHelper {

  def convertFilters(filters: Seq[Filter]): Expression = {
    filters.flatMap(convertFilter)
      .reduceLeftOption(Predicates.and)
      .getOrElse(Predicates.alwaysTrue())
  }

  def convertFilter(filter: Filter): Option[Expression] = filter match {
    case EqualTo(attribute, value) =>
      Some(Predicates.eq(new NameReference(attribute), toLiteral(value)))
    case EqualNullSafe(attribute, value) =>
      Some(Predicates.eq(new NameReference(attribute), toLiteral(value)))
    case LessThan(attribute, value) =>
      Some(Predicates.lt(new NameReference(attribute), toLiteral(value)))
    case LessThanOrEqual(attribute, value) =>
      Some(Predicates.lteq(new NameReference(attribute), toLiteral(value)))
    case GreaterThan(attribute, value) =>
      Some(Predicates.gt(new NameReference(attribute), toLiteral(value)))
    case GreaterThanOrEqual(attribute, value) =>
      Some(Predicates.gteq(new NameReference(attribute), toLiteral(value)))
    case In(attribute, values) =>
      Some(Predicates.in(new NameReference(attribute), values.map(toLiteral(_).asInstanceOf[Expression]).toList.asJava))
    case And(left, right) =>
      for {
        convertedLeft <- convertFilter(left)
        convertedRight <- convertFilter(right)
      } yield Predicates.and(convertedLeft, convertedRight)
    case Or(left, right) =>
      for {
        convertedLeft <- convertFilter(left)
        convertedRight <- convertFilter(right)
      } yield Predicates.or(convertedLeft, convertedRight)
    case StringStartsWith(attribute, value) =>
      Some(Predicates.startsWith(new NameReference(attribute), toLiteral(value)))
    case StringContains(attribute, value) =>
      Some(Predicates.contains(new NameReference(attribute), toLiteral(value)))
    case Not(child) =>
      convertFilter(child).map(Predicates.not)
    case IsNull(attribute) =>
      Some(Predicates.isNull(new NameReference(attribute)))
    case IsNotNull(attribute) =>
      Some(Predicates.isNotNull(new NameReference(attribute)))
    case _ =>
      None
  }

  def toLiteral(value: Any): Literal[_] = {
    value match {
      case timestamp : Timestamp =>
        new Literal(DateTimeUtils.fromJavaTimestamp(timestamp), Types.TimestampType.get())
      case date: Date =>
        new Literal(DateTimeUtils.fromJavaDate(date), Types.DateType.get())
      case instant: Instant =>
        new Literal(DateTimeUtils.instantToMicros(instant), Types.TimestampType.get())
      case localDate: LocalDate =>
        new Literal(Math.toIntExact(localDate.toEpochDay), Types.TimestampType.get())
      case _ =>
        Literal.from(value)
    }
  }

  def convertDataType(sparkType: DataType): Type = sparkType match {
    case StructType(fields) =>
      val convertedFields = fields.zipWithIndex.map {
        case (field, idx) =>
          Types.Field.get(idx, field.nullable, field.name, convertDataType(field.dataType), field.getComment().orNull)
      }.toList.asJava
      Types.RecordType.get(convertedFields)
    case BooleanType =>
      Types.BooleanType.get()
    case IntegerType | ShortType | ByteType =>
      Types.IntType.get()
    case LongType =>
      Types.LongType.get()
    case FloatType =>
      Types.FloatType.get()
    case DoubleType =>
      Types.DoubleType.get()
    case StringType | CharType(_) | VarcharType(_) =>
      Types.StringType.get()
    case DateType =>
      Types.DateType.get()
    case TimestampType =>
      Types.TimestampType.get()
    case _type: DecimalType =>
      Types.DecimalType.get(_type.precision, _type.scale)
    case _ =>
      throw new UnsupportedOperationException(s"Cannot convert spark type $sparkType to the relate HUDI type")
  }
}
