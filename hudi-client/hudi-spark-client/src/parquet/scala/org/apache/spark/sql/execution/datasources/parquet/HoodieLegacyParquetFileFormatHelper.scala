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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.spark.sql.HoodieSchemaUtils
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.expressions.{ArrayTransform, Attribute, Cast, CreateNamedStruct, CreateStruct, Expression, GetStructField, LambdaFunction, Literal, MapEntries, MapFromEntries, NamedLambdaVariable, UnsafeProjection}
import org.apache.spark.sql.types._

object HoodieLegacyParquetFileFormatHelper {
  def generateUnsafeProjection(fullSchema: Seq[Attribute],
                               timeZoneId: Option[String],
                               typeChangeInfos: java.util.Map[Integer, org.apache.hudi.common.util.collection.Pair[DataType, DataType]],
                               requiredSchema: StructType,
                               partitionSchema: StructType,
                               schemaUtils: HoodieSchemaUtils): UnsafeProjection = {
    val addedCastCache = scala.collection.mutable.HashMap.empty[(DataType, DataType), Boolean]

    def hasUnsupportedConversion(src: DataType, dst: DataType): Boolean = {
      addedCastCache.getOrElseUpdate((src, dst), {
        (src, dst) match {
          case (FloatType, DoubleType) => true
          case (IntegerType, DecimalType()) => true
          case (LongType, DecimalType()) => true
          case (FloatType, DecimalType()) => true
          case (DoubleType, DecimalType()) => true
          case (StringType, DecimalType()) => true
          case (StringType, DateType) => true
          case (StructType(srcFields), StructType(dstFields)) =>
            srcFields.zip(dstFields).exists { case (sf, df) => hasUnsupportedConversion(sf.dataType, df.dataType) }
          case (ArrayType(sElem, _), ArrayType(dElem, _)) =>
            hasUnsupportedConversion(sElem, dElem)
          case (MapType(sKey, sVal, _), MapType(dKey, dVal, _)) =>
            hasUnsupportedConversion(sKey, dKey) || hasUnsupportedConversion(sVal, dVal)
          case _ => false
        }
      })
    }

    def recursivelyCastExpressions(expr: Expression, srcType: DataType, dstType: DataType): Expression = {
      lazy val needTimeZone = Cast.needsTimeZone(srcType, dstType)
      (srcType, dstType) match {
        case (FloatType, DoubleType) =>
          val toStr = Cast(expr, StringType, if (needTimeZone) timeZoneId else None)
          Cast(toStr, dstType, if (needTimeZone) timeZoneId else None)
        case (IntegerType | LongType | FloatType | DoubleType, dec: DecimalType) =>
          val toStr = Cast(expr, StringType, if (needTimeZone) timeZoneId else None)
          Cast(toStr, dec, if (needTimeZone) timeZoneId else None)
        case (StringType, dec: DecimalType) =>
          Cast(expr, dec, if (needTimeZone) timeZoneId else None)
        case (StringType, DateType) =>
          Cast(expr, DateType, if (needTimeZone) timeZoneId else None)
        case (s: StructType, d: StructType) if hasUnsupportedConversion(s, d) =>
          val structFields = s.fields.zip(d.fields).zipWithIndex.map {
            case ((srcField, dstField), i) =>
              val child = GetStructField(expr, i, Some(dstField.name))
              recursivelyCastExpressions(child, srcField.dataType, dstField.dataType)
          }
          CreateNamedStruct(d.fields.zip(structFields).flatMap {
            case (f, c) => Seq(Literal(f.name), c)
          })
        case (ArrayType(sElementType, containsNull), ArrayType(dElementType, _)) if hasUnsupportedConversion(sElementType, dElementType) =>
          val lambdaVar = NamedLambdaVariable("element", sElementType, containsNull)
          val body = recursivelyCastExpressions(lambdaVar, sElementType, dElementType)
          val func = LambdaFunction(body, Seq(lambdaVar))
          ArrayTransform(expr, func)
        case (MapType(sKeyType, sValType, vnull), MapType(dKeyType, dValType, _))
          if hasUnsupportedConversion(sKeyType, dKeyType) || hasUnsupportedConversion(sValType, dValType) =>
          val kv = NamedLambdaVariable("kv", new StructType()
            .add("key", sKeyType, nullable = false)
            .add("value", sValType, nullable = vnull), nullable = false)
          val newKey = recursivelyCastExpressions(GetStructField(kv, 0), sKeyType, dKeyType)
          val newVal = recursivelyCastExpressions(GetStructField(kv, 1), sValType, dValType)
          val entry = CreateStruct(Seq(newKey, newVal))
          val func = LambdaFunction(entry, Seq(kv))
          val transformed = ArrayTransform(MapEntries(expr), func)
          MapFromEntries(transformed)
        case _ =>
          // most cases should be covered here we only need to do the recursive work for float to double
          Cast(expr, dstType, if (needTimeZone) timeZoneId else None)
      }
    }

    if (typeChangeInfos.isEmpty) {
      GenerateUnsafeProjection.generate(fullSchema, fullSchema)
    } else {
      // find type changed.
      val newSchema = new StructType(requiredSchema.fields.zipWithIndex.map { case (f, i) =>
        if (typeChangeInfos.containsKey(i)) {
          StructField(f.name, typeChangeInfos.get(i).getRight, f.nullable, f.metadata)
        } else f
      })
      val newFullSchema = schemaUtils.toAttributes(newSchema) ++ schemaUtils.toAttributes(partitionSchema)
      val castSchema = newFullSchema.zipWithIndex.map { case (attr, i) =>
        if (typeChangeInfos.containsKey(i)) {
          val srcType = typeChangeInfos.get(i).getRight
          val dstType = typeChangeInfos.get(i).getLeft
          recursivelyCastExpressions(attr, srcType, dstType)
        } else attr
      }
      GenerateUnsafeProjection.generate(castSchema, newFullSchema)
    }
  }
}
