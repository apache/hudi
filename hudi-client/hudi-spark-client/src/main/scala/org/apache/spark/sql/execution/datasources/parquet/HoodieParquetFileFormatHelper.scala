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

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.metadata.FileMetaData
import org.apache.spark.sql.HoodieSchemaUtils
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.expressions.{ArrayTransform, Attribute, Cast, CreateNamedStruct, CreateStruct, Expression, GetStructField, LambdaFunction, Literal, MapEntries, MapFromEntries, NamedLambdaVariable, UnsafeProjection}
import org.apache.spark.sql.types.{ArrayType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, StringType, StructField, StructType}

object HoodieParquetFileFormatHelper {

  def buildImplicitSchemaChangeInfo(hadoopConf: Configuration,
                                    parquetFileMetaData: FileMetaData,
                                    requiredSchema: StructType): (java.util.Map[Integer, org.apache.hudi.common.util.collection.Pair[DataType, DataType]], StructType) = {
    val convert = new ParquetToSparkSchemaConverter(hadoopConf)
    val fileStruct = convert.convert(parquetFileMetaData.getSchema)
    buildImplicitSchemaChangeInfo(fileStruct, requiredSchema)
  }

  def buildImplicitSchemaChangeInfo(fileStruct: StructType,
                                    requiredSchema: StructType): (java.util.Map[Integer, org.apache.hudi.common.util.collection.Pair[DataType, DataType]], StructType) = {
    val implicitTypeChangeInfo: java.util.Map[Integer, org.apache.hudi.common.util.collection.Pair[DataType, DataType]] = new java.util.HashMap()

    val fileStructMap = fileStruct.fields.map(f => (f.name, f.dataType)).toMap
    // if there are missing fields or if field's data type needs to be changed while reading, we handle it here.
    val sparkRequestStructFields = requiredSchema.map(f => {
      val requiredType = f.dataType
      if (fileStructMap.contains(f.name) && !isDataTypeEqual(requiredType, fileStructMap(f.name))) {
        val readerType = addMissingFields(requiredType, fileStructMap(f.name))
        implicitTypeChangeInfo.put(new Integer(requiredSchema.fieldIndex(f.name)), org.apache.hudi.common.util.collection.Pair.of(requiredType, readerType))
        StructField(f.name, readerType, f.nullable)
      } else {
        f
      }
    })
    (implicitTypeChangeInfo, StructType(sparkRequestStructFields))
  }

  def isDataTypeEqual(requiredType: DataType, fileType: DataType): Boolean = (requiredType, fileType) match {
    case (requiredType, fileType) if requiredType == fileType => true

    case (ArrayType(rt, _), ArrayType(ft, _)) =>
      // Do not care about nullability as schema evolution require fields to be nullable
      isDataTypeEqual(rt, ft)

    case (MapType(requiredKey, requiredValue, _), MapType(fileKey, fileValue, _)) =>
      // Likewise, do not care about nullability as schema evolution require fields to be nullable
      isDataTypeEqual(requiredKey, fileKey) && isDataTypeEqual(requiredValue, fileValue)

    case (StructType(requiredFields), StructType(fileFields)) =>
      // Find fields that are in requiredFields and fileFields as they might not be the same during add column + change column operations
      val commonFieldNames = requiredFields.map(_.name) intersect fileFields.map(_.name)

      // Need to match by name instead of StructField as name will stay the same whilst type may change
      val fileFilteredFields = fileFields.filter(f => commonFieldNames.contains(f.name)).sortWith(_.name < _.name)
      val requiredFilteredFields = requiredFields.filter(f => commonFieldNames.contains(f.name)).sortWith(_.name < _.name)

      // Sorting ensures that the same field names are being compared for type differences
      requiredFilteredFields.zip(fileFilteredFields).forall {
        case (requiredField, fileFilteredField) =>
          isDataTypeEqual(requiredField.dataType, fileFilteredField.dataType)
      }

    case _ => false
  }

  def addMissingFields(requiredType: DataType, fileType: DataType): DataType = (requiredType, fileType) match {
    case (requiredType, fileType) if requiredType == fileType => fileType
    case (ArrayType(rt, _), ArrayType(ft, _)) => ArrayType(addMissingFields(rt, ft))
    case (MapType(requiredKey, requiredValue, _), MapType(fileKey, fileValue, _)) => MapType(addMissingFields(requiredKey, fileKey), addMissingFields(requiredValue, fileValue))
    case (StructType(requiredFields), StructType(fileFields)) =>
      val fileFieldMap = fileFields.map(f => f.name -> f).toMap
      StructType(requiredFields.map(f => {
        fileFieldMap.get(f.name) match {
          case Some(ff) => StructField(ff.name, addMissingFields(f.dataType, ff.dataType), ff.nullable, ff.metadata)
          case None => f
        }
      }))
    case _ => fileType
  }

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
