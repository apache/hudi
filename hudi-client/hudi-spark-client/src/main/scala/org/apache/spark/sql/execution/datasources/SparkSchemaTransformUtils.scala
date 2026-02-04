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

import org.apache.spark.sql.HoodieSchemaUtils
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.expressions.{ArrayTransform, Attribute, AttributeReference, Cast, CreateNamedStruct, CreateStruct, Expression, GetStructField, LambdaFunction, Literal, MapEntries, MapFromEntries, NamedLambdaVariable, UnsafeProjection}
import org.apache.spark.sql.types.{ArrayType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, StringType, StructField, StructType, TimestampNTZType}

/**
 * Format-agnostic utilities for Spark schema transformations including NULL padding
 * and recursive type casting with workarounds for unsafe conversions.
 *
 * These utilities are used by file format readers that need to:
 * - Pad missing columns with NULL literals (required for Lance)
 * - Handle nested struct/array/map type conversions
 * - Work around Spark unsafe cast issues (float->double, numeric->decimal)
 *
 * Note: The following functions were originally part of HoodieParquetFileFormatHelper
 * and have been moved here to allow reuse across multiple file formats:
 * - buildImplicitSchemaChangeInfo
 * - isDataTypeEqual
 * - generateUnsafeProjection
 * - hasUnsupportedConversion
 * - recursivelyCastExpressions
 */
object SparkSchemaTransformUtils {

  /**
   * Generate UnsafeProjection for type casting with special handling for unsupported conversions.
   *
   * @param fullSchema Complete schema including data and partition columns
   * @param timeZoneId Session timezone for timestamp conversions
   * @param typeChangeInfos Map of field index to (targetType, readerType) for fields needing casting
   * @param requiredSchema Schema requested by the query (data columns only)
   * @param partitionSchema Schema of partition columns
   * @param schemaUtils Spark adapter schema utilities
   * @return UnsafeProjection that applies type casting to rows
   */
  def generateUnsafeProjection(fullSchema: Seq[Attribute],
                               timeZoneId: Option[String],
                               typeChangeInfos: java.util.Map[Integer, org.apache.hudi.common.util.collection.Pair[DataType, DataType]],
                               requiredSchema: StructType,
                               partitionSchema: StructType,
                               schemaUtils: HoodieSchemaUtils): UnsafeProjection = {
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
          SparkSchemaTransformUtils.recursivelyCastExpressions(
            attr, srcType, dstType, timeZoneId
          )
        } else attr
      }
      GenerateUnsafeProjection.generate(castSchema, newFullSchema)
    }
  }

  /**
   * Generate UnsafeProjection that pads missing columns with NULL literals.
   *
   * @param inputSchema Schema from file (fields actually present)
   * @param targetSchema Target output schema (may have more fields than file)
   * @return UnsafeProjection that pads missing columns with NULLs
   */
  def generateNullPaddingProjection(
      inputSchema: StructType,
      targetSchema: StructType
  ): UnsafeProjection = {
    val inputAttributes = inputSchema.fields.map(f =>
      AttributeReference(f.name, f.dataType, f.nullable)())
    val inputFieldMap = inputAttributes.map(a => a.name -> a).toMap

    // Build expressions for all target fields, padding missing columns with NULL
    val expressions = targetSchema.fields.map { field =>
      inputFieldMap.get(field.name) match {
        case Some(attr) =>
          // Field exists in input - check if nested padding needed
          if (needsNestedPadding(attr.dataType, field.dataType)) {
            recursivelyPadExpression(attr, attr.dataType, field.dataType)
          } else {
            attr
          }
        case None =>
          // Field missing from input, use NULL literal for padding
          Literal(null, field.dataType)
      }
    }

    GenerateUnsafeProjection.generate(expressions, inputAttributes)
  }

  /**
   * Recursively pad nested struct/array/map fields with NULLs.
   *
   * @param expr Source expression
   * @param srcType Source data type
   * @param dstType Destination data type (may have additional nested fields)
   * @return Expression with NULL padding for missing nested fields
   */
  private def recursivelyPadExpression(
      expr: Expression,
      srcType: DataType,
      dstType: DataType
  ): Expression = (srcType, dstType) match {
    case (s: StructType, d: StructType) =>
      val srcFieldMap = s.fields.zipWithIndex.map { case (f, i) => f.name -> (f, i) }.toMap
      val structFields = d.fields.map { dstField =>
        srcFieldMap.get(dstField.name) match {
          case Some((srcField, srcIndex)) =>
            val child = GetStructField(expr, srcIndex, Some(dstField.name))
            recursivelyPadExpression(child, srcField.dataType, dstField.dataType)
          case None =>
            Literal(null, dstField.dataType)
        }
      }
      CreateNamedStruct(d.fields.zip(structFields).flatMap {
        case (f, c) => Seq(Literal(f.name), c)
      })

    case (ArrayType(sElementType, containsNull), ArrayType(dElementType, _))
        if needsNestedPadding(sElementType, dElementType) =>
      val lambdaVar = NamedLambdaVariable("element", sElementType, containsNull)
      val body = recursivelyPadExpression(lambdaVar, sElementType, dElementType)
      val func = LambdaFunction(body, Seq(lambdaVar))
      ArrayTransform(expr, func)

    case (MapType(sKeyType, sValType, vnull), MapType(dKeyType, dValType, _))
        if needsNestedPadding(sKeyType, dKeyType) || needsNestedPadding(sValType, dValType) =>
      val kv = NamedLambdaVariable("kv", new StructType()
        .add("key", sKeyType, nullable = false)
        .add("value", sValType, nullable = vnull), nullable = false)
      val newKey = recursivelyPadExpression(GetStructField(kv, 0), sKeyType, dKeyType)
      val newVal = recursivelyPadExpression(GetStructField(kv, 1), sValType, dValType)
      val entry = CreateStruct(Seq(newKey, newVal))
      val func = LambdaFunction(entry, Seq(kv))
      val transformed = ArrayTransform(MapEntries(expr), func)
      MapFromEntries(transformed)

    case _ =>
      // No padding needed, return expression as-is
      expr
  }

  /**
   * Recursively cast expressions with special handling for unsupported conversions.
   *
   * @param expr Source expression to cast
   * @param srcType Source data type
   * @param dstType Destination data type
   * @param timeZoneId Session timezone for timestamp conversions
   * @return Casted expression with workarounds for unsafe conversions
   */
  def recursivelyCastExpressions(
      expr: Expression,
      srcType: DataType,
      dstType: DataType,
      timeZoneId: Option[String]
  ): Expression = {
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
            recursivelyCastExpressions(child, srcField.dataType, dstField.dataType, timeZoneId)
        }
        CreateNamedStruct(d.fields.zip(structFields).flatMap {
          case (f, c) => Seq(Literal(f.name), c)
        })
      case (ArrayType(sElementType, containsNull), ArrayType(dElementType, _)) if hasUnsupportedConversion(sElementType, dElementType) =>
        val lambdaVar = NamedLambdaVariable("element", sElementType, containsNull)
        val body = recursivelyCastExpressions(lambdaVar, sElementType, dElementType, timeZoneId)
        val func = LambdaFunction(body, Seq(lambdaVar))
        ArrayTransform(expr, func)
      case (MapType(sKeyType, sValType, vnull), MapType(dKeyType, dValType, _))
        if hasUnsupportedConversion(sKeyType, dKeyType) || hasUnsupportedConversion(sValType, dValType) =>
        val kv = NamedLambdaVariable("kv", new StructType()
          .add("key", sKeyType, nullable = false)
          .add("value", sValType, nullable = vnull), nullable = false)
        val newKey = recursivelyCastExpressions(GetStructField(kv, 0), sKeyType, dKeyType, timeZoneId)
        val newVal = recursivelyCastExpressions(GetStructField(kv, 1), sValType, dValType, timeZoneId)
        val entry = CreateStruct(Seq(newKey, newVal))
        val func = LambdaFunction(entry, Seq(kv))
        val transformed = ArrayTransform(MapEntries(expr), func)
        MapFromEntries(transformed)
      case _ =>
        // most cases should be covered here we only need to do the recursive work for float to double
        Cast(expr, dstType, if (needTimeZone) timeZoneId else None)
    }
  }

  /**
   * Used to determine if padding is required for nested struct fields.
   * @param srcType Source data type
   * @param dstType Destination data type
   * @return true if destination has additional fields requiring NULL padding
   */
  private def needsNestedPadding(srcType: DataType, dstType: DataType): Boolean = (srcType, dstType) match {
    // Need padding if destination has more fields or nested fields differ
    case (StructType(srcFields), StructType(dstFields)) =>
      dstFields.length > srcFields.length ||
        srcFields.zip(dstFields).exists { case (sf, df) => needsNestedPadding(sf.dataType, df.dataType) }
    case (ArrayType(srcElem, _), ArrayType(dstElem, _)) =>
      needsNestedPadding(srcElem, dstElem)
    case (MapType(srcKey, srcVal, _), MapType(dstKey, dstVal, _)) =>
      // lance does not support map type so not needed currently
      needsNestedPadding(srcKey, dstKey) || needsNestedPadding(srcVal, dstVal)
    case _ => false
  }

  /**
   * Filter requested schema to only include fields that exist in file schema.
   * Used by readers that can only read columns present in the file.
   *
   * @param requestedSchema Schema requested by the query
   * @param fileSchema Schema from the file
   * @return Filtered schema containing only fields present in file
   */
  def filterSchemaByFileSchema(requestedSchema: StructType, fileSchema: StructType): StructType = {
    val fileFieldMap = fileSchema.fields.map(f => f.name -> f).toMap

    val filteredFields = requestedSchema.fields.flatMap { requestedField =>
      fileFieldMap.get(requestedField.name).map { fileField =>
        val filteredDataType = filterDataType(requestedField.dataType, fileField.dataType)
        StructField(requestedField.name, filteredDataType, requestedField.nullable, requestedField.metadata)
      }
    }

    StructType(filteredFields)
  }

  /**
   * Recursively filter data types to only include nested fields that exist in file.
   */
  private def filterDataType(requestedType: DataType, fileType: DataType): DataType = (requestedType, fileType) match {
    case (requestedType, fileType) if requestedType == fileType => fileType

    case (ArrayType(requestedElement, containsNull), ArrayType(fileElement, _)) =>
      ArrayType(filterDataType(requestedElement, fileElement), containsNull)

    case (MapType(requestedKey, requestedValue, valueContainsNull), MapType(fileKey, fileValue, _)) =>
      MapType(
        filterDataType(requestedKey, fileKey),
        filterDataType(requestedValue, fileValue),
        valueContainsNull
      )

    case (StructType(requestedFields), StructType(fileFields)) =>
      val fileFieldMap = fileFields.map(f => f.name -> f).toMap
      val filteredFields = requestedFields.flatMap { requestedField =>
        fileFieldMap.get(requestedField.name).map { fileField =>
          StructField(
            requestedField.name,
            filterDataType(requestedField.dataType, fileField.dataType),
            requestedField.nullable,
            requestedField.metadata
          )
        }
      }
      StructType(filteredFields)

    case _ => requestedType
  }

  /**
   * Check if a type conversion requires special handling (unsafe cast workaround).
   * Caches results for performance.
   *
   * @param src Source data type
   * @param dst Destination data type
   * @return true if conversion needs special handling (e.g., float->double via string)
   */
  def hasUnsupportedConversion(src: DataType, dst: DataType): Boolean = {
    val addedCastCache = scala.collection.mutable.HashMap.empty[(DataType, DataType), Boolean]
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

  /**
   * Build schema change information by comparing file schema to required schema.
   * Analyzes schema differences to determine:
   * 1. Which fields need type conversions (stored in typeChangeInfos map)
   * 2. The adjusted reader schema to use when reading the file
   *
   * @param fileStruct Schema from the file (as Spark StructType)
   * @param requiredSchema Schema requested by the query
   * @return Tuple of (typeChangeInfos map, adjusted reader schema)
   */
  def buildImplicitSchemaChangeInfo(
      fileStruct: StructType,
      requiredSchema: StructType
  ): (java.util.Map[Integer, org.apache.hudi.common.util.collection.Pair[DataType, DataType]], StructType) = {
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

  /**
   * Check if two Spark data types are equal for schema evolution purposes.
   * Ignores nullability and handles special cases like TimestampNTZ stored as Long.
   *
   * @param requiredType Type requested by query
   * @param fileType Type from file schema
   * @return true if types are compatible for reading
   */
  def isDataTypeEqual(requiredType: DataType, fileType: DataType): Boolean = (requiredType, fileType) match {
    case (requiredType, fileType) if requiredType == fileType => true

    // prevent illegal cast - TimestampNTZ can be stored as Long in files
    case (TimestampNTZType, LongType) => true

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

  /**
   * Add missing nested fields from required type to file type.
   * Used during schema evolution when query expects fields that don't exist in file.
   *
   * @param requiredType Type requested by query (may have extra fields)
   * @param fileType Type from file schema
   * @return Reconciled type that includes both file and required fields
   */
  def addMissingFields(requiredType: DataType, fileType: DataType): DataType = (requiredType, fileType) match {
    case (requiredType, fileType) if requiredType == fileType => fileType
    case (ArrayType(rt, _), ArrayType(ft, _)) => ArrayType(addMissingFields(rt, ft))
    case (MapType(requiredKey, requiredValue, _), MapType(fileKey, fileValue, _)) =>
      MapType(addMissingFields(requiredKey, fileKey), addMissingFields(requiredValue, fileValue))
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
}
