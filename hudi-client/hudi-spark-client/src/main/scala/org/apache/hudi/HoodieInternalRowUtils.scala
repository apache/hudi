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

package org.apache.hudi

import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable

import org.apache.avro.Schema
import org.apache.hudi.avro.HoodieAvroUtils.{createFullName, fromJavaDate, toJavaDate}
import org.apache.hudi.common.model.HoodieRecord.HoodieMetadataField
import org.apache.hudi.exception.HoodieException

import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, JoinedRow, MutableProjection, Projection}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, GenericArrayData, MapData}
import org.apache.spark.sql.types._

object HoodieInternalRowUtils {

  val projectionMap = new ConcurrentHashMap[(StructType, StructType), MutableProjection]
  val schemaMap = new ConcurrentHashMap[Schema, StructType]
  val SchemaPosMap = new ConcurrentHashMap[StructType, Map[String, (StructField, Int)]]

  /**
   * @see org.apache.hudi.avro.HoodieAvroUtils#stitchRecords(org.apache.avro.generic.GenericRecord, org.apache.avro.generic.GenericRecord, org.apache.avro.Schema)
   */
  def stitchRecords(left: InternalRow, leftSchema: StructType, right: InternalRow, rightSchema: StructType, stitchedSchema: StructType): InternalRow = {
    val mergeSchema = StructType(leftSchema.fields ++ rightSchema.fields)
    val row = new JoinedRow(left, right)
    val projection = getCacheProjection(mergeSchema, stitchedSchema)
    projection(row)
  }

  /**
   * @see org.apache.hudi.avro.HoodieAvroUtils#rewriteRecord(org.apache.avro.generic.GenericRecord, org.apache.avro.Schema)
   */
  def rewriteRecord(oldRecord: InternalRow, oldSchema: StructType, newSchema: StructType): InternalRow = {
    val newRow = new GenericInternalRow(Array.fill(newSchema.fields.length)(null).asInstanceOf[Array[Any]])

    val oldFieldMap = getCacheSchemaPosMap(oldSchema)
    for ((field, pos) <- newSchema.fields.zipWithIndex) {
      var oldValue: AnyRef = null
      if (oldFieldMap.contains(field.name)) {
        val (oldField, oldPos) = oldFieldMap(field.name)
        oldValue = oldRecord.get(oldPos, oldField.dataType)
      }
      if (oldValue != null) {
        field.dataType match {
          case structType: StructType =>
            val oldField = StructType(Seq(oldFieldMap(field.name)._1))
            rewriteRecord(oldValue.asInstanceOf[InternalRow], oldField, structType)
          case decimalType: DecimalType =>
            val oldField = oldFieldMap(field.name)._1.asInstanceOf[DecimalType]
            if (decimalType.scale != oldField.scale || decimalType.precision != oldField.precision) {
              newRow.update(pos, Decimal.fromDecimal(oldValue.asInstanceOf[Decimal].toBigDecimal.setScale(newSchema.asInstanceOf[DecimalType].scale))
              )
            } else {
              newRow.update(pos, oldValue)
            }
          case _ =>
            newRow.update(pos, oldValue)
        }
      } else {
        // TODO default value in newSchema
      }
    }

    newRow
  }

  /**
   * @see org.apache.hudi.avro.HoodieAvroUtils#rewriteRecordWithNewSchema(org.apache.avro.generic.IndexedRecord, org.apache.avro.Schema, java.util.Map)
   */
  def rewriteRecordWithNewSchema(oldRecord: InternalRow, oldSchema: StructType, newSchema: StructType, renameCols: java.util.Map[String, String]): InternalRow = {
    rewriteRecordWithNewSchema(oldRecord, oldSchema, newSchema, renameCols, new java.util.LinkedList[String]).asInstanceOf[InternalRow]
  }

  /**
   * @see org.apache.hudi.avro.HoodieAvroUtils#rewriteRecordWithNewSchema(java.lang.Object, org.apache.avro.Schema, org.apache.avro.Schema, java.util.Map, java.util.Deque)
   */
  private def rewriteRecordWithNewSchema(oldRecord: Any, oldSchema: DataType, newSchema: DataType, renameCols: java.util.Map[String, String], fieldNames: java.util.Deque[String]): Any = {
    if (oldRecord == null) {
      null
    } else {
      newSchema match {
        case targetSchema: StructType =>
          if (!oldRecord.isInstanceOf[InternalRow]) {
            throw new IllegalArgumentException("cannot rewrite record with different type")
          }
          val oldRow = oldRecord.asInstanceOf[InternalRow]
          val helper = mutable.Map[Integer, Any]()

          val oldSchemaPos = getCacheSchemaPosMap(oldSchema.asInstanceOf[StructType])
          targetSchema.fields.zipWithIndex.foreach { case (field, i) =>
            fieldNames.push(field.name)
            if (oldSchemaPos.contains(field.name)) {
              val (oldField, oldPos) = oldSchemaPos(field.name)
              helper(i) = rewriteRecordWithNewSchema(oldRow.get(oldPos, oldField.dataType), oldField.dataType, field.dataType, renameCols, fieldNames)
            } else {
              val fieldFullName = createFullName(fieldNames)
              val colNamePartsFromOldSchema = renameCols.getOrDefault(fieldFullName, "").split("\\.")
              val lastColNameFromOldSchema = colNamePartsFromOldSchema(colNamePartsFromOldSchema.length - 1)
              // deal with rename
              if (!oldSchemaPos.contains(field.name) && oldSchemaPos.contains(lastColNameFromOldSchema)) {
                // find rename
                val (oldField, oldPos) = oldSchemaPos(lastColNameFromOldSchema)
                helper(i) = rewriteRecordWithNewSchema(oldRow.get(oldPos, oldField.dataType), oldField.dataType, field.dataType, renameCols, fieldNames)
              }
            }
            fieldNames.pop()
          }
          val newRow = new GenericInternalRow(Array.fill(targetSchema.length)(null).asInstanceOf[Array[Any]])
          targetSchema.fields.zipWithIndex.foreach { case (_, i) =>
            if (helper.contains(i)) {
              newRow.update(i, helper(i))
            } else {
              // TODO add default val
              newRow.update(i, null)
            }
          }

          newRow
        case targetSchema: ArrayType =>
          if (!oldRecord.isInstanceOf[ArrayData]) {
            throw new IllegalArgumentException("cannot rewrite record with different type")
          }
          val oldElementType = oldSchema.asInstanceOf[ArrayType].elementType
          val oldArray = oldRecord.asInstanceOf[ArrayData]
          val newElementType = targetSchema.elementType
          val newArray = new GenericArrayData(Array.fill(oldArray.numElements())(null).asInstanceOf[Array[Any]])
          fieldNames.push("element")
          oldArray.toSeq[Any](oldElementType).zipWithIndex.foreach { case (value, i) => newArray.update(i, rewriteRecordWithNewSchema(value.asInstanceOf[AnyRef], oldElementType, newElementType, renameCols, fieldNames)) }
          fieldNames.pop()

          newArray
        case targetSchema: MapType =>
          if (!oldRecord.isInstanceOf[MapData]) {
            throw new IllegalArgumentException("cannot rewrite record with different type")
          }
          val oldValueType = oldSchema.asInstanceOf[MapType].valueType
          val oldKeyType = oldSchema.asInstanceOf[MapType].keyType
          val oldMap = oldRecord.asInstanceOf[MapData]
          val newValueType = targetSchema.valueType
          val newKeyArray = new GenericArrayData(Array.fill(oldMap.keyArray().numElements())(null).asInstanceOf[Array[Any]])
          val newValueArray = new GenericArrayData(Array.fill(oldMap.valueArray().numElements())(null).asInstanceOf[Array[Any]])
          val newMap = new ArrayBasedMapData(newKeyArray, newValueArray)
          fieldNames.push("value")
          oldMap.keyArray().toSeq[Any](oldKeyType).zipWithIndex.foreach { case (value, i) => newKeyArray.update(i, value) }
          oldMap.valueArray().toSeq[Any](oldValueType).zipWithIndex.foreach { case (value, i) => newValueArray.update(i, rewriteRecordWithNewSchema(value.asInstanceOf[AnyRef], oldValueType, newValueType, renameCols, fieldNames)) }
          fieldNames.pop()

          newMap
        case _ => rewritePrimaryType(oldRecord, oldSchema, newSchema)
      }
    }
  }

  /**
   * @see org.apache.hudi.avro.HoodieAvroUtils#rewriteRecordWithMetadata(org.apache.avro.generic.GenericRecord, org.apache.avro.Schema, java.lang.String)
   */
  def rewriteRecordWithMetadata(record: InternalRow, oldSchema: StructType, newSchema: StructType, fileName: String): InternalRow = {
    val newRecord = rewriteRecord(record, oldSchema, newSchema)
    newRecord.update(HoodieMetadataField.FILENAME_METADATA_FIELD.ordinal, CatalystTypeConverters.convertToCatalyst(fileName))

    newRecord
  }

  /**
   * @see org.apache.hudi.avro.HoodieAvroUtils#rewriteEvolutionRecordWithMetadata(org.apache.avro.generic.GenericRecord, org.apache.avro.Schema, java.lang.String)
   */
  def rewriteEvolutionRecordWithMetadata(record: InternalRow, oldSchema: StructType, newSchema: StructType, fileName: String): InternalRow = {
    val newRecord = rewriteRecordWithNewSchema(record, oldSchema, newSchema, new java.util.HashMap[String, String]())
    newRecord.update(HoodieMetadataField.FILENAME_METADATA_FIELD.ordinal, CatalystTypeConverters.convertToCatalyst(fileName))

    newRecord
  }

  def getCacheSchema(schema: Schema): StructType = {
    if (!schemaMap.contains(schema)) {
      schemaMap.synchronized {
        if (!schemaMap.contains(schema)) {
          val structType = AvroConversionUtils.convertAvroSchemaToStructType(schema)
          schemaMap.put(schema, structType)
        }
      }
    }
    schemaMap.get(schema)
  }

  private def getCacheProjection(from: StructType, to: StructType): Projection = {
    val schemaPair = (from, to)
    if (!projectionMap.contains(schemaPair)) {
      projectionMap.synchronized {
        if (!projectionMap.contains(schemaPair)) {
          // TODO: modify here
          // val projection = HoodieCatalystExpressionUtils.generateMutableProjection(from, to)
          // projectionMap.put(schemaPair, projection)
        }
      }
    }
    projectionMap.get(schemaPair)
  }

  def getCacheSchemaPosMap(schema: StructType): Map[String, (StructField, Int)] = {
    if (!SchemaPosMap.contains(schema)) {
      SchemaPosMap.synchronized {
        if (!SchemaPosMap.contains(schema)) {
          val fieldMap = schema.fields.zipWithIndex.map { case (field, i) => (field.name, (field, i)) }.toMap
          SchemaPosMap.put(schema, fieldMap)
        }
      }
    }
    SchemaPosMap.get(schema)
  }

  private def rewritePrimaryType(oldValue: Any, oldSchema: DataType, newSchema: DataType): Any = {
    if (oldSchema.equals(newSchema) || (oldSchema.isInstanceOf[DecimalType] && newSchema.isInstanceOf[DecimalType])) {
      oldSchema match {
        case NullType | BooleanType | IntegerType | LongType | FloatType | DoubleType | StringType | DateType | TimestampType | BinaryType =>
          oldValue
        case DecimalType() =>
          Decimal.fromDecimal(oldValue.asInstanceOf[Decimal].toBigDecimal.setScale(newSchema.asInstanceOf[DecimalType].scale))
        case _ =>
          throw new HoodieException("Unknown schema type: " + newSchema)
      }
    } else {
      rewritePrimaryTypeWithDiffSchemaType(oldValue, oldSchema, newSchema)
    }
  }

  private def rewritePrimaryTypeWithDiffSchemaType(oldValue: Any, oldSchema: DataType, newSchema: DataType): Any = {
    val value = newSchema match {
      case NullType | BooleanType =>
      case DateType if oldSchema.equals(StringType) =>
        fromJavaDate(java.sql.Date.valueOf(oldValue.toString))
      case LongType =>
        oldSchema match {
          case IntegerType => oldValue.asInstanceOf[Int].longValue()
          case _ =>
        }
      case FloatType =>
        oldSchema match {
          case IntegerType => oldValue.asInstanceOf[Int].floatValue()
          case LongType => oldValue.asInstanceOf[Long].floatValue()
          case _ =>
        }
      case DoubleType =>
        oldSchema match {
          case IntegerType => oldValue.asInstanceOf[Int].doubleValue()
          case LongType => oldValue.asInstanceOf[Long].doubleValue()
          case FloatType => java.lang.Double.valueOf(oldValue.asInstanceOf[Float] + "")
          case _ =>
        }
      case BinaryType =>
        oldSchema match {
          case StringType => oldValue.asInstanceOf[String].getBytes(StandardCharsets.UTF_8)
          case _ =>
        }
      case StringType =>
        oldSchema match {
          case BinaryType => new String(oldValue.asInstanceOf[Array[Byte]])
          case DateType => toJavaDate(oldValue.asInstanceOf[Integer]).toString
          case IntegerType | LongType | FloatType | DoubleType | DecimalType() => oldValue.toString
          case _ =>
        }
      case DecimalType() =>
        oldSchema match {
          case IntegerType | LongType | FloatType | DoubleType | StringType =>
            val scale = newSchema.asInstanceOf[DecimalType].scale

            Decimal.fromDecimal(BigDecimal(oldValue.toString).setScale(scale))
          case _ =>
        }
      case _ =>
    }
    if (value == None) {
      throw new HoodieException(String.format("cannot support rewrite value for schema type: %s since the old schema type is: %s", newSchema, oldSchema))
    } else {
      value
    }
  }
}
