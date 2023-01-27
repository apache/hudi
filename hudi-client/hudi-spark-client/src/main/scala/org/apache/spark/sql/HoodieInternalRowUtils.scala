/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import java.nio.charset.StandardCharsets
import org.apache.avro.Schema
import org.apache.hbase.thirdparty.com.google.common.base.Supplier
import org.apache.hudi.AvroConversionUtils.convertAvroSchemaToStructType
import org.apache.hudi.avro.HoodieAvroUtils.{createFullName, toJavaDate}
import org.apache.hudi.exception.HoodieException
import org.apache.spark.sql.HoodieCatalystExpressionUtils.generateUnsafeProjection
import org.apache.spark.sql.HoodieUnsafeRowUtils.{NestedFieldPath, composeNestedFieldPath}
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, SpecificInternalRow, UnsafeProjection}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, GenericArrayData, MapData}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.mutable.ParHashMap

object HoodieInternalRowUtils {

  // Projection are all thread local. Projection is not thread-safe
  private val unsafeWriterAndProjectionThreadLocal: ThreadLocal[mutable.HashMap[(StructType, StructType), (InternalRow => InternalRow, UnsafeProjection)]] =
    ThreadLocal.withInitial(new Supplier[mutable.HashMap[(StructType, StructType), UnsafeProjection]] {
      override def get(): mutable.HashMap[(StructType, StructType), (InternalRow => InternalRow, UnsafeProjection)] =
        new mutable.HashMap[(StructType, StructType), (InternalRow => InternalRow, UnsafeProjection)]
    })

  private val schemaMap = new ParHashMap[Schema, StructType]
  private val orderPosListMap = new ParHashMap[(StructType, String), NestedFieldPath]

  def genUnsafeRowWriter(prevSchema: StructType, newSchema: StructType): InternalRow => InternalRow = {
    val writer = newWriter(prevSchema, newSchema)
    val phonyUpdater = new CatalystDataUpdater {
      var value: InternalRow = _
      override def set(ordinal: Int, value: Any): Unit =
        this.value = value.asInstanceOf[InternalRow]
    }

    oldRow => {
      writer(phonyUpdater, 0, oldRow)
      phonyUpdater.value
    }
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

          val oldStrucType = oldSchema.asInstanceOf[StructType]
          targetSchema.fields.zipWithIndex.foreach { case (field, i) =>
            fieldNames.push(field.name)
            if (existField(oldStrucType, field.name)) {
              val oldField = oldStrucType(field.name)
              val oldPos = oldStrucType.fieldIndex(field.name)
              helper(i) = rewriteRecordWithNewSchema(oldRow.get(oldPos, oldField.dataType), oldField.dataType, field.dataType, renameCols, fieldNames)
            } else {
              val fieldFullName = createFullName(fieldNames)
              val colNamePartsFromOldSchema = renameCols.getOrDefault(fieldFullName, "").split("\\.")
              val lastColNameFromOldSchema = colNamePartsFromOldSchema(colNamePartsFromOldSchema.length - 1)
              // deal with rename
              if (!existField(oldStrucType, field.name) && existField(oldStrucType, lastColNameFromOldSchema)) {
                // find rename
                val oldField = oldStrucType(lastColNameFromOldSchema)
                val oldPos = oldStrucType.fieldIndex(lastColNameFromOldSchema)
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
          oldMap.keyArray().toSeq[Any](oldKeyType).zipWithIndex.foreach { case (value, i) => newKeyArray.update(i, rewritePrimaryType(value, oldKeyType, oldKeyType)) }
          oldMap.valueArray().toSeq[Any](oldValueType).zipWithIndex.foreach { case (value, i) => newValueArray.update(i, rewriteRecordWithNewSchema(value.asInstanceOf[AnyRef], oldValueType, newValueType, renameCols, fieldNames)) }
          fieldNames.pop()

          newMap
        case _ => rewritePrimaryType(oldRecord, oldSchema, newSchema)
      }
    }
  }

  def getCachedUnsafeProjection(from: StructType, to: StructType): UnsafeProjection = {
    val (_, projection) = getCachedUnsafeRowWriterAndUnsafeProjection(from, to)
    projection
  }

  def getCachedUnsafeRowWriterAndUnsafeProjection(from: StructType, to: StructType): (InternalRow => InternalRow, UnsafeProjection) = {
    unsafeWriterAndProjectionThreadLocal.get()
      .getOrElseUpdate((from, to), (genUnsafeRowWriter(from, to), generateUnsafeProjection(from, to)))
  }

  def getCachedPosList(structType: StructType, field: String): Option[NestedFieldPath] =
    Option(orderPosListMap.getOrElse((structType, field), composeNestedFieldPath(structType, field)))

  def getCachedSchema(schema: Schema): StructType =
    schemaMap.getOrElse(schema, convertAvroSchemaToStructType(schema))

  def existField(structType: StructType, fieldRef: String): Boolean =
    getCachedPosList(structType, fieldRef) != null

  private def rewritePrimaryType(oldValue: Any, oldSchema: DataType, newSchema: DataType) = {
    if (oldSchema.equals(newSchema) || (oldSchema.isInstanceOf[DecimalType] && newSchema.isInstanceOf[DecimalType])) {
      oldSchema match {
        case NullType | BooleanType | IntegerType | LongType | FloatType | DoubleType | DateType | TimestampType | BinaryType =>
          oldValue
        // Copy UTF8String before putting into GenericInternalRow
        case StringType => UTF8String.fromString(oldValue.toString)
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
        CatalystTypeConverters.convertToCatalyst(java.sql.Date.valueOf(oldValue.toString))
      case LongType =>
        oldSchema match {
          case IntegerType => CatalystTypeConverters.convertToCatalyst(oldValue.asInstanceOf[Int].longValue())
          case _ =>
        }
      case FloatType =>
        oldSchema match {
          case IntegerType => CatalystTypeConverters.convertToCatalyst(oldValue.asInstanceOf[Int].floatValue())
          case LongType => CatalystTypeConverters.convertToCatalyst(oldValue.asInstanceOf[Long].floatValue())
          case _ =>
        }
      case DoubleType =>
        oldSchema match {
          case IntegerType => CatalystTypeConverters.convertToCatalyst(oldValue.asInstanceOf[Int].doubleValue())
          case LongType => CatalystTypeConverters.convertToCatalyst(oldValue.asInstanceOf[Long].doubleValue())
          case FloatType => CatalystTypeConverters.convertToCatalyst(java.lang.Double.valueOf(oldValue.asInstanceOf[Float] + ""))
          case _ =>
        }
      case BinaryType =>
        oldSchema match {
          case StringType => CatalystTypeConverters.convertToCatalyst(oldValue.asInstanceOf[String].getBytes(StandardCharsets.UTF_8))
          case _ =>
        }
      case StringType =>
        oldSchema match {
          case BinaryType => CatalystTypeConverters.convertToCatalyst(new String(oldValue.asInstanceOf[Array[Byte]]))
          case DateType => CatalystTypeConverters.convertToCatalyst(toJavaDate(oldValue.asInstanceOf[Integer]).toString)
          case IntegerType | LongType | FloatType | DoubleType | DecimalType() => CatalystTypeConverters.convertToCatalyst(oldValue.toString)
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
      CatalystTypeConverters.convertToCatalyst(value)
    }
  }

  def removeFields(schema: StructType, fieldsToRemove: java.util.List[String]): StructType = {
    StructType(schema.fields.filter(field => !fieldsToRemove.contains(field.name)))
  }

  private def genRowWriter(prevStructType: StructType, newStructType: StructType): (CatalystDataUpdater, Any) => Unit = {
    // TODO need to canonicalize schemas (casing)
    val fieldWriters = ArrayBuffer.empty[(CatalystDataUpdater, Int, Any) => Unit]
    val positionMap = ArrayBuffer.empty[Int]

    for (newField <- newStructType.fields) {
      val prevFieldPos = prevStructType.getFieldIndex(newField.name).getOrElse(-1)
      val fieldWriter: (CatalystDataUpdater, Int, Any) => Unit =
        if (prevFieldPos >= 0) {
          val prevField = prevStructType.fields(prevFieldPos)
          newWriter(prevField.dataType, newField.dataType)
        } else {
          // TODO handle defaults
          (setter, ordinal, _) => setter.setNullAt(ordinal)
        }

      positionMap += prevFieldPos
      fieldWriters += fieldWriter
    }

    (fieldUpdater, row) => {
      var pos = 0
      while (pos < fieldWriters.length) {
        val prevPos = positionMap(pos)
        val prevValue = if (prevPos >= 0) {
          row.asInstanceOf[InternalRow].get(prevPos, prevStructType.fields(prevPos).dataType)
        } else {
          null
        }

        fieldWriters(pos)(fieldUpdater, pos, prevValue)
        pos += 1
      }
    }
  }

  private def newWriter(prevDataType: DataType, newDataType: DataType): (CatalystDataUpdater, Int, Any) => Unit = {
    // TODO support map/array
    (newDataType, prevDataType) match {
      case (newType, prevType) if newType == prevType =>
        (fieldUpdater, ordinal, value) => fieldUpdater.set(ordinal, value)

      case (newStructType: StructType, prevStructType: StructType) =>
        val writer = genRowWriter(prevStructType, newStructType)
        val newRow = new SpecificInternalRow(newStructType.fields.map(_.dataType))
        val rowUpdater = new RowUpdater(newRow)
        (fieldUpdater, ordinal, value) => {
          // TODO elaborate
          writer(rowUpdater, value)
          fieldUpdater.set(ordinal, newRow)
        }

      case (newDecimal: DecimalType, _: DecimalType) =>
        // TODO validate decimal type is expanding
        (fieldUpdater, ordinal, value) =>
          fieldUpdater.setDecimal(ordinal, Decimal.fromDecimal(value.asInstanceOf[Decimal].toBigDecimal.setScale(newDecimal.scale)))

      case (_: ShortType, _) =>
        prevDataType match {
          case _: ByteType => (fieldUpdater, ordinal, value) => fieldUpdater.setShort(ordinal, value.asInstanceOf[Byte].toShort)
          case _ =>
            throw new IllegalArgumentException(s"$prevDataType and $newDataType are incompatible")
        }

      case (_: IntegerType, _) =>
        prevDataType match {
          case _: ShortType => (fieldUpdater, ordinal, value) => fieldUpdater.setInt(ordinal, value.asInstanceOf[Short].toInt)
          case _: ByteType => (fieldUpdater, ordinal, value) => fieldUpdater.setInt(ordinal, value.asInstanceOf[Byte].toInt)
          case _ =>
            throw new IllegalArgumentException(s"$prevDataType and $newDataType are incompatible")
        }

      case (_: LongType, _) =>
        prevDataType match {
          case _: IntegerType => (fieldUpdater, ordinal, value) => fieldUpdater.setLong(ordinal, value.asInstanceOf[Int].toLong)
          case _: ShortType => (fieldUpdater, ordinal, value) => fieldUpdater.setLong(ordinal, value.asInstanceOf[Short].toLong)
          case _: ByteType => (fieldUpdater, ordinal, value) => fieldUpdater.setLong(ordinal, value.asInstanceOf[Byte].toLong)
          case _ =>
            throw new IllegalArgumentException(s"$prevDataType and $newDataType are incompatible")
        }

      case (_: FloatType, _) =>
        prevDataType match {
          case _: LongType => (fieldUpdater, ordinal, value) => fieldUpdater.setFloat(ordinal, value.asInstanceOf[Long].toFloat)
          case _: IntegerType => (fieldUpdater, ordinal, value) => fieldUpdater.setFloat(ordinal, value.asInstanceOf[Int].toFloat)
          case _: ShortType => (fieldUpdater, ordinal, value) => fieldUpdater.setFloat(ordinal, value.asInstanceOf[Short].toFloat)
          case _: ByteType => (fieldUpdater, ordinal, value) => fieldUpdater.setFloat(ordinal, value.asInstanceOf[Byte].toFloat)
          case _ =>
            throw new IllegalArgumentException(s"$prevDataType and $newDataType are incompatible")
        }

      case (_: DoubleType, _) =>
        prevDataType match {
          case _: FloatType => (fieldUpdater, ordinal, value) => fieldUpdater.setDouble(ordinal, value.asInstanceOf[Float].toDouble)
          case _: LongType => (fieldUpdater, ordinal, value) => fieldUpdater.setDouble(ordinal, value.asInstanceOf[Long].toDouble)
          case _: IntegerType => (fieldUpdater, ordinal, value) => fieldUpdater.setDouble(ordinal, value.asInstanceOf[Int].toDouble)
          case _: ShortType => (fieldUpdater, ordinal, value) => fieldUpdater.setDouble(ordinal, value.asInstanceOf[Short].toDouble)
          case _: ByteType => (fieldUpdater, ordinal, value) => fieldUpdater.setDouble(ordinal, value.asInstanceOf[Byte].toDouble)
          case _ =>
            throw new IllegalArgumentException(s"$prevDataType and $newDataType are incompatible")
        }

      case (_: BinaryType, _: StringType) =>
        (fieldUpdater, ordinal, value) => fieldUpdater.set(ordinal, value.asInstanceOf[UTF8String].getBytes)

      case (_, _) =>
        throw new IllegalArgumentException(s"$prevDataType and $newDataType are incompatible")
    }
  }

  sealed trait CatalystDataUpdater {
    def set(ordinal: Int, value: Any): Unit
    def setNullAt(ordinal: Int): Unit = set(ordinal, null)
    def setBoolean(ordinal: Int, value: Boolean): Unit = set(ordinal, value)
    def setByte(ordinal: Int, value: Byte): Unit = set(ordinal, value)
    def setShort(ordinal: Int, value: Short): Unit = set(ordinal, value)
    def setInt(ordinal: Int, value: Int): Unit = set(ordinal, value)
    def setLong(ordinal: Int, value: Long): Unit = set(ordinal, value)
    def setDouble(ordinal: Int, value: Double): Unit = set(ordinal, value)
    def setFloat(ordinal: Int, value: Float): Unit = set(ordinal, value)
    def setDecimal(ordinal: Int, value: Decimal): Unit = set(ordinal, value)
  }

  final class RowUpdater(row: InternalRow) extends CatalystDataUpdater {
    override def set(ordinal: Int, value: Any): Unit = row.update(ordinal, value)
    override def setNullAt(ordinal: Int): Unit = row.setNullAt(ordinal)
    override def setBoolean(ordinal: Int, value: Boolean): Unit = row.setBoolean(ordinal, value)
    override def setByte(ordinal: Int, value: Byte): Unit = row.setByte(ordinal, value)
    override def setShort(ordinal: Int, value: Short): Unit = row.setShort(ordinal, value)
    override def setInt(ordinal: Int, value: Int): Unit = row.setInt(ordinal, value)
    override def setLong(ordinal: Int, value: Long): Unit = row.setLong(ordinal, value)
    override def setDouble(ordinal: Int, value: Double): Unit = row.setDouble(ordinal, value)
    override def setFloat(ordinal: Int, value: Float): Unit = row.setFloat(ordinal, value)
    override def setDecimal(ordinal: Int, value: Decimal): Unit =
      row.setDecimal(ordinal, value, value.precision)
  }

  final class ArrayDataUpdater(array: ArrayData) extends CatalystDataUpdater {
    override def set(ordinal: Int, value: Any): Unit = array.update(ordinal, value)
    override def setNullAt(ordinal: Int): Unit = array.setNullAt(ordinal)
    override def setBoolean(ordinal: Int, value: Boolean): Unit = array.setBoolean(ordinal, value)
    override def setByte(ordinal: Int, value: Byte): Unit = array.setByte(ordinal, value)
    override def setShort(ordinal: Int, value: Short): Unit = array.setShort(ordinal, value)
    override def setInt(ordinal: Int, value: Int): Unit = array.setInt(ordinal, value)
    override def setLong(ordinal: Int, value: Long): Unit = array.setLong(ordinal, value)
    override def setDouble(ordinal: Int, value: Double): Unit = array.setDouble(ordinal, value)
    override def setFloat(ordinal: Int, value: Float): Unit = array.setFloat(ordinal, value)
    override def setDecimal(ordinal: Int, value: Decimal): Unit = array.update(ordinal, value)
  }

}
