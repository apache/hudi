package org.apache.hudi

import org.apache.hudi.common.model.FileSlice
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

class InternalRowBroadcast(internalRow: InternalRow, broadcast: Broadcast[Map[String, FileSlice]]) extends InternalRow {

  def getSlice(fileId: String): Option[FileSlice] = {
      broadcast.value.get(fileId)
  }

  def getInternalRow: InternalRow = internalRow

  override def numFields: Int = internalRow.numFields

  override def setNullAt(i: Int): Unit = internalRow.setNullAt(i)

  override def update(i: Int, value: Any): Unit = internalRow.update(i, value)

  override def copy(): InternalRow = new InternalRowBroadcast(internalRow.copy(), broadcast)

  override def isNullAt(ordinal: Int): Boolean = internalRow.isNullAt(ordinal)

  override def getBoolean(ordinal: Int): Boolean = internalRow.getBoolean(ordinal)

  override def getByte(ordinal: Int): Byte = internalRow.getByte(ordinal)

  override def getShort(ordinal: Int): Short = internalRow.getShort(ordinal)

  override def getInt(ordinal: Int): Int = internalRow.getInt(ordinal)

  override def getLong(ordinal: Int): Long = internalRow.getLong(ordinal)

  override def getFloat(ordinal: Int): Float = internalRow.getFloat(ordinal)

  override def getDouble(ordinal: Int): Double = internalRow.getDouble(ordinal)

  override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal = internalRow.getDecimal(ordinal, precision, scale)

  override def getUTF8String(ordinal: Int): UTF8String = internalRow.getUTF8String(ordinal)

  override def getBinary(ordinal: Int): Array[Byte] = internalRow.getBinary(ordinal)

  override def getInterval(ordinal: Int): CalendarInterval = internalRow.getInterval(ordinal)

  override def getStruct(ordinal: Int, numFields: Int): InternalRow = internalRow.getStruct(ordinal, numFields)

  override def getArray(ordinal: Int): ArrayData = internalRow.getArray(ordinal)

  override def getMap(ordinal: Int): MapData = internalRow.getMap(ordinal)

  override def get(ordinal: Int, dataType: DataType): AnyRef = internalRow.get(ordinal, dataType)
}
