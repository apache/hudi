package org.apache.spark.sql.execution.vectorized;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.CalendarIntervalType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampNTZType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class UpdateableColumnVector extends ColumnVector {

  private final Integer[] translatedPositions;
  private final ColumnVector vector;

  public UpdateableColumnVector(ColumnVector vector, Integer[] translatedPositions) {
    super(vector.dataType());
    this.vector = vector;
    this.translatedPositions = translatedPositions;
  }

  @Override
  public void close() {
    vector.close();
  }

  @Override
  public boolean hasNull() {
    return vector.hasNull();
  }

  @Override
  public int numNulls() {
    return vector.numNulls();
  }


  @Override
  public boolean isNullAt(int rowId) {
    return vector.isNullAt(translatedPositions[rowId]);
  }

  @Override
  public boolean getBoolean(int rowId) {
    return vector.getBoolean(translatedPositions[rowId]);
  }

  @Override
  public byte getByte(int rowId) {
    return vector.getByte(translatedPositions[rowId]);
  }

  @Override
  public short getShort(int rowId) {
    return vector.getShort(translatedPositions[rowId]);
  }

  @Override
  public int getInt(int rowId) {
    return vector.getInt(translatedPositions[rowId]);
  }

  @Override
  public long getLong(int rowId) {
    return vector.getLong(translatedPositions[rowId]);
  }

  @Override
  public float getFloat(int rowId) {
    return vector.getFloat(translatedPositions[rowId]);
  }

  @Override
  public double getDouble(int rowId) {
    return vector.getDouble(translatedPositions[rowId]);
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    return vector.getArray(translatedPositions[rowId]);
  }

  @Override
  public ColumnarMap getMap(int ordinal) {
    return vector.getMap(translatedPositions[ordinal]);
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    return vector.getDecimal(translatedPositions[rowId], precision, scale);
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    return vector.getUTF8String(translatedPositions[rowId]);
  }

  @Override
  public byte[] getBinary(int rowId) {
    return vector.getBinary(translatedPositions[rowId]);
  }

  @Override
  public ColumnVector getChild(int ordinal) {
    return vector.getChild(translatedPositions[ordinal]);
  }

  public static void nullAll(ColumnarBatch batch, int rowId) {
    for (int i = 0; i < batch.numCols(); i++) {
      ((WritableColumnVector) batch.column(i)).putNull(rowId);
    }
  }
  public static void populateAll(ColumnarBatch batch, InternalRow row, int rowId) {
    for (int i = 0; i < batch.numCols(); i++) {
      populate((WritableColumnVector) batch.column(i), row, i, rowId);
    }
  }

  public static void populate(WritableColumnVector col, InternalRow row, int fieldIdx, int rowId) {
    DataType t = col.dataType();

    if (row.isNullAt(fieldIdx)) {
      col.putNull(rowId);
    } else {
      if (t == DataTypes.BooleanType) {
        col.putBoolean(rowId, row.getBoolean(fieldIdx));
      } else if (t == DataTypes.BinaryType) {
        col.putByteArray(rowId, row.getBinary(fieldIdx));
      } else if (t == DataTypes.ByteType) {
        col.putByte(rowId, row.getByte(fieldIdx));
      } else if (t == DataTypes.ShortType) {
        col.putShort(rowId, row.getShort(fieldIdx));
      } else if (t == DataTypes.IntegerType) {
        col.putInt(rowId, row.getInt(fieldIdx));
      } else if (t == DataTypes.LongType) {
        col.putLong(rowId, row.getLong(fieldIdx));
      } else if (t == DataTypes.FloatType) {
        col.putFloat(rowId, row.getFloat(fieldIdx));
      } else if (t == DataTypes.DoubleType) {
        col.putDouble(rowId, row.getDouble(fieldIdx));
      } else if (t == DataTypes.StringType) {
        col.putByteArray(rowId, row.getUTF8String(fieldIdx).getBytes());
      } else if (t instanceof DecimalType) {
        DecimalType dt = (DecimalType) t;
        Decimal d = row.getDecimal(fieldIdx, dt.precision(), dt.scale());
        if (dt.precision() <= Decimal.MAX_INT_DIGITS()) {
          col.putInt(rowId, (int) d.toUnscaledLong());
        } else if (dt.precision() <= Decimal.MAX_LONG_DIGITS()) {
          col.putLong(rowId, d.toUnscaledLong());
        } else {
          final BigInteger integer = d.toJavaBigDecimal().unscaledValue();
          byte[] bytes = integer.toByteArray();
          col.putByteArray(rowId, bytes, 0, bytes.length);
        }
      } else if (t instanceof CalendarIntervalType) {
        CalendarInterval c = (CalendarInterval) row.get(fieldIdx, t);
        col.getChild(0).putInt(rowId, c.months);
        col.getChild(1).putInt(rowId, c.days);
        col.getChild(2).putLong(rowId, c.microseconds);
      } else if (t instanceof DateType) {
        col.putInt(rowId, row.getInt(fieldIdx));
      } else if (t instanceof TimestampType || t instanceof TimestampNTZType) {
        col.putLong(rowId, row.getLong(fieldIdx));
      } else {
        throw new RuntimeException(String.format("DataType %s is not supported" +
            " in column vectorized reader.", t.sql()));
      }
    }
  }

  private static void appendValue(WritableColumnVector dst, DataType t, Object o) {
    if (o == null) {
      if (t instanceof CalendarIntervalType) {
        dst.appendStruct(true);
      } else {
        dst.appendNull();
      }
    } else {
      if (t == DataTypes.BooleanType) {
        dst.appendBoolean((Boolean) o);
      } else if (t == DataTypes.ByteType) {
        dst.appendByte((Byte) o);
      } else if (t == DataTypes.ShortType) {
        dst.appendShort((Short) o);
      } else if (t == DataTypes.IntegerType) {
        dst.appendInt((Integer) o);
      } else if (t == DataTypes.LongType) {
        dst.appendLong((Long) o);
      } else if (t == DataTypes.FloatType) {
        dst.appendFloat((Float) o);
      } else if (t == DataTypes.DoubleType) {
        dst.appendDouble((Double) o);
      } else if (t == DataTypes.StringType) {
        byte[] b =((String)o).getBytes(StandardCharsets.UTF_8);
        dst.appendByteArray(b, 0, b.length);
      } else if (t instanceof DecimalType) {
        DecimalType dt = (DecimalType) t;
        Decimal d = Decimal.apply((BigDecimal) o, dt.precision(), dt.scale());
        if (dt.precision() <= Decimal.MAX_INT_DIGITS()) {
          dst.appendInt((int) d.toUnscaledLong());
        } else if (dt.precision() <= Decimal.MAX_LONG_DIGITS()) {
          dst.appendLong(d.toUnscaledLong());
        } else {
          final BigInteger integer = d.toJavaBigDecimal().unscaledValue();
          byte[] bytes = integer.toByteArray();
          dst.appendByteArray(bytes, 0, bytes.length);
        }
      } else if (t instanceof CalendarIntervalType) {
        CalendarInterval c = (CalendarInterval)o;
        dst.appendStruct(false);
        dst.getChild(0).appendInt(c.months);
        dst.getChild(1).appendInt(c.days);
        dst.getChild(2).appendLong(c.microseconds);
      } else if (t instanceof DateType) {
        dst.appendInt(DateTimeUtils.fromJavaDate((Date)o));
      } else {
        throw new UnsupportedOperationException("Type " + t);
      }
    }
  }

  private static void appendValue(WritableColumnVector dst, DataType t, InternalRow src, int fieldIdx) {
    if (t instanceof ArrayType) {
      ArrayType at = (ArrayType)t;
      if (src.isNullAt(fieldIdx)) {
        dst.appendNull();
      } else {
        List<Object> values = Arrays.asList(src.getArray(fieldIdx).array());
        dst.appendArray(values.size());
        for (Object o : values) {
          appendValue(dst.arrayData(), at.elementType(), o);
        }
      }
    } else if (t instanceof StructType) {
      StructType st = (StructType)t;
      if (src.isNullAt(fieldIdx)) {
        dst.appendStruct(true);
      } else {
        dst.appendStruct(false);
        InternalRow c = src.getStruct(fieldIdx, 0);
        for (int i = 0; i < st.fields().length; i++) {
          appendValue(dst.getChild(i), st.fields()[i].dataType(), c, i);
        }
      }
    } else {
      appendValue(dst, t, src.get(fieldIdx, t));
    }
  }

  /**
   * Converts an iterator of rows into a single ColumnBatch.
   */
  public static ColumnarBatch toBatch(
      StructType schema, Boolean enableOffHeapColumnVector, Iterator<InternalRow> row) {
    int capacity = 4 * 1024;
    WritableColumnVector[] columnVectors;
    if (enableOffHeapColumnVector) {
      columnVectors = OffHeapColumnVector.allocateColumns(capacity, schema);
    } else {
      columnVectors = OnHeapColumnVector.allocateColumns(capacity, schema);
    }

    int n = 0;
    while (row.hasNext()) {
      InternalRow r = row.next();
      for (int i = 0; i < schema.fields().length; i++) {
        appendValue(columnVectors[i], schema.fields()[i].dataType(), r, i);
      }
      n++;
    }
    ColumnarBatch batch = new ColumnarBatch(columnVectors);
    batch.setNumRows(n);
    return batch;
  }
}
