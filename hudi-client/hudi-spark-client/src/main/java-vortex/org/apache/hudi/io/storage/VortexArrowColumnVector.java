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

// spotless:disabled
package org.apache.hudi.io.storage;

import org.apache.hudi.io.vortex.VortexArrowProperties;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseLargeVariableWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BaseVariableWidthViewVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.IntervalMonthDayNanoVector;
import org.apache.arrow.vector.IntervalYearVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.ViewVarBinaryVector;
import org.apache.arrow.vector.ViewVarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.ListViewVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;

import java.math.BigDecimal;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Spark ColumnVector implementation that wraps Apache Arrow vectors from Vortex data.
 * <p>
 * This class provides a bridge between Vortex's Arrow-based data representation and Spark's
 * ColumnVector interface. It supports the same Arrow vector types as Spark 4.1's own
 * {@code ArrowColumnVector} — booleans, signed integers, single/double floats, 128-bit decimals,
 * strings and binary (regular and large), day dates, timestamps of every unit with and without
 * timezone (normalized to microseconds), microsecond durations, year-month and month-day-nano
 * intervals, nulls, lists, maps, and structs — plus the Arrow view types (Utf8View, BinaryView,
 * ListView) that Vortex produces natively.
 * <p>
 * Arrow types outside that set (unsigned integers, half floats, 256-bit decimals,
 * non-microsecond durations, Time, fixed-size binary and lists, large lists, unions, run-end and
 * dictionary encodings) are rejected with a descriptive {@link UnsupportedOperationException}.
 *
 * @see ColumnVector
 * @see ValueVector
 * <p>
 * Ported from {@code dev.vortex.spark.read.VortexArrowColumnVector} in the Vortex Spark connector
 * 0.93.0 (Apache License 2.0, Copyright the Vortex contributors). The connector relocates Arrow,
 * so Hudi cannot use it with the plain Arrow vectors that vortex-jni returns.
 */
public class VortexArrowColumnVector extends ColumnVector {

  static {
    // Set the Arrow properties before this class uses Arrow buffers and vectors.
    VortexArrowProperties.ensureConfigured();
  }
  VortexArrowColumnVector.ArrowVectorAccessor accessor;
  VortexArrowColumnVector[] childColumns;

  /**
   * Returns the underlying Apache Arrow ValueVector wrapped by this column vector.
   *
   * @return the Arrow ValueVector containing the actual data
   */
  public ValueVector getValueVector() {
    return accessor.vector;
  }

  /**
   * Returns whether this column contains any null values.
   *
   * @return true if the column contains at least one null value, false otherwise
   */
  @Override
  public boolean hasNull() {
    return accessor.getNullCount() > 0;
  }

  /**
   * Returns the total number of null values in this column.
   *
   * @return the count of null values
   */
  @Override
  public int numNulls() {
    return accessor.getNullCount();
  }

  /**
   * No-op: the underlying Arrow {@link ValueVector}s are owned by the
   * {@link org.apache.arrow.vector.ipc.ArrowReader} that produced
   * this batch and are released when that reader is closed.
   */
  @Override
  public void close() {}

  /**
   * Returns whether the value at the specified row is null.
   *
   * @param rowId the row index to check
   * @return true if the value at rowId is null, false otherwise
   */
  @Override
  public boolean isNullAt(int rowId) {
    return accessor.isNullAt(rowId);
  }

  /**
   * Returns the boolean value at the specified row.
   *
   * @param rowId the row index
   * @return the boolean value at rowId
   * @throws UnsupportedOperationException if this column is not of boolean type
   */
  @Override
  public boolean getBoolean(int rowId) {
    return accessor.getBoolean(rowId);
  }

  /**
   * Returns the byte value at the specified row.
   *
   * @param rowId the row index
   * @return the byte value at rowId
   * @throws UnsupportedOperationException if this column is not of byte type
   */
  @Override
  public byte getByte(int rowId) {
    return accessor.getByte(rowId);
  }

  /**
   * Returns the short value at the specified row.
   *
   * @param rowId the row index
   * @return the short value at rowId
   * @throws UnsupportedOperationException if this column is not of short type
   */
  @Override
  public short getShort(int rowId) {
    return accessor.getShort(rowId);
  }

  /**
   * Returns the int value at the specified row.
   *
   * @param rowId the row index
   * @return the int value at rowId
   * @throws UnsupportedOperationException if this column is not of int type
   */
  @Override
  public int getInt(int rowId) {
    return accessor.getInt(rowId);
  }

  /**
   * Returns the long value at the specified row.
   *
   * @param rowId the row index
   * @return the long value at rowId
   * @throws UnsupportedOperationException if this column is not of long type
   */
  @Override
  public long getLong(int rowId) {
    return accessor.getLong(rowId);
  }

  /**
   * Returns the float value at the specified row.
   *
   * @param rowId the row index
   * @return the float value at rowId
   * @throws UnsupportedOperationException if this column is not of float type
   */
  @Override
  public float getFloat(int rowId) {
    return accessor.getFloat(rowId);
  }

  /**
   * Returns the double value at the specified row.
   *
   * @param rowId the row index
   * @return the double value at rowId
   * @throws UnsupportedOperationException if this column is not of double type
   */
  @Override
  public double getDouble(int rowId) {
    return accessor.getDouble(rowId);
  }

  /**
   * Returns the decimal value at the specified row with the given precision and scale.
   *
   * @param rowId the row index
   * @param precision the precision of the decimal
   * @param scale the scale of the decimal
   * @return the Decimal value at rowId, or null if the value is null
   * @throws UnsupportedOperationException if this column is not of decimal type
   */
  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    if (isNullAt(rowId)) return null;
    return accessor.getDecimal(rowId, precision, scale);
  }

  /**
   * Returns the UTF8String value at the specified row.
   *
   * @param rowId the row index
   * @return the UTF8String value at rowId, or null if the value is null
   * @throws UnsupportedOperationException if this column is not of string type
   */
  @Override
  public UTF8String getUTF8String(int rowId) {
    if (isNullAt(rowId)) return null;
    return accessor.getUTF8String(rowId);
  }

  /**
   * Returns the binary data (byte array) at the specified row.
   *
   * @param rowId the row index
   * @return the byte array at rowId, or null if the value is null
   * @throws UnsupportedOperationException if this column is not of binary type
   */
  @Override
  public byte[] getBinary(int rowId) {
    if (isNullAt(rowId)) return null;
    return accessor.getBinary(rowId);
  }

  /**
   * Returns the array value at the specified row.
   *
   * @param rowId the row index
   * @return the ColumnarArray at rowId, or null if the value is null
   * @throws UnsupportedOperationException if this column is not of array type
   */
  @Override
  public ColumnarArray getArray(int rowId) {
    if (isNullAt(rowId)) return null;
    return accessor.getArray(rowId);
  }

  /**
   * Returns the map value at the specified row.
   *
   * @param rowId the row index
   * @return the ColumnarMap at rowId, or null if the value is null
   * @throws UnsupportedOperationException if this column is not of map type
   */
  @Override
  public ColumnarMap getMap(int rowId) {
    if (isNullAt(rowId)) return null;
    return accessor.getMap(rowId);
  }

  /**
   * Returns the child column at the specified ordinal.
   * <p>
   * This is used for complex types like structs where each field is represented
   * as a child column. For month-day-nano interval columns, the children follow the
   * months/days/microseconds protocol documented on {@link ColumnVector#getInterval(int)}.
   *
   * @param ordinal the index of the child column
   * @return the child VortexArrowColumnVector at the specified ordinal
   * @throws ArrayIndexOutOfBoundsException if ordinal is out of bounds
   */
  @Override
  public VortexArrowColumnVector getChild(int ordinal) {
    return childColumns[ordinal];
  }

  /**
   * Creates a new VortexArrowColumnVector with the specified Spark DataType.
   * <p>
   * This constructor is used internally for creating column vectors before
   * the underlying Arrow vector is available.
   *
   * @param type the Spark DataType for this column
   */
  VortexArrowColumnVector(DataType type) {
    super(type);
  }

  /**
   * Creates a new VortexArrowColumnVector wrapping the specified Arrow ValueVector.
   * <p>
   * This constructor automatically determines the appropriate Spark DataType from
   * the Arrow field and initializes the type-specific accessor.
   *
   * @param vector the Arrow ValueVector to wrap
   * @throws UnsupportedOperationException if the vector type is not supported
   */
  public VortexArrowColumnVector(ValueVector vector) {
    this(VortexArrowUtils.fromArrowField(vector.getField()));
    initAccessor(vector);
  }

  /**
   * A column of row indices. The Vortex row index expression gives unsigned 64-bit values, which
   * Spark reads as {@code long}. A row index is always less than 2^63, thus the value stays exact.
   */
  static VortexArrowColumnVector rowIndices(UInt8Vector vector) {
    return withAccessor(DataTypes.LongType, new RowIndexAccessor(vector));
  }

  private static VortexArrowColumnVector withAccessor(DataType type, ArrowVectorAccessor accessor) {
    VortexArrowColumnVector column = new VortexArrowColumnVector(type);
    column.accessor = accessor;
    return column;
  }

  /** A column of {@code type} that is null in every one of its {@code rowCount} rows. */
  static VortexArrowColumnVector nulls(DataType type, int rowCount) {
    VortexArrowColumnVector column = withAccessor(type, new NullAccessor(new NullVector("", rowCount)));
    if (type instanceof StructType) {
      StructType struct = (StructType) type;
      column.childColumns = Arrays.stream(struct.fields())
          .map(field -> nulls(field.dataType(), rowCount))
          .toArray(VortexArrowColumnVector[]::new);
    }
    return column;
  }

  /** This struct column read as {@code type}, with {@code fields} as its fields. */
  VortexArrowColumnVector withFields(DataType type, VortexArrowColumnVector[] fields) {
    VortexArrowColumnVector column = withAccessor(type, accessor);
    column.childColumns = fields;
    return column;
  }

  /**
   * Returns a view of this column with the shape of {@code requested}. The fields of a struct are
   * found by name, thus the view has the requested fields in the requested order. A requested
   * field that this column does not have is null. Arrays and maps get a view of their elements.
   *
   * <p>Spark removes the struct fields that a query does not read from the requested type, and
   * then reads the fields by position. Thus, a column must have the requested shape.
   *
   * @throws UnsupportedOperationException if a value type is different from the requested type
   */
  VortexArrowColumnVector conformTo(DataType requested) {
    DataType type = dataType();
    if (hasSameShape(type, requested)) {
      return this;
    }

    if (type instanceof StructType && requested instanceof StructType) {
      StructType fileStruct = (StructType) type;
      StructField[] requestedFields = ((StructType) requested).fields();
      VortexArrowColumnVector[] fields = new VortexArrowColumnVector[requestedFields.length];
      for (int i = 0; i < requestedFields.length; i++) {
        int index = VortexScans.fieldIndex(fileStruct, requestedFields[i].name());
        fields[i] = index < 0
            ? nulls(requestedFields[i].dataType(), accessor.vector.getValueCount())
            : childColumns[index].conformTo(requestedFields[i].dataType());
      }
      return withFields(requested, fields);
    }

    if (type instanceof ArrayType && requested instanceof ArrayType) {
      DataType elementType = ((ArrayType) requested).elementType();
      return withAccessor(requested, accessor.withElements(elementType));
    }

    if (type instanceof MapType && requested instanceof MapType) {
      MapType map = (MapType) requested;
      return withAccessor(requested, accessor.withEntries(map.keyType(), map.valueType()));
    }

    if (isWidening(type, requested)) {
      // The accessors of the narrow types also give the values as the wider types.
      return withAccessor(requested, accessor);
    }

    throw new UnsupportedOperationException(
        "Cannot read a Vortex column of type " + type.catalogString() + " as " + requested.catalogString());
  }

  /**
   * Returns true if a column of {@code fileType} can give a view of type {@code requested}, with
   * the same rules as {@link #conformTo}: the fields of a struct are found by name, a requested
   * field that the file does not have is null, and a narrow number type can be read as a wider one.
   */
  public static boolean canConformTo(DataType fileType, DataType requested) {
    if (hasSameShape(fileType, requested) || isWidening(fileType, requested)) {
      return true;
    }

    if (fileType instanceof StructType && requested instanceof StructType) {
      StructType fileStruct = (StructType) fileType;
      for (StructField field : ((StructType) requested).fields()) {
        int index = VortexScans.fieldIndex(fileStruct, field.name());
        if (index >= 0 && !canConformTo(fileStruct.fields()[index].dataType(), field.dataType())) {
          return false;
        }
      }
      return true;
    }

    if (fileType instanceof ArrayType && requested instanceof ArrayType) {
      return canConformTo(((ArrayType) fileType).elementType(), ((ArrayType) requested).elementType());
    }

    if (fileType instanceof MapType && requested instanceof MapType) {
      MapType fileMap = (MapType) fileType;
      MapType requestedMap = (MapType) requested;
      return canConformTo(fileMap.keyType(), requestedMap.keyType())
          && canConformTo(fileMap.valueType(), requestedMap.valueType());
    }
    return false;
  }

  /**
   * Returns true if each value of {@code fileType} is exactly a value of {@code requested}, and the
   * accessor of {@code fileType} gives it with the getter of {@code requested}.
   */
  private static boolean isWidening(DataType fileType, DataType requested) {
    if (fileType instanceof ByteType) {
      return requested instanceof ShortType || requested instanceof IntegerType || requested instanceof LongType;
    }

    if (fileType instanceof ShortType) {
      return requested instanceof IntegerType || requested instanceof LongType;
    }

    if (fileType instanceof IntegerType) {
      return requested instanceof LongType || requested instanceof DoubleType;
    }

    if (fileType instanceof FloatType) {
      return requested instanceof DoubleType;
    }
    return false;
  }

  /**
   * Returns true if the two types are the same when nullability is not compared.
   */
  static boolean hasSameShape(DataType left, DataType right) {
    if (left instanceof StructType && right instanceof StructType) {
      StructField[] leftFields = ((StructType) left).fields();
      StructField[] rightFields = ((StructType) right).fields();
      if (leftFields.length != rightFields.length) {
        return false;
      }
      for (int i = 0; i < leftFields.length; i++) {
        if (!leftFields[i].name().equals(rightFields[i].name())
            || !hasSameShape(leftFields[i].dataType(), rightFields[i].dataType())) {
          return false;
        }
      }
      return true;
    }

    if (left instanceof ArrayType && right instanceof ArrayType) {
      return hasSameShape(((ArrayType) left).elementType(), ((ArrayType) right).elementType());
    }

    if (left instanceof MapType && right instanceof MapType) {
      MapType leftMap = (MapType) left;
      MapType rightMap = (MapType) right;
      return hasSameShape(leftMap.keyType(), rightMap.keyType())
          && hasSameShape(leftMap.valueType(), rightMap.valueType());
    }
    return left.equals(right);
  }

  void initAccessor(ValueVector vector) {
    if (vector instanceof BitVector) {
      BitVector bitVector = (BitVector) vector;
      accessor = new VortexArrowColumnVector.BooleanAccessor(bitVector);
    } else if (vector instanceof TinyIntVector) {
      TinyIntVector tinyIntVector = (TinyIntVector) vector;
      accessor = new VortexArrowColumnVector.ByteAccessor(tinyIntVector);
    } else if (vector instanceof SmallIntVector) {
      SmallIntVector smallIntVector = (SmallIntVector) vector;
      accessor = new VortexArrowColumnVector.ShortAccessor(smallIntVector);
    } else if (vector instanceof IntVector) {
      IntVector intVector = (IntVector) vector;
      accessor = new VortexArrowColumnVector.IntAccessor(intVector);
    } else if (vector instanceof BigIntVector) {
      BigIntVector bigIntVector = (BigIntVector) vector;
      accessor = new VortexArrowColumnVector.LongAccessor(bigIntVector);
    } else if (vector instanceof Float4Vector) {
      Float4Vector float4Vector = (Float4Vector) vector;
      accessor = new VortexArrowColumnVector.FloatAccessor(float4Vector);
    } else if (vector instanceof Float8Vector) {
      Float8Vector float8Vector = (Float8Vector) vector;
      accessor = new VortexArrowColumnVector.DoubleAccessor(float8Vector);
    } else if (vector instanceof DecimalVector) {
      DecimalVector decimalVector = (DecimalVector) vector;
      int precision = decimalVector.getPrecision();
      if (precision > 0 && precision <= Decimal.MAX_LONG_DIGITS()) {
        accessor = new VortexArrowColumnVector.SmallDecimalAccessor(decimalVector, ByteOrder.nativeOrder());
      } else {
        accessor = new VortexArrowColumnVector.DecimalAccessor(decimalVector, ByteOrder.nativeOrder());
      }
    } else if (vector instanceof VarCharVector) {
      VarCharVector varCharVector = (VarCharVector) vector;
      accessor = new VortexArrowColumnVector.StringAccessor(varCharVector);
    } else if (vector instanceof LargeVarCharVector) {
      LargeVarCharVector largeVarCharVector = (LargeVarCharVector) vector;
      accessor = new VortexArrowColumnVector.LargeStringAccessor(largeVarCharVector);
    } else if (vector instanceof ViewVarCharVector) {
      ViewVarCharVector viewVarCharVector = (ViewVarCharVector) vector;
      accessor = new VortexArrowColumnVector.StringViewAccessor(viewVarCharVector);
    } else if (vector instanceof VarBinaryVector) {
      VarBinaryVector varBinaryVector = (VarBinaryVector) vector;
      accessor = new VortexArrowColumnVector.BinaryAccessor(varBinaryVector);
    } else if (vector instanceof LargeVarBinaryVector) {
      LargeVarBinaryVector largeVarBinaryVector = (LargeVarBinaryVector) vector;
      accessor = new VortexArrowColumnVector.LargeBinaryAccessor(largeVarBinaryVector);
    } else if (vector instanceof ViewVarBinaryVector) {
      ViewVarBinaryVector viewVarBinaryVector = (ViewVarBinaryVector) vector;
      accessor = new VortexArrowColumnVector.BinaryViewAccessor(viewVarBinaryVector);
    } else if (vector instanceof DateDayVector) {
      DateDayVector dateDayVector = (DateDayVector) vector;
      accessor = new VortexArrowColumnVector.DateAccessor(dateDayVector);
    } else if (vector instanceof TimeStampVector) {
      TimeStampVector timeStampVector = (TimeStampVector) vector;
      // Covers all eight unit/timezone variants; values are normalized to microseconds.
      accessor = new VortexArrowColumnVector.TimestampAccessor(timeStampVector);
    } else if (vector instanceof MapVector) {
      MapVector mapVector = (MapVector) vector;
      // MapVector extends ListVector, so this check must come first.
      accessor = new VortexArrowColumnVector.MapAccessor(mapVector);
    } else if (vector instanceof ListVector) {
      ListVector listVector = (ListVector) vector;
      accessor = new VortexArrowColumnVector.ArrayAccessor(listVector);
    } else if (vector instanceof ListViewVector) {
      ListViewVector listViewVector = (ListViewVector) vector;
      accessor = new VortexArrowColumnVector.ListViewAccessor(listViewVector);
    } else if (vector instanceof StructVector) {
      StructVector structVector = (StructVector) vector;
      accessor = new VortexArrowColumnVector.StructAccessor(structVector);

      childColumns = new VortexArrowColumnVector[structVector.size()];
      for (int i = 0; i < childColumns.length; ++i) {
        childColumns[i] = new VortexArrowColumnVector(structVector.getVectorById(i));
      }
    } else if (vector instanceof NullVector) {
      NullVector nullVector = (NullVector) vector;
      accessor = new VortexArrowColumnVector.NullAccessor(nullVector);
    } else if (vector instanceof IntervalYearVector) {
      IntervalYearVector intervalYearVector = (IntervalYearVector) vector;
      accessor = new VortexArrowColumnVector.IntervalYearAccessor(intervalYearVector);
    } else if (vector instanceof IntervalMonthDayNanoVector) {
      IntervalMonthDayNanoVector intervalMonthDayNanoVector = (IntervalMonthDayNanoVector) vector;
      accessor = new VortexArrowColumnVector.IntervalMonthDayNanoAccessor(intervalMonthDayNanoVector);
      // CalendarInterval values are read through ColumnVector.getInterval, which uses the
      // months/days/microseconds child column protocol.
      childColumns = new VortexArrowColumnVector[] {
        withAccessor(DataTypes.IntegerType, new IntervalMonthsAccessor(intervalMonthDayNanoVector)),
        withAccessor(DataTypes.IntegerType, new IntervalDaysAccessor(intervalMonthDayNanoVector)),
        withAccessor(DataTypes.LongType, new IntervalMicrosAccessor(intervalMonthDayNanoVector)),
      };
    } else if (vector instanceof DurationVector) {
      DurationVector durationVector = (DurationVector) vector;
      accessor = new VortexArrowColumnVector.DurationAccessor(durationVector);
    } else {
      throw new UnsupportedOperationException("Unsupported Arrow vector type: "
          + vector.getClass().getSimpleName() + " for field " + vector.getField());
    }
  }

  abstract static class ArrowVectorAccessor {
    /** Marks a vector whose validity {@link #isNullAt} asks the vector for, as it is not a plain bitmap. */
    private static final long VALIDITY_FROM_VECTOR = -1;

    final ValueVector vector;

    private final int nullCount;

    /**
     * The address of the vector's validity bitmap, 0 when every value is valid, or {@link #VALIDITY_FROM_VECTOR}.
     *
     * <p>Spark reads every Vortex column as nullable, so generated code checks each value for null. Reading the
     * bit directly, or nothing at all, keeps that check off Arrow's interface calls.
     */
    private final long validityAddress;

    ArrowVectorAccessor(ValueVector vector) {
      this.vector = vector;
      this.nullCount = vector.getNullCount();
      this.validityAddress = validityAddress(vector, nullCount);
    }

    private static long validityAddress(ValueVector vector, int nullCount) {
      if (nullCount == 0) {
        return 0;
      }
      if (vector instanceof BaseFixedWidthVector
          || vector instanceof BaseVariableWidthVector
          || vector instanceof BaseLargeVariableWidthVector
          || vector instanceof BaseVariableWidthViewVector) {
        return vector.getValidityBuffer().memoryAddress();
      }
      return VALIDITY_FROM_VECTOR;
    }

    final boolean isNullAt(int rowId) {
      long address = validityAddress;
      if (address == 0) {
        return false;
      }
      if (address == VALIDITY_FROM_VECTOR) {
        return vector.isNull(rowId);
      }
      return (Platform.getByte(null, address + (rowId >>> 3)) & (1 << (rowId & 7))) == 0;
    }

    final int getNullCount() {
      return nullCount;
    }

    /** The address of a fixed-width vector's values. */
    static long valuesAddress(BaseFixedWidthVector vector) {
      return vector.getDataBuffer().memoryAddress();
    }

    boolean getBoolean(int rowId) {
      throw new UnsupportedOperationException(getClass().getName());
    }

    byte getByte(int rowId) {
      throw new UnsupportedOperationException(getClass().getName());
    }

    short getShort(int rowId) {
      throw new UnsupportedOperationException(getClass().getName());
    }

    int getInt(int rowId) {
      throw new UnsupportedOperationException(getClass().getName());
    }

    long getLong(int rowId) {
      throw new UnsupportedOperationException(getClass().getName());
    }

    float getFloat(int rowId) {
      throw new UnsupportedOperationException(getClass().getName());
    }

    double getDouble(int rowId) {
      throw new UnsupportedOperationException(getClass().getName());
    }

    Decimal getDecimal(int rowId, int precision, int scale) {
      throw new UnsupportedOperationException(getClass().getName());
    }

    UTF8String getUTF8String(int rowId) {
      throw new UnsupportedOperationException(getClass().getName());
    }

    byte[] getBinary(int rowId) {
      throw new UnsupportedOperationException(getClass().getName());
    }

    ColumnarArray getArray(int rowId) {
      throw new UnsupportedOperationException(getClass().getName());
    }

    ColumnarMap getMap(int rowId) {
      throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Returns an accessor for the same arrays whose elements have the shape of {@code elementType}.
     */
    ArrowVectorAccessor withElements(DataType elementType) {
      throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Returns an accessor for the same maps whose keys and values have the requested shapes.
     */
    ArrowVectorAccessor withEntries(DataType keyType, DataType valueType) {
      throw new UnsupportedOperationException(getClass().getName());
    }
  }
  static class BooleanAccessor extends VortexArrowColumnVector.ArrowVectorAccessor {

    private final long values;

    BooleanAccessor(BitVector vector) {
      super(vector);
      this.values = valuesAddress(vector);
    }

    @Override
    final boolean getBoolean(int rowId) {
      return (Platform.getByte(null, values + (rowId >>> 3)) & (1 << (rowId & 7))) != 0;
    }
  }
  static class ByteAccessor extends VortexArrowColumnVector.ArrowVectorAccessor {

    private final long values;

    ByteAccessor(TinyIntVector vector) {
      super(vector);
      this.values = valuesAddress(vector);
    }

    @Override
    final byte getByte(int rowId) {
      return Platform.getByte(null, values + rowId);
    }

    @Override
    final short getShort(int rowId) {
      return getByte(rowId);
    }

    @Override
    final int getInt(int rowId) {
      return getByte(rowId);
    }

    @Override
    final long getLong(int rowId) {
      return getByte(rowId);
    }
  }
  static class ShortAccessor extends VortexArrowColumnVector.ArrowVectorAccessor {

    private final long values;

    ShortAccessor(SmallIntVector vector) {
      super(vector);
      this.values = valuesAddress(vector);
    }

    @Override
    final short getShort(int rowId) {
      return Platform.getShort(null, values + ((long) rowId << 1));
    }

    @Override
    final int getInt(int rowId) {
      return getShort(rowId);
    }

    @Override
    final long getLong(int rowId) {
      return getShort(rowId);
    }
  }
  static class IntAccessor extends VortexArrowColumnVector.ArrowVectorAccessor {

    private final long values;

    IntAccessor(IntVector vector) {
      super(vector);
      this.values = valuesAddress(vector);
    }

    @Override
    final int getInt(int rowId) {
      return Platform.getInt(null, values + ((long) rowId << 2));
    }

    @Override
    final long getLong(int rowId) {
      return getInt(rowId);
    }

    @Override
    final double getDouble(int rowId) {
      return getInt(rowId);
    }
  }
  static class LongAccessor extends VortexArrowColumnVector.ArrowVectorAccessor {

    private final long values;

    LongAccessor(BigIntVector vector) {
      super(vector);
      this.values = valuesAddress(vector);
    }

    @Override
    final long getLong(int rowId) {
      return Platform.getLong(null, values + ((long) rowId << 3));
    }
  }
  static final class RowIndexAccessor extends VortexArrowColumnVector.ArrowVectorAccessor {

    private final long values;

    RowIndexAccessor(UInt8Vector vector) {
      super(vector);
      this.values = valuesAddress(vector);
    }

    @Override
    long getLong(int rowId) {
      return Platform.getLong(null, values + ((long) rowId << 3));
    }
  }

  static class FloatAccessor extends VortexArrowColumnVector.ArrowVectorAccessor {

    private final long values;

    FloatAccessor(Float4Vector vector) {
      super(vector);
      this.values = valuesAddress(vector);
    }

    @Override
    final float getFloat(int rowId) {
      return Platform.getFloat(null, values + ((long) rowId << 2));
    }

    @Override
    final double getDouble(int rowId) {
      return getFloat(rowId);
    }
  }
  static class DoubleAccessor extends VortexArrowColumnVector.ArrowVectorAccessor {

    private final long values;

    DoubleAccessor(Float8Vector vector) {
      super(vector);
      this.values = valuesAddress(vector);
    }

    @Override
    final double getDouble(int rowId) {
      return Platform.getDouble(null, values + ((long) rowId << 3));
    }
  }
  static class DecimalAccessor extends VortexArrowColumnVector.ArrowVectorAccessor {
    /** The values with an absolute value less than this bound have a compact Spark Decimal. */
    private static final long COMPACT_BOUND = 1_000_000_000_000_000_000L;

    private final DecimalVector accessor;

    private final long values;

    private final int lowWordOffset;

    private final int highWordOffset;

    DecimalAccessor(DecimalVector vector, ByteOrder byteOrder) {
      super(vector);
      this.accessor = vector;
      this.values = valuesAddress(vector);
      // Arrow Java stores Decimal128 in native byte order, including the order of its two words.
      this.lowWordOffset = byteOrder == ByteOrder.LITTLE_ENDIAN ? 0 : Long.BYTES;
      this.highWordOffset = byteOrder == ByteOrder.LITTLE_ENDIAN ? Long.BYTES : 0;
    }

    @Override
    final Decimal getDecimal(int rowId, int precision, int scale) {
      if (isNullAt(rowId)) {
        return null;
      }

      long value = values + (long) rowId * DecimalVector.TYPE_WIDTH;
      long unscaled = Platform.getLong(null, value + lowWordOffset);
      long high = Platform.getLong(null, value + highWordOffset);
      if (high == (unscaled >> 63)
          && unscaled < COMPACT_BOUND
          && unscaled > -COMPACT_BOUND
          && scale == accessor.getScale()
          && precision > Decimal.MAX_LONG_DIGITS()) {
        // The value has at most 18 digits, thus it is a value of the requested type as it is. Spark
        // keeps such a value as a long, without a BigDecimal, also for a wide decimal type.
        return Decimal.apply(unscaled, precision, scale);
      }
      return Decimal.apply(accessor.getObject(rowId), precision, scale);
    }
  }

  static final class SmallDecimalAccessor extends VortexArrowColumnVector.ArrowVectorAccessor {
    private static final long[] LONG_POWERS_OF_TEN = longPowersOfTen();
    private final long values;

    private final DecimalVector accessor;
    private final int lowWordOffset;
    private final int highWordOffset;

    SmallDecimalAccessor(DecimalVector vector, ByteOrder byteOrder) {
      super(vector);
      this.accessor = vector;
      this.values = valuesAddress(vector);
      // Arrow Java stores Decimal128 in native byte order, including the order of its two words.
      this.lowWordOffset = byteOrder == ByteOrder.LITTLE_ENDIAN ? 0 : Long.BYTES;
      this.highWordOffset = byteOrder == ByteOrder.LITTLE_ENDIAN ? Long.BYTES : 0;
    }

    private static long[] longPowersOfTen() {
      long[] powers = new long[Decimal.MAX_LONG_DIGITS() + 1];
      powers[0] = 1;
      for (int i = 1; i < powers.length; i++) {
        powers[i] = powers[i - 1] * 10;
      }
      return powers;
    }

    /**
     * Whether every value of the source type is a value of {@code DECIMAL(precision, scale)} with the same unscaled
     * value, which then fits a long.
     */
    private boolean fitsRequestedType(int precision, int scale) {
      return scale >= 0
          && scale == accessor.getScale()
          && accessor.getPrecision() <= precision
          && precision <= Decimal.MAX_LONG_DIGITS();
    }

    /** Whether {@code unscaled} has at most the source precision's digits, as every valid value does. */
    private boolean fitsSourcePrecision(long unscaled) {
      long bound = LONG_POWERS_OF_TEN[accessor.getPrecision()];
      return unscaled < bound && unscaled > -bound;
    }

    @Override
    final Decimal getDecimal(int rowId, int precision, int scale) {
      if (isNullAt(rowId)) {
        return null;
      }
      long value = values + (long) rowId * DecimalVector.TYPE_WIDTH;
      long unscaled = Platform.getLong(null, value + lowWordOffset);
      long high = Platform.getLong(null, value + highWordOffset);
      if (high == (unscaled >> 63)) {
        if (fitsRequestedType(precision, scale) && fitsSourcePrecision(unscaled)) {
          // The value is a value of the requested type as it is. Spark's own vectorized readers return
          // such decimals as a long, without a BigDecimal per value.
          return Decimal.createUnsafe(unscaled, precision, scale);
        }
        // Decode with the source scale; Spark performs the requested rescaling and overflow checks.
        // Keep the expanded Decimal representation for Spark's checked integer casts.
        return Decimal.apply(BigDecimal.valueOf(unscaled, accessor.getScale()), precision, scale);
      }
      // Preserve all 128 bits when the stored integer does not fit in a long.
      return Decimal.apply(accessor.getObject(rowId), precision, scale);
    }
  }

  /**
   * Reads strings straight from the vector's buffers, whose addresses it keeps. The column checks for null before it
   * asks for a value, so the accessor does not check again.
   */
  static class StringAccessor extends VortexArrowColumnVector.ArrowVectorAccessor {

    private final long offsets;
    private final long data;

    StringAccessor(VarCharVector vector) {
      super(vector);
      this.offsets = vector.getOffsetBuffer().memoryAddress();
      this.data = vector.getDataBuffer().memoryAddress();
    }

    @Override
    final UTF8String getUTF8String(int rowId) {
      long offset = offsets + ((long) rowId << 2);
      int start = Platform.getInt(null, offset);
      int end = Platform.getInt(null, offset + Integer.BYTES);
      return UTF8String.fromAddress(null, data + start, end - start);
    }
  }

  /** Reads large strings like {@link StringAccessor}, through 64-bit offsets. */
  static class LargeStringAccessor extends VortexArrowColumnVector.ArrowVectorAccessor {

    private final long offsets;
    private final long data;

    LargeStringAccessor(LargeVarCharVector vector) {
      super(vector);
      this.offsets = vector.getOffsetBuffer().memoryAddress();
      this.data = vector.getDataBuffer().memoryAddress();
    }

    @Override
    final UTF8String getUTF8String(int rowId) {
      long offset = offsets + ((long) rowId << 3);
      long start = Platform.getLong(null, offset);
      long end = Platform.getLong(null, offset + Long.BYTES);
      // A single string cannot be larger than the max integer size, so the conversion is safe.
      return UTF8String.fromAddress(null, data + start, (int) (end - start));
    }
  }
  static class StringViewAccessor extends VortexArrowColumnVector.ArrowVectorAccessor {

    private final long views;

    /** The address of each data buffer, by the index a view gives. */
    private final long[] data;

    StringViewAccessor(ViewVarCharVector vector) {
      super(vector);
      this.views = vector.getDataBuffer().memoryAddress();
      this.data = vector.getDataBuffers().stream()
          .mapToLong(ArrowBuf::memoryAddress)
          .toArray();
    }

    /**
     * Aliases the value's bytes, like {@link StringAccessor}. A view holds its length in its first four bytes, and
     * then either the value itself when it is short, or a prefix, the index of a data buffer, and an offset into it.
     */
    @Override
    final UTF8String getUTF8String(int rowId) {
      long view = views + (long) rowId * BaseVariableWidthViewVector.ELEMENT_SIZE;
      int length = Platform.getInt(null, view);
      if (length <= BaseVariableWidthViewVector.INLINE_SIZE) {
        return UTF8String.fromAddress(null, view + Integer.BYTES, length);
      }

      long buffer = data[Platform.getInt(null, view + 2L * Integer.BYTES)];
      long offset = Integer.toUnsignedLong(Platform.getInt(null, view + 3L * Integer.BYTES));
      return UTF8String.fromAddress(null, buffer + offset, length);
    }
  }
  static class BinaryAccessor extends VortexArrowColumnVector.ArrowVectorAccessor {

    private final VarBinaryVector accessor;

    BinaryAccessor(VarBinaryVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final byte[] getBinary(int rowId) {
      return accessor.getObject(rowId);
    }
  }
  static class LargeBinaryAccessor extends VortexArrowColumnVector.ArrowVectorAccessor {

    private final LargeVarBinaryVector accessor;

    LargeBinaryAccessor(LargeVarBinaryVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final byte[] getBinary(int rowId) {
      return accessor.getObject(rowId);
    }
  }
  static class BinaryViewAccessor extends VortexArrowColumnVector.ArrowVectorAccessor {

    private final ViewVarBinaryVector accessor;

    BinaryViewAccessor(ViewVarBinaryVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final byte[] getBinary(int rowId) {
      return accessor.getObject(rowId);
    }
  }
  static class DateAccessor extends VortexArrowColumnVector.ArrowVectorAccessor {

    private final long values;

    DateAccessor(DateDayVector vector) {
      super(vector);
      this.values = valuesAddress(vector);
    }

    @Override
    final int getInt(int rowId) {
      return Platform.getInt(null, values + ((long) rowId << 2));
    }
  }

  /**
   * Reads any of the eight timestamp vector variants (four units, with or without timezone),
   * normalizing values to the microseconds Spark expects for TimestampType and TimestampNTZType.
   * Seconds and milliseconds fail on overflow rather than silently wrapping; nanoseconds floor
   * towards negative infinity, matching Spark's nanosecond-to-microsecond conversions.
   */
  static class TimestampAccessor extends VortexArrowColumnVector.ArrowVectorAccessor {

    private final long values;
    private final TimeUnit unit;

    TimestampAccessor(TimeStampVector vector) {
      super(vector);
      this.values = valuesAddress(vector);
      this.unit = ((ArrowType.Timestamp) vector.getField().getType()).getUnit();
    }

    @Override
    final long getLong(int rowId) {
      long value = Platform.getLong(null, values + ((long) rowId << 3));
      switch (unit) {
        case SECOND:
          return Math.multiplyExact(value, 1_000_000L);
        case MILLISECOND:
          return Math.multiplyExact(value, 1_000L);
        case MICROSECOND:
          return value;
        case NANOSECOND:
          return Math.floorDiv(value, 1_000L);
        default:
          throw new UnsupportedOperationException("Unsupported timestamp unit: " + unit);
      }
    }
  }
  static class ArrayAccessor extends VortexArrowColumnVector.ArrowVectorAccessor {

    private final ListVector accessor;
    private final VortexArrowColumnVector arrayData;

    ArrayAccessor(ListVector vector) {
      this(vector, new VortexArrowColumnVector(vector.getDataVector()));
    }

    private ArrayAccessor(ListVector vector, VortexArrowColumnVector arrayData) {
      super(vector);
      this.accessor = vector;
      this.arrayData = arrayData;
    }

    @Override
    ArrowVectorAccessor withElements(DataType elementType) {
      return new ArrayAccessor(accessor, arrayData.conformTo(elementType));
    }

    @Override
    final ColumnarArray getArray(int rowId) {
      int start = accessor.getElementStartIndex(rowId);
      int end = accessor.getElementEndIndex(rowId);
      return new ColumnarArray(arrayData, start, end - start);
    }
  }
  static class ListViewAccessor extends VortexArrowColumnVector.ArrowVectorAccessor {

    private final ListViewVector accessor;
    private final VortexArrowColumnVector arrayData;

    ListViewAccessor(ListViewVector vector) {
      this(vector, new VortexArrowColumnVector(vector.getDataVector()));
    }

    private ListViewAccessor(ListViewVector vector, VortexArrowColumnVector arrayData) {
      super(vector);
      this.accessor = vector;
      this.arrayData = arrayData;
    }

    @Override
    ArrowVectorAccessor withElements(DataType elementType) {
      return new ListViewAccessor(accessor, arrayData.conformTo(elementType));
    }

    @Override
    final ColumnarArray getArray(int rowId) {
      int start = accessor.getElementStartIndex(rowId);
      int end = accessor.getElementEndIndex(rowId);
      return new ColumnarArray(arrayData, start, end - start);
    }
  }

  /**
   * Any call to "get" method will throw UnsupportedOperationException.
   * <p>
   * Access struct values in a ArrowColumnVector doesn't use this accessor. Instead, it uses
   * getStruct() method defined in the parent class. Any call to "get" method in this class is a
   * bug in the code.
   */
  static class StructAccessor extends VortexArrowColumnVector.ArrowVectorAccessor {

    StructAccessor(StructVector vector) {
      super(vector);
    }
  }
  static class MapAccessor extends VortexArrowColumnVector.ArrowVectorAccessor {
    private final MapVector accessor;
    private final VortexArrowColumnVector keys;
    private final VortexArrowColumnVector values;

    MapAccessor(MapVector vector) {
      this(vector,
          new VortexArrowColumnVector(((StructVector) vector.getDataVector()).getChild(MapVector.KEY_NAME)),
          new VortexArrowColumnVector(((StructVector) vector.getDataVector()).getChild(MapVector.VALUE_NAME)));
    }

    private MapAccessor(MapVector vector, VortexArrowColumnVector keys, VortexArrowColumnVector values) {
      super(vector);
      this.accessor = vector;
      this.keys = keys;
      this.values = values;
    }

    @Override
    ArrowVectorAccessor withEntries(DataType keyType, DataType valueType) {
      return new MapAccessor(accessor, keys.conformTo(keyType), values.conformTo(valueType));
    }

    @Override
    final ColumnarMap getMap(int rowId) {
      int index = rowId * MapVector.OFFSET_WIDTH;
      int offset = accessor.getOffsetBuffer().getInt(index);
      int length = accessor.getInnerValueCountAt(rowId);
      return new ColumnarMap(keys, values, offset, length);
    }
  }
  static class NullAccessor extends VortexArrowColumnVector.ArrowVectorAccessor {

    NullAccessor(NullVector vector) {
      super(vector);
    }
  }
  static class IntervalYearAccessor extends VortexArrowColumnVector.ArrowVectorAccessor {

    private final IntervalYearVector accessor;

    IntervalYearAccessor(IntervalYearVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    int getInt(int rowId) {
      return accessor.get(rowId);
    }
  }

  /**
   * Accessor for the {@code MONTH_DAY_NANO} interval vector itself. Values are read through
   * {@link ColumnVector#getInterval(int)}, which uses the months/days/microseconds child
   * columns; this accessor only provides null tracking.
   */
  static class IntervalMonthDayNanoAccessor extends VortexArrowColumnVector.ArrowVectorAccessor {

    IntervalMonthDayNanoAccessor(IntervalMonthDayNanoVector vector) {
      super(vector);
    }
  }
  static class IntervalMonthsAccessor extends VortexArrowColumnVector.ArrowVectorAccessor {

    private final IntervalMonthDayNanoVector accessor;

    IntervalMonthsAccessor(IntervalMonthDayNanoVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final int getInt(int rowId) {
      return IntervalMonthDayNanoVector.getMonths(accessor.getDataBuffer(), rowId);
    }
  }
  static class IntervalDaysAccessor extends VortexArrowColumnVector.ArrowVectorAccessor {

    private final IntervalMonthDayNanoVector accessor;

    IntervalDaysAccessor(IntervalMonthDayNanoVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final int getInt(int rowId) {
      return IntervalMonthDayNanoVector.getDays(accessor.getDataBuffer(), rowId);
    }
  }
  static class IntervalMicrosAccessor extends VortexArrowColumnVector.ArrowVectorAccessor {

    private final IntervalMonthDayNanoVector accessor;

    IntervalMicrosAccessor(IntervalMonthDayNanoVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final long getLong(int rowId) {
      // Truncating division matches Spark's ArrowColumnVector month-day-nano handling.
      return IntervalMonthDayNanoVector.getNanoseconds(accessor.getDataBuffer(), rowId) / 1_000L;
    }
  }
  static class DurationAccessor extends VortexArrowColumnVector.ArrowVectorAccessor {

    private final DurationVector accessor;

    DurationAccessor(DurationVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final long getLong(int rowId) {
      return DurationVector.get(accessor.getDataBuffer(), rowId);
    }
  }
}
