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

package org.apache.hudi.io.storage.row.lance;

import org.apache.hudi.exception.HoodieNotSupportedException;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;

/**
 * Writes Flink {@link RowData} values into Arrow vectors used by Lance base files.
 *
 * <p>The type-specific writer tree is created once for the supplied Arrow vectors. Record writes
 * then use direct {@link RowData} and {@link ArrayData} accessors without creating field or element
 * getters on the record-level path.
 *
 * <p>Supports primitive types, {@code ROW}, and {@code ARRAY}. {@code MAP} is not supported by the
 * current Lance Java writer. Enable it once Lance adds Java writer support for {@code MAP}.
 *
 * @see <a href="https://github.com/lance-format/lance/issues/3620">Lance issue #3620</a>
 */
public class LanceRowDataWriter {

  private final FieldWriter[] fieldWriters;

  public LanceRowDataWriter(RowType rowType, List<FieldVector> vectors, boolean utcTimestamp) {
    this.fieldWriters = new FieldWriter[rowType.getFieldCount()];
    for (int i = 0; i < fieldWriters.length; i++) {
      fieldWriters[i] = createWriter(rowType.getTypeAt(i), vectors.get(i), utcTimestamp);
    }
  }

  public void write(RowData row, int rowId) {
    for (int i = 0; i < fieldWriters.length; i++) {
      fieldWriters[i].write(row, i, rowId);
    }
  }

  private static FieldWriter createWriter(LogicalType type, FieldVector vector, boolean utcTimestamp) {
    switch (type.getTypeRoot()) {
      case BOOLEAN:
        return new BooleanWriter((BitVector) vector);
      case TINYINT:
        return new TinyIntWriter((TinyIntVector) vector);
      case SMALLINT:
        return new SmallIntWriter((SmallIntVector) vector);
      case INTEGER:
        return new IntWriter((IntVector) vector);
      case DATE:
        return new DateWriter((DateDayVector) vector);
      case TIME_WITHOUT_TIME_ZONE:
        return new TimeWriter((TimeMilliVector) vector);
      case BIGINT:
        return new BigIntWriter((BigIntVector) vector);
      case FLOAT:
        return new FloatWriter((Float4Vector) vector);
      case DOUBLE:
        return new DoubleWriter((Float8Vector) vector);
      case CHAR:
      case VARCHAR:
        return new StringWriter((VarCharVector) vector);
      case BINARY:
      case VARBINARY:
        return new BinaryWriter((VarBinaryVector) vector);
      case DECIMAL:
        return new DecimalWriter((DecimalVector) vector, (DecimalType) type);
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return new TimestampWriter((TimeStampVector) vector, getPrecision(type), utcTimestamp);
      case ROW:
        RowType rowType = (RowType) type;
        StructVector structVector = (StructVector) vector;
        FieldWriter[] fieldWriters = new FieldWriter[rowType.getFieldCount()];
        for (int i = 0; i < fieldWriters.length; i++) {
          fieldWriters[i] = createWriter(
              rowType.getTypeAt(i), (FieldVector) structVector.getChildByOrdinal(i), utcTimestamp);
        }
        return new RowWriter(structVector, fieldWriters);
      case ARRAY:
        ArrayType arrayType = (ArrayType) type;
        ListVector listVector = (ListVector) vector;
        FieldWriter elementWriter = createWriter(
            arrayType.getElementType(), listVector.getDataVector(), utcTimestamp);
        return new ArrayWriter(listVector, elementWriter);
      default:
        throw unsupported(type);
    }
  }

  private abstract static class FieldWriter {
    protected final FieldVector fieldVector;

    private FieldWriter(FieldVector vector) {
      this.fieldVector = vector;
    }

    final void write(RowData row, int ordinal, int rowId) {
      if (row.isNullAt(ordinal)) {
        fieldVector.setNull(rowId);
      } else {
        writeNonNull(row, ordinal, rowId);
      }
    }

    final void write(ArrayData array, int ordinal, int rowId) {
      if (array.isNullAt(ordinal)) {
        fieldVector.setNull(rowId);
      } else {
        writeNonNull(array, ordinal, rowId);
      }
    }

    abstract void writeNonNull(RowData row, int ordinal, int rowId);

    abstract void writeNonNull(ArrayData array, int ordinal, int rowId);
  }

  private static class BooleanWriter extends FieldWriter {
    private final BitVector vector;

    private BooleanWriter(BitVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    void writeNonNull(RowData row, int ordinal, int rowId) {
      vector.setSafe(rowId, row.getBoolean(ordinal) ? 1 : 0);
    }

    @Override
    void writeNonNull(ArrayData array, int ordinal, int rowId) {
      vector.setSafe(rowId, array.getBoolean(ordinal) ? 1 : 0);
    }
  }

  private static class TinyIntWriter extends FieldWriter {
    private final TinyIntVector vector;

    private TinyIntWriter(TinyIntVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    void writeNonNull(RowData row, int ordinal, int rowId) {
      vector.setSafe(rowId, row.getByte(ordinal));
    }

    @Override
    void writeNonNull(ArrayData array, int ordinal, int rowId) {
      vector.setSafe(rowId, array.getByte(ordinal));
    }
  }

  private static class SmallIntWriter extends FieldWriter {
    private final SmallIntVector vector;

    private SmallIntWriter(SmallIntVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    void writeNonNull(RowData row, int ordinal, int rowId) {
      vector.setSafe(rowId, row.getShort(ordinal));
    }

    @Override
    void writeNonNull(ArrayData array, int ordinal, int rowId) {
      vector.setSafe(rowId, array.getShort(ordinal));
    }
  }

  private static class IntWriter extends FieldWriter {
    private final IntVector vector;

    private IntWriter(IntVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    void writeNonNull(RowData row, int ordinal, int rowId) {
      vector.setSafe(rowId, row.getInt(ordinal));
    }

    @Override
    void writeNonNull(ArrayData array, int ordinal, int rowId) {
      vector.setSafe(rowId, array.getInt(ordinal));
    }
  }

  private static class DateWriter extends FieldWriter {
    private final DateDayVector vector;

    private DateWriter(DateDayVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    void writeNonNull(RowData row, int ordinal, int rowId) {
      vector.setSafe(rowId, row.getInt(ordinal));
    }

    @Override
    void writeNonNull(ArrayData array, int ordinal, int rowId) {
      vector.setSafe(rowId, array.getInt(ordinal));
    }
  }

  private static class TimeWriter extends FieldWriter {
    private final TimeMilliVector vector;

    private TimeWriter(TimeMilliVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    void writeNonNull(RowData row, int ordinal, int rowId) {
      vector.setSafe(rowId, row.getInt(ordinal));
    }

    @Override
    void writeNonNull(ArrayData array, int ordinal, int rowId) {
      vector.setSafe(rowId, array.getInt(ordinal));
    }
  }

  private static class BigIntWriter extends FieldWriter {
    private final BigIntVector vector;

    private BigIntWriter(BigIntVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    void writeNonNull(RowData row, int ordinal, int rowId) {
      vector.setSafe(rowId, row.getLong(ordinal));
    }

    @Override
    void writeNonNull(ArrayData array, int ordinal, int rowId) {
      vector.setSafe(rowId, array.getLong(ordinal));
    }
  }

  private static class FloatWriter extends FieldWriter {
    private final Float4Vector vector;

    private FloatWriter(Float4Vector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    void writeNonNull(RowData row, int ordinal, int rowId) {
      vector.setSafe(rowId, row.getFloat(ordinal));
    }

    @Override
    void writeNonNull(ArrayData array, int ordinal, int rowId) {
      vector.setSafe(rowId, array.getFloat(ordinal));
    }
  }

  private static class DoubleWriter extends FieldWriter {
    private final Float8Vector vector;

    private DoubleWriter(Float8Vector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    void writeNonNull(RowData row, int ordinal, int rowId) {
      vector.setSafe(rowId, row.getDouble(ordinal));
    }

    @Override
    void writeNonNull(ArrayData array, int ordinal, int rowId) {
      vector.setSafe(rowId, array.getDouble(ordinal));
    }
  }

  private static class StringWriter extends FieldWriter {
    private final VarCharVector vector;

    private StringWriter(VarCharVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    void writeNonNull(RowData row, int ordinal, int rowId) {
      vector.setSafe(rowId, row.getString(ordinal).toBytes());
    }

    @Override
    void writeNonNull(ArrayData array, int ordinal, int rowId) {
      vector.setSafe(rowId, array.getString(ordinal).toBytes());
    }
  }

  private static class BinaryWriter extends FieldWriter {
    private final VarBinaryVector vector;

    private BinaryWriter(VarBinaryVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    void writeNonNull(RowData row, int ordinal, int rowId) {
      vector.setSafe(rowId, row.getBinary(ordinal));
    }

    @Override
    void writeNonNull(ArrayData array, int ordinal, int rowId) {
      vector.setSafe(rowId, array.getBinary(ordinal));
    }
  }

  private static class DecimalWriter extends FieldWriter {
    private final DecimalVector vector;
    private final int precision;
    private final int scale;

    private DecimalWriter(DecimalVector vector, DecimalType decimalType) {
      super(vector);
      this.vector = vector;
      this.precision = decimalType.getPrecision();
      this.scale = decimalType.getScale();
    }

    @Override
    void writeNonNull(RowData row, int ordinal, int rowId) {
      vector.setSafe(rowId, row.getDecimal(ordinal, precision, scale).toBigDecimal());
    }

    @Override
    void writeNonNull(ArrayData array, int ordinal, int rowId) {
      vector.setSafe(rowId, array.getDecimal(ordinal, precision, scale).toBigDecimal());
    }
  }

  private static class TimestampWriter extends FieldWriter {
    private final TimeStampVector vector;
    private final int precision;
    private final boolean utcTimestamp;

    private TimestampWriter(TimeStampVector vector, int precision, boolean utcTimestamp) {
      super(vector);
      this.vector = vector;
      this.precision = precision;
      this.utcTimestamp = utcTimestamp;
    }

    @Override
    void writeNonNull(RowData row, int ordinal, int rowId) {
      vector.setSafe(rowId, timestampToMicros(row.getTimestamp(ordinal, precision), precision, utcTimestamp));
    }

    @Override
    void writeNonNull(ArrayData array, int ordinal, int rowId) {
      vector.setSafe(rowId, timestampToMicros(array.getTimestamp(ordinal, precision), precision, utcTimestamp));
    }
  }

  private static class RowWriter extends FieldWriter {
    private final StructVector vector;
    private final FieldWriter[] fieldWriters;

    private RowWriter(StructVector vector, FieldWriter[] fieldWriters) {
      super(vector);
      this.vector = vector;
      this.fieldWriters = fieldWriters;
    }

    @Override
    void writeNonNull(RowData row, int ordinal, int rowId) {
      writeRow(row.getRow(ordinal, fieldWriters.length), rowId);
    }

    @Override
    void writeNonNull(ArrayData array, int ordinal, int rowId) {
      writeRow(array.getRow(ordinal, fieldWriters.length), rowId);
    }

    private void writeRow(RowData row, int rowId) {
      vector.setIndexDefined(rowId);
      for (int i = 0; i < fieldWriters.length; i++) {
        fieldWriters[i].write(row, i, rowId);
      }
    }
  }

  private static class ArrayWriter extends FieldWriter {
    private final ListVector vector;
    private final FieldWriter elementWriter;

    private ArrayWriter(ListVector vector, FieldWriter elementWriter) {
      super(vector);
      this.vector = vector;
      this.elementWriter = elementWriter;
    }

    @Override
    void writeNonNull(RowData row, int ordinal, int rowId) {
      writeArray(row.getArray(ordinal), rowId);
    }

    @Override
    void writeNonNull(ArrayData array, int ordinal, int rowId) {
      writeArray(array.getArray(ordinal), rowId);
    }

    private void writeArray(ArrayData array, int rowId) {
      int startIndex = vector.startNewValue(rowId);
      for (int i = 0; i < array.size(); i++) {
        elementWriter.write(array, i, startIndex + i);
      }
      vector.endValue(rowId, array.size());
    }
  }

  private static long timestampToMicros(TimestampData timestampData, int precision, boolean utcTimestamp) {
    long millis = utcTimestamp ? timestampData.getMillisecond() : timestampData.toTimestamp().getTime();
    return precision > 3 && utcTimestamp
        ? millis * 1000L + timestampData.getNanoOfMillisecond() / 1000L
        : millis * 1000L;
  }

  private static HoodieNotSupportedException unsupported(LogicalType type) {
    return new HoodieNotSupportedException(
        "Flink Lance base-file support currently supports primitive, ROW, and ARRAY columns; unsupported type: " + type);
  }
}
