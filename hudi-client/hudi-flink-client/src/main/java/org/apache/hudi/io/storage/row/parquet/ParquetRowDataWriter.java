/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.io.storage.row.parquet;

import org.apache.hudi.common.util.ValidationUtils;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalDataUtils;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.util.Preconditions;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Timestamp;
import java.util.Arrays;

import static org.apache.flink.formats.parquet.utils.ParquetSchemaConverter.computeMinBytesForDecimalPrecision;
import static org.apache.flink.formats.parquet.vector.reader.TimestampColumnReader.JULIAN_EPOCH_OFFSET_DAYS;
import static org.apache.flink.formats.parquet.vector.reader.TimestampColumnReader.MILLIS_IN_DAY;
import static org.apache.flink.formats.parquet.vector.reader.TimestampColumnReader.NANOS_PER_MILLISECOND;
import static org.apache.flink.formats.parquet.vector.reader.TimestampColumnReader.NANOS_PER_SECOND;

/**
 * Writes a record to the Parquet API with the expected schema in order to be written to a file.
 *
 * <p>Reference {@code org.apache.flink.formats.parquet.row.ParquetRowDataWriter}
 * to support timestamp of INT64 8 bytes and complex data types.
 */
public class ParquetRowDataWriter {

  private final RecordConsumer recordConsumer;
  private final boolean utcTimestamp;

  private final FieldWriter[] filedWriters;
  private final String[] fieldNames;

  public ParquetRowDataWriter(
      RecordConsumer recordConsumer,
      RowType rowType,
      GroupType schema,
      boolean utcTimestamp) {
    this.recordConsumer = recordConsumer;
    this.utcTimestamp = utcTimestamp;

    this.filedWriters = new FieldWriter[rowType.getFieldCount()];
    this.fieldNames = rowType.getFieldNames().toArray(new String[0]);
    for (int i = 0; i < rowType.getFieldCount(); i++) {
      this.filedWriters[i] = createWriter(rowType.getTypeAt(i));
    }
  }

  /**
   * It writes a record to Parquet.
   *
   * @param record Contains the record that is going to be written.
   */
  public void write(final RowData record) {
    recordConsumer.startMessage();
    for (int i = 0; i < filedWriters.length; i++) {
      if (!record.isNullAt(i)) {
        String fieldName = fieldNames[i];
        FieldWriter writer = filedWriters[i];

        recordConsumer.startField(fieldName, i);
        writer.write(record, i);
        recordConsumer.endField(fieldName, i);
      }
    }
    recordConsumer.endMessage();
  }

  private FieldWriter createWriter(LogicalType t) {
    switch (t.getTypeRoot()) {
      case CHAR:
      case VARCHAR:
        return new StringWriter();
      case BOOLEAN:
        return new BooleanWriter();
      case BINARY:
      case VARBINARY:
        return new BinaryWriter();
      case DECIMAL:
        DecimalType decimalType = (DecimalType) t;
        return createDecimalWriter(decimalType.getPrecision(), decimalType.getScale());
      case TINYINT:
        return new ByteWriter();
      case SMALLINT:
        return new ShortWriter();
      case DATE:
      case TIME_WITHOUT_TIME_ZONE:
      case INTEGER:
        return new IntWriter();
      case BIGINT:
        return new LongWriter();
      case FLOAT:
        return new FloatWriter();
      case DOUBLE:
        return new DoubleWriter();
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        TimestampType timestampType = (TimestampType) t;
        final int tsPrecision = timestampType.getPrecision();
        if (tsPrecision == 3 || tsPrecision == 6) {
          return new Timestamp64Writer(tsPrecision);
        } else {
          return new Timestamp96Writer(tsPrecision);
        }
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        LocalZonedTimestampType localZonedTimestampType = (LocalZonedTimestampType) t;
        final int tsLtzPrecision = localZonedTimestampType.getPrecision();
        if (tsLtzPrecision == 3 || tsLtzPrecision == 6) {
          return new Timestamp64Writer(tsLtzPrecision);
        } else {
          return new Timestamp96Writer(tsLtzPrecision);
        }
      case ARRAY:
        ArrayType arrayType = (ArrayType) t;
        LogicalType elementType = arrayType.getElementType();
        FieldWriter elementWriter = createWriter(elementType);
        return new ArrayWriter(elementWriter);
      case MAP:
        MapType mapType = (MapType) t;
        LogicalType keyType = mapType.getKeyType();
        LogicalType valueType = mapType.getValueType();
        FieldWriter keyWriter = createWriter(keyType);
        FieldWriter valueWriter = createWriter(valueType);
        return new MapWriter(keyWriter, valueWriter);
      case ROW:
        RowType rowType = (RowType) t;
        FieldWriter[] fieldWriters = rowType.getFields().stream()
            .map(RowType.RowField::getType).map(this::createWriter).toArray(FieldWriter[]::new);
        String[] fieldNames = rowType.getFields().stream()
            .map(RowType.RowField::getName).toArray(String[]::new);
        return new RowWriter(fieldNames, fieldWriters);
      default:
        throw new UnsupportedOperationException("Unsupported type: " + t);
    }
  }

  private interface FieldWriter {
    void write(RowData row, int ordinal);

    void write(ArrayData array, int ordinal);
  }

  private class BooleanWriter implements FieldWriter {

    @Override
    public void write(RowData row, int ordinal) {
      recordConsumer.addBoolean(row.getBoolean(ordinal));
    }

    @Override
    public void write(ArrayData array, int ordinal) {
      recordConsumer.addBoolean(array.getBoolean(ordinal));
    }
  }

  private class ByteWriter implements FieldWriter {

    @Override
    public void write(RowData row, int ordinal) {
      recordConsumer.addInteger(row.getByte(ordinal));
    }

    @Override
    public void write(ArrayData array, int ordinal) {
      recordConsumer.addInteger(array.getByte(ordinal));
    }
  }

  private class ShortWriter implements FieldWriter {

    @Override
    public void write(RowData row, int ordinal) {
      recordConsumer.addInteger(row.getShort(ordinal));
    }

    @Override
    public void write(ArrayData array, int ordinal) {
      recordConsumer.addInteger(array.getShort(ordinal));
    }
  }

  private class LongWriter implements FieldWriter {

    @Override
    public void write(RowData row, int ordinal) {
      recordConsumer.addLong(row.getLong(ordinal));
    }

    @Override
    public void write(ArrayData array, int ordinal) {
      recordConsumer.addLong(array.getLong(ordinal));
    }
  }

  private class FloatWriter implements FieldWriter {

    @Override
    public void write(RowData row, int ordinal) {
      recordConsumer.addFloat(row.getFloat(ordinal));
    }

    @Override
    public void write(ArrayData array, int ordinal) {
      recordConsumer.addFloat(array.getFloat(ordinal));
    }
  }

  private class DoubleWriter implements FieldWriter {

    @Override
    public void write(RowData row, int ordinal) {
      recordConsumer.addDouble(row.getDouble(ordinal));
    }

    @Override
    public void write(ArrayData array, int ordinal) {
      recordConsumer.addDouble(array.getDouble(ordinal));
    }
  }

  private class StringWriter implements FieldWriter {

    @Override
    public void write(RowData row, int ordinal) {
      recordConsumer.addBinary(Binary.fromReusedByteArray(row.getString(ordinal).toBytes()));
    }

    @Override
    public void write(ArrayData array, int ordinal) {
      recordConsumer.addBinary(Binary.fromReusedByteArray(array.getString(ordinal).toBytes()));
    }
  }

  private class BinaryWriter implements FieldWriter {

    @Override
    public void write(RowData row, int ordinal) {
      recordConsumer.addBinary(Binary.fromReusedByteArray(row.getBinary(ordinal)));
    }

    @Override
    public void write(ArrayData array, int ordinal) {
      recordConsumer.addBinary(Binary.fromReusedByteArray(array.getBinary(ordinal)));
    }
  }

  private class IntWriter implements FieldWriter {

    @Override
    public void write(RowData row, int ordinal) {
      recordConsumer.addInteger(row.getInt(ordinal));
    }

    @Override
    public void write(ArrayData array, int ordinal) {
      recordConsumer.addInteger(array.getInt(ordinal));
    }
  }

  /**
   * TIMESTAMP_MILLIS and TIMESTAMP_MICROS is the deprecated ConvertedType of TIMESTAMP with the MILLIS and MICROS
   * precision respectively. See
   * https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#timestamp
   */
  private class Timestamp64Writer implements FieldWriter {
    private final int precision;
    private Timestamp64Writer(int precision) {
      ValidationUtils.checkArgument(precision == 3 || precision == 6,
          "Timestamp64Writer is only able to support precisions of {3, 6}");
      this.precision = precision;
    }

    @Override
    public void write(RowData row, int ordinal) {
      TimestampData timestampData = row.getTimestamp(ordinal, precision);
      recordConsumer.addLong(timestampToInt64(timestampData, precision));
    }

    @Override
    public void write(ArrayData array, int ordinal) {
      TimestampData timestampData = array.getTimestamp(ordinal, precision);
      recordConsumer.addLong(timestampToInt64(timestampData, precision));
    }
  }

  /**
   * Converts a {@code TimestampData} to its corresponding int64 value. This function only accepts TimestampData of
   * precision 3 or 6. Special attention will need to be given to a TimestampData of precision = 6.
   * <p>
   * For example representing `1970-01-01T00:00:03.100001` of precision 6 will have:
   * <ul>
   *   <li>millisecond = 3100</li>
   *   <li>nanoOfMillisecond = 1000</li>
   * </ul>
   * As such, the int64 value will be:
   * <p>
   * millisecond * 1000 + nanoOfMillisecond / 1000
   *
   * @param timestampData TimestampData to be converted to int64 format
   * @param precision the precision of the TimestampData
   * @return int64 value of the TimestampData
   */
  private long timestampToInt64(TimestampData timestampData, int precision) {
    if (precision == 3) {
      return utcTimestamp ? timestampData.getMillisecond() : timestampData.toTimestamp().getTime();
    } else {
      // using an else clause here as precision has been validated to be {3, 6} in the constructor
      // convert timestampData to microseconds format
      return utcTimestamp ? timestampData.getMillisecond() * 1000 + timestampData.getNanoOfMillisecond() / 1000 :
          timestampData.toTimestamp().getTime() * 1000;
    }
  }

  /**
   * Timestamp of INT96 bytes, julianDay(4) + nanosOfDay(8). See
   * https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#timestamp
   * <p>
   * TODO: Leaving this here as there might be a requirement to support TIMESTAMP(9) in the future
   */
  private class Timestamp96Writer implements FieldWriter {

    private final int precision;

    private Timestamp96Writer(int precision) {
      this.precision = precision;
    }

    @Override
    public void write(RowData row, int ordinal) {
      recordConsumer.addBinary(timestampToInt96(row.getTimestamp(ordinal, precision)));
    }

    @Override
    public void write(ArrayData array, int ordinal) {
      recordConsumer.addBinary(timestampToInt96(array.getTimestamp(ordinal, precision)));
    }
  }

  private Binary timestampToInt96(TimestampData timestampData) {
    int julianDay;
    long nanosOfDay;
    if (utcTimestamp) {
      long mills = timestampData.getMillisecond();
      julianDay = (int) ((mills / MILLIS_IN_DAY) + JULIAN_EPOCH_OFFSET_DAYS);
      nanosOfDay =
          (mills % MILLIS_IN_DAY) * NANOS_PER_MILLISECOND
              + timestampData.getNanoOfMillisecond();
    } else {
      Timestamp timestamp = timestampData.toTimestamp();
      long mills = timestamp.getTime();
      julianDay = (int) ((mills / MILLIS_IN_DAY) + JULIAN_EPOCH_OFFSET_DAYS);
      nanosOfDay = ((mills % MILLIS_IN_DAY) / 1000) * NANOS_PER_SECOND + timestamp.getNanos();
    }

    ByteBuffer buf = ByteBuffer.allocate(12);
    buf.order(ByteOrder.LITTLE_ENDIAN);
    buf.putLong(nanosOfDay);
    buf.putInt(julianDay);
    buf.flip();
    return Binary.fromConstantByteBuffer(buf);
  }

  private FieldWriter createDecimalWriter(int precision, int scale) {
    Preconditions.checkArgument(
        precision <= DecimalType.MAX_PRECISION,
        "Decimal precision %s exceeds max precision %s",
        precision,
        DecimalType.MAX_PRECISION);

    /*
     * This is optimizer for UnscaledBytesWriter.
     */
    class LongUnscaledBytesWriter implements FieldWriter {
      private final int numBytes;
      private final int initShift;
      private final byte[] decimalBuffer;

      private LongUnscaledBytesWriter() {
        this.numBytes = computeMinBytesForDecimalPrecision(precision);
        this.initShift = 8 * (numBytes - 1);
        this.decimalBuffer = new byte[numBytes];
      }

      @Override
      public void write(RowData row, int ordinal) {
        long unscaledLong = row.getDecimal(ordinal, precision, scale).toUnscaledLong();
        doWrite(unscaledLong);
      }

      @Override
      public void write(ArrayData array, int ordinal) {
        long unscaledLong = array.getDecimal(ordinal, precision, scale).toUnscaledLong();
        doWrite(unscaledLong);
      }

      private void doWrite(long unscaled) {
        int i = 0;
        int shift = initShift;
        while (i < numBytes) {
          decimalBuffer[i] = (byte) (unscaled >> shift);
          i += 1;
          shift -= 8;
        }

        recordConsumer.addBinary(Binary.fromReusedByteArray(decimalBuffer, 0, numBytes));
      }
    }

    class UnscaledBytesWriter implements FieldWriter {
      private final int numBytes;
      private final byte[] decimalBuffer;

      private UnscaledBytesWriter() {
        this.numBytes = computeMinBytesForDecimalPrecision(precision);
        this.decimalBuffer = new byte[numBytes];
      }

      @Override
      public void write(RowData row, int ordinal) {
        byte[] bytes = row.getDecimal(ordinal, precision, scale).toUnscaledBytes();
        doWrite(bytes);
      }

      @Override
      public void write(ArrayData array, int ordinal) {
        byte[] bytes = array.getDecimal(ordinal, precision, scale).toUnscaledBytes();
        doWrite(bytes);
      }

      private void doWrite(byte[] bytes) {
        byte[] writtenBytes;
        if (bytes.length == numBytes) {
          // Avoid copy.
          writtenBytes = bytes;
        } else {
          byte signByte = bytes[0] < 0 ? (byte) -1 : (byte) 0;
          Arrays.fill(decimalBuffer, 0, numBytes - bytes.length, signByte);
          System.arraycopy(
              bytes, 0, decimalBuffer, numBytes - bytes.length, bytes.length);
          writtenBytes = decimalBuffer;
        }
        recordConsumer.addBinary(Binary.fromReusedByteArray(writtenBytes, 0, numBytes));
      }
    }

    // 1 <= precision <= 18, writes as FIXED_LEN_BYTE_ARRAY
    // optimizer for UnscaledBytesWriter
    if (DecimalDataUtils.is32BitDecimal(precision)
        || DecimalDataUtils.is64BitDecimal(precision)) {
      return new LongUnscaledBytesWriter();
    }

    // 19 <= precision <= 38, writes as FIXED_LEN_BYTE_ARRAY
    return new UnscaledBytesWriter();
  }

  private class ArrayWriter implements FieldWriter {
    private final FieldWriter elementWriter;

    private ArrayWriter(FieldWriter elementWriter) {
      this.elementWriter = elementWriter;
    }

    @Override
    public void write(RowData row, int ordinal) {
      ArrayData arrayData = row.getArray(ordinal);
      doWrite(arrayData);
    }

    @Override
    public void write(ArrayData array, int ordinal) {
      ArrayData arrayData = array.getArray(ordinal);
      doWrite(arrayData);
    }

    private void doWrite(ArrayData arrayData) {
      recordConsumer.startGroup();
      if (arrayData.size() > 0) {
        // align with Spark And Avro regarding the standard mode array type, see:
        // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists
        //
        // <list-repetition> group <name> (LIST) {
        //   repeated group list {
        //     <element-repetition> <element-type> element;
        //   }
        // }
        final String repeatedGroup = "list";
        final String elementField = "element";
        recordConsumer.startField(repeatedGroup, 0);
        for (int i = 0; i < arrayData.size(); i++) {
          recordConsumer.startGroup();
          if (!arrayData.isNullAt(i)) {
            // Only creates the element field if the current array element is not null.
            recordConsumer.startField(elementField, 0);
            elementWriter.write(arrayData, i);
            recordConsumer.endField(elementField, 0);
          }
          recordConsumer.endGroup();
        }
        recordConsumer.endField(repeatedGroup, 0);
      }
      recordConsumer.endGroup();
    }
  }

  private class MapWriter implements FieldWriter {
    private final FieldWriter keyWriter;
    private final FieldWriter valueWriter;

    private MapWriter(FieldWriter keyWriter, FieldWriter valueWriter) {
      this.keyWriter = keyWriter;
      this.valueWriter = valueWriter;
    }

    @Override
    public void write(RowData row, int ordinal) {
      MapData map = row.getMap(ordinal);
      doWrite(map);
    }

    @Override
    public void write(ArrayData array, int ordinal) {
      MapData map = array.getMap(ordinal);
      doWrite(map);
    }

    private void doWrite(MapData mapData) {
      ArrayData keyArray = mapData.keyArray();
      ArrayData valueArray = mapData.valueArray();
      recordConsumer.startGroup();
      if (mapData.size() > 0) {
        final String repeatedGroup = "key_value";
        final String kField = "key";
        final String vField = "value";
        recordConsumer.startField(repeatedGroup, 0);
        for (int i = 0; i < mapData.size(); i++) {
          recordConsumer.startGroup();
          // key
          recordConsumer.startField(kField, 0);
          this.keyWriter.write(keyArray, i);
          recordConsumer.endField(kField, 0);
          // value
          if (!valueArray.isNullAt(i)) {
            // Only creates the "value" field if the value if non-empty
            recordConsumer.startField(vField, 1);
            this.valueWriter.write(valueArray, i);
            recordConsumer.endField(vField, 1);
          }
          recordConsumer.endGroup();
        }
        recordConsumer.endField(repeatedGroup, 0);
      }
      recordConsumer.endGroup();
    }
  }

  private class RowWriter implements FieldWriter {
    private final String[] fieldNames;
    private final FieldWriter[] fieldWriters;

    private RowWriter(String[] fieldNames, FieldWriter[] fieldWriters) {
      this.fieldNames = fieldNames;
      this.fieldWriters = fieldWriters;
    }

    @Override
    public void write(RowData row, int ordinal) {
      RowData nested = row.getRow(ordinal, fieldWriters.length);
      doWrite(nested);
    }

    @Override
    public void write(ArrayData array, int ordinal) {
      RowData nested = array.getRow(ordinal, fieldWriters.length);
      doWrite(nested);
    }

    private void doWrite(RowData row) {
      recordConsumer.startGroup();
      for (int i = 0; i < row.getArity(); i++) {
        if (!row.isNullAt(i)) {
          String fieldName = fieldNames[i];
          recordConsumer.startField(fieldName, i);
          fieldWriters[i].write(row, i);
          recordConsumer.endField(fieldName, i);
        }
      }
      recordConsumer.endGroup();
    }
  }
}

