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

package org.apache.hudi.io.storage.row;

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
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;

/**
 * Primitive RowData/Arrow conversion helpers for Flink Lance base files.
 */
public final class HoodieFlinkLanceArrowUtils {

  private HoodieFlinkLanceArrowUtils() {
  }

  public static Schema toArrowSchema(RowType rowType) {
    List<Field> fields = new ArrayList<>(rowType.getFieldCount());
    for (RowType.RowField field : rowType.getFields()) {
      fields.add(toArrowField(field.getName(), field.getType()));
    }
    return new Schema(fields);
  }

  public static RowType toRowType(Schema schema) {
    List<RowType.RowField> fields = new ArrayList<>(schema.getFields().size());
    for (Field field : schema.getFields()) {
      fields.add(new RowType.RowField(field.getName(), toLogicalType(field.getType())));
    }
    return new RowType(fields);
  }

  public static RowData toRowData(RowType rowType, List<FieldVector> vectors, int rowId) {
    GenericRowData rowData = new GenericRowData(vectors.size());
    for (int i = 0; i < vectors.size(); i++) {
      FieldVector vector = vectors.get(i);
      if (vector.isNull(rowId)) {
        rowData.setField(i, null);
      } else {
        rowData.setField(i, readValue(rowType.getTypeAt(i), vector, rowId));
      }
    }
    return rowData;
  }

  public static void writeValue(LogicalType type, FieldVector vector, int rowId, RowData rowData, int ordinal) {
    writeValue(type, vector, rowId, rowData, ordinal, true);
  }

  public static void writeValue(LogicalType type, FieldVector vector, int rowId, RowData rowData, int ordinal, boolean utcTimestamp) {
    if (rowData.isNullAt(ordinal)) {
      vector.setNull(rowId);
      return;
    }
    switch (type.getTypeRoot()) {
      case BOOLEAN:
        ((BitVector) vector).setSafe(rowId, rowData.getBoolean(ordinal) ? 1 : 0);
        return;
      case TINYINT:
        ((TinyIntVector) vector).setSafe(rowId, rowData.getByte(ordinal));
        return;
      case SMALLINT:
        ((SmallIntVector) vector).setSafe(rowId, rowData.getShort(ordinal));
        return;
      case INTEGER:
        ((IntVector) vector).setSafe(rowId, rowData.getInt(ordinal));
        return;
      case DATE:
        ((DateDayVector) vector).setSafe(rowId, rowData.getInt(ordinal));
        return;
      case TIME_WITHOUT_TIME_ZONE:
        ((TimeMilliVector) vector).setSafe(rowId, rowData.getInt(ordinal));
        return;
      case BIGINT:
        ((BigIntVector) vector).setSafe(rowId, rowData.getLong(ordinal));
        return;
      case FLOAT:
        ((Float4Vector) vector).setSafe(rowId, rowData.getFloat(ordinal));
        return;
      case DOUBLE:
        ((Float8Vector) vector).setSafe(rowId, rowData.getDouble(ordinal));
        return;
      case CHAR:
      case VARCHAR:
        ((VarCharVector) vector).setSafe(rowId, rowData.getString(ordinal).toBytes());
        return;
      case BINARY:
      case VARBINARY:
        ((VarBinaryVector) vector).setSafe(rowId, rowData.getBinary(ordinal));
        return;
      case DECIMAL:
        DecimalType decimalType = (DecimalType) type;
        DecimalData decimal = rowData.getDecimal(ordinal, decimalType.getPrecision(), decimalType.getScale());
        ((DecimalVector) vector).setSafe(rowId, decimal.toBigDecimal());
        return;
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        TimestampData timestamp = rowData.getTimestamp(ordinal, getPrecision(type));
        long micros = timestampToMicros(timestamp, getPrecision(type), utcTimestamp);
        ((TimeStampMicroVector) vector).setSafe(rowId, micros);
        return;
      default:
        throw unsupported(type);
    }
  }

  private static Object readValue(LogicalType type, ValueVector vector, int rowId) {
    switch (type.getTypeRoot()) {
      case BOOLEAN:
        return ((BitVector) vector).get(rowId) == 1;
      case TINYINT:
        return ((TinyIntVector) vector).get(rowId);
      case SMALLINT:
        return ((SmallIntVector) vector).get(rowId);
      case INTEGER:
        return ((IntVector) vector).get(rowId);
      case DATE:
        return ((DateDayVector) vector).get(rowId);
      case TIME_WITHOUT_TIME_ZONE:
        return ((TimeMilliVector) vector).get(rowId);
      case BIGINT:
        return ((BigIntVector) vector).get(rowId);
      case FLOAT:
        return ((Float4Vector) vector).get(rowId);
      case DOUBLE:
        return ((Float8Vector) vector).get(rowId);
      case CHAR:
      case VARCHAR:
        return StringData.fromBytes(((VarCharVector) vector).get(rowId));
      case BINARY:
      case VARBINARY:
        return ((VarBinaryVector) vector).get(rowId);
      case DECIMAL:
        DecimalType decimalType = (DecimalType) type;
        BigDecimal decimal = ((DecimalVector) vector).getObject(rowId);
        return DecimalData.fromBigDecimal(decimal, decimalType.getPrecision(), decimalType.getScale());
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        long micros = ((TimeStampMicroVector) vector).get(rowId);
        return TimestampData.fromEpochMillis(Math.floorDiv(micros, 1000L), (int) Math.floorMod(micros, 1000L) * 1000);
      default:
        throw unsupported(type);
    }
  }

  private static Field toArrowField(String name, LogicalType type) {
    return new Field(name, FieldType.nullable(toArrowType(type)), Collections.emptyList());
  }

  private static ArrowType toArrowType(LogicalType type) {
    switch (type.getTypeRoot()) {
      case BOOLEAN:
        return ArrowType.Bool.INSTANCE;
      case TINYINT:
        return new ArrowType.Int(8, true);
      case SMALLINT:
        return new ArrowType.Int(16, true);
      case INTEGER:
        return new ArrowType.Int(32, true);
      case BIGINT:
        return new ArrowType.Int(64, true);
      case FLOAT:
        return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
      case DOUBLE:
        return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
      case CHAR:
      case VARCHAR:
        return ArrowType.Utf8.INSTANCE;
      case BINARY:
      case VARBINARY:
        return ArrowType.Binary.INSTANCE;
      case DATE:
        return new ArrowType.Date(DateUnit.DAY);
      case TIME_WITHOUT_TIME_ZONE:
        return new ArrowType.Time(TimeUnit.MILLISECOND, 32);
      case DECIMAL:
        DecimalType decimalType = (DecimalType) type;
        return new ArrowType.Decimal(decimalType.getPrecision(), decimalType.getScale(), 128);
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        return new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC");
      default:
        throw unsupported(type);
    }
  }

  private static LogicalType toLogicalType(ArrowType arrowType) {
    if (arrowType instanceof ArrowType.Bool) {
      return new BooleanType();
    } else if (arrowType instanceof ArrowType.Int) {
      ArrowType.Int intType = (ArrowType.Int) arrowType;
      switch (intType.getBitWidth()) {
        case 8:
          return new TinyIntType();
        case 16:
          return new SmallIntType();
        case 32:
          return new IntType();
        case 64:
          return new BigIntType();
        default:
          throw new HoodieNotSupportedException("Unsupported Arrow int width for Lance Flink reader: " + intType.getBitWidth());
      }
    } else if (arrowType instanceof ArrowType.FloatingPoint) {
      ArrowType.FloatingPoint fp = (ArrowType.FloatingPoint) arrowType;
      return fp.getPrecision() == FloatingPointPrecision.SINGLE
          ? new FloatType()
          : new DoubleType();
    } else if (arrowType instanceof ArrowType.Utf8) {
      return new VarCharType();
    } else if (arrowType instanceof ArrowType.Binary) {
      return new VarBinaryType();
    } else if (arrowType instanceof ArrowType.Date) {
      return new DateType();
    } else if (arrowType instanceof ArrowType.Time) {
      return new TimeType();
    } else if (arrowType instanceof ArrowType.Decimal) {
      ArrowType.Decimal decimal = (ArrowType.Decimal) arrowType;
      return new DecimalType(decimal.getPrecision(), decimal.getScale());
    } else if (arrowType instanceof ArrowType.Timestamp) {
      ArrowType.Timestamp timestamp = (ArrowType.Timestamp) arrowType;
      return timestamp.getTimezone() == null
          ? new TimestampType(6)
          : new LocalZonedTimestampType(6);
    }
    throw new HoodieNotSupportedException("Unsupported Arrow type for Lance Flink reader: " + arrowType);
  }

  private static long timestampToMicros(TimestampData timestampData, int precision, boolean utcTimestamp) {
    long millis = utcTimestamp ? timestampData.getMillisecond() : timestampData.toTimestamp().getTime();
    return precision > 3 && utcTimestamp
        ? millis * 1000L + timestampData.getNanoOfMillisecond() / 1000L
        : millis * 1000L;
  }

  private static HoodieNotSupportedException unsupported(LogicalType type) {
    return new HoodieNotSupportedException("Flink Lance base-file support currently supports primitive append-only columns; unsupported type: " + type);
  }
}
