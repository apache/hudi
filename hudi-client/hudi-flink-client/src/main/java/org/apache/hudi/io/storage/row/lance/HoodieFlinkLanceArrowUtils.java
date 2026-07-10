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

import org.apache.hudi.common.util.ValidationUtils;
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
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
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
import java.util.List;

/**
 * RowData/Arrow conversion helpers for Flink Lance base files.
 *
 * <p>Supports primitive types, {@code ROW}, and {@code ARRAY}. Currently, {@code MAP} is not
 * supported in the current Lance version. Enable it once the lance version is upgraded to support
 * {@code MAP}.
 *
 * @see <a href="https://github.com/lance-format/lance/issues/3620">Lance issue #3620</a>
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
      fields.add(new RowType.RowField(field.getName(), toLogicalType(field, field.getName())));
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

  private static Object readValue(LogicalType type, ValueVector vector, int rowId) {
    if (vector.isNull(rowId)) {
      return null;
    }
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
        long micros = ((TimeStampVector) vector).get(rowId);
        return TimestampData.fromEpochMillis(Math.floorDiv(micros, 1000L), (int) Math.floorMod(micros, 1000L) * 1000);
      case ROW:
        return readRow((RowType) type, (StructVector) vector, rowId);
      case ARRAY:
        return readArray((ArrayType) type, (ListVector) vector, rowId);
      default:
        throw unsupported(type);
    }
  }

  private static Field toArrowField(String name, LogicalType type) {
    List<Field> children = new ArrayList<>();
    switch (type.getTypeRoot()) {
      case ROW:
        for (RowType.RowField field : ((RowType) type).getFields()) {
          children.add(toArrowField(field.getName(), field.getType()));
        }
        break;
      case ARRAY:
        children.add(toArrowField("element", ((ArrayType) type).getElementType()));
        break;
      default:
        break;
    }
    return new Field(name, new FieldType(type.isNullable(), toArrowType(type), null), children);
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
      case ROW:
        return ArrowType.Struct.INSTANCE;
      case ARRAY:
        return ArrowType.List.INSTANCE;
      default:
        throw unsupported(type);
    }
  }

  private static LogicalType toLogicalType(Field field, String path) {
    ArrowType arrowType = field.getType();
    LogicalType logicalType;
    if (arrowType instanceof ArrowType.Bool) {
      logicalType = new BooleanType();
    } else if (arrowType instanceof ArrowType.Int) {
      ArrowType.Int intType = (ArrowType.Int) arrowType;
      switch (intType.getBitWidth()) {
        case 8:
          logicalType = new TinyIntType();
          break;
        case 16:
          logicalType = new SmallIntType();
          break;
        case 32:
          logicalType = new IntType();
          break;
        case 64:
          logicalType = new BigIntType();
          break;
        default:
          throw new HoodieNotSupportedException("Unsupported Arrow int width for Lance Flink reader: " + intType.getBitWidth());
      }
    } else if (arrowType instanceof ArrowType.FloatingPoint) {
      ArrowType.FloatingPoint fp = (ArrowType.FloatingPoint) arrowType;
      logicalType = fp.getPrecision() == FloatingPointPrecision.SINGLE
          ? new FloatType()
          : new DoubleType();
    } else if (arrowType instanceof ArrowType.Utf8) {
      logicalType = new VarCharType();
    } else if (arrowType instanceof ArrowType.Binary) {
      logicalType = new VarBinaryType();
    } else if (arrowType instanceof ArrowType.Date) {
      logicalType = new DateType();
    } else if (arrowType instanceof ArrowType.Time) {
      logicalType = new TimeType();
    } else if (arrowType instanceof ArrowType.Decimal) {
      ArrowType.Decimal decimal = (ArrowType.Decimal) arrowType;
      logicalType = new DecimalType(decimal.getPrecision(), decimal.getScale());
    } else if (arrowType instanceof ArrowType.Timestamp) {
      ArrowType.Timestamp timestamp = (ArrowType.Timestamp) arrowType;
      logicalType = timestamp.getTimezone() == null
          ? new TimestampType(6)
          : new LocalZonedTimestampType(6);
    } else if (arrowType instanceof ArrowType.Struct) {
      List<RowType.RowField> fields = new ArrayList<>(field.getChildren().size());
      for (Field child : field.getChildren()) {
        fields.add(new RowType.RowField(child.getName(), toLogicalType(child, path + "." + child.getName())));
      }
      logicalType = new RowType(field.isNullable(), fields);
    } else if (arrowType instanceof ArrowType.List) {
      ValidationUtils.checkArgument(field.getChildren().size() == 1,
          String.format("Unsupported Arrow schema at '%s': LIST must contain exactly one child field", path));
      Field element = field.getChildren().get(0);
      logicalType = new ArrayType(field.isNullable(), toLogicalType(element, path + "[]"));
    } else {
      throw new HoodieNotSupportedException("Unsupported Arrow type for Lance Flink reader at '" + path + "': " + arrowType);
    }
    return logicalType.copy(field.isNullable());
  }

  private static RowData readRow(RowType rowType, StructVector vector, int rowId) {
    GenericRowData row = new GenericRowData(rowType.getFieldCount());
    for (int i = 0; i < rowType.getFieldCount(); i++) {
      row.setField(i, readValue(rowType.getTypeAt(i), vector.getChildByOrdinal(i), rowId));
    }
    return row;
  }

  private static ArrayData readArray(ArrayType arrayType, ListVector vector, int rowId) {
    int startIndex = vector.getElementStartIndex(rowId);
    int endIndex = vector.getElementEndIndex(rowId);
    Object[] values = new Object[endIndex - startIndex];
    FieldVector dataVector = vector.getDataVector();
    for (int i = 0; i < values.length; i++) {
      values[i] = readValue(arrayType.getElementType(), dataVector, startIndex + i);
    }
    return new GenericArrayData(values);
  }

  private static HoodieNotSupportedException unsupported(LogicalType type) {
    return new HoodieNotSupportedException("Flink Lance base-file support currently supports primitive, ROW, and ARRAY columns; unsupported type: " + type);
  }
}
