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

package org.apache.hudi.common.model;

import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.avro.model.HoodieValueTypeInfo;
import org.apache.hudi.common.util.ValidationUtils;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.parquet.schema.LogicalTypeTokenParser;
import org.apache.parquet.schema.PrimitiveType;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.hudi.avro.HoodieAvroUtils.unwrapAvroValueWrapper;

/**
 * Hoodie metadata for the column range of data stored in columnar format (like Parquet)
 *
 * NOTE: {@link Comparable} is used as raw-type so that we can handle polymorphism, where
 *        caller apriori is not aware of the type {@link HoodieColumnRangeMetadata} is
 *        associated with
 */
@SuppressWarnings("rawtype")
public class HoodieColumnRangeMetadata<T extends Comparable> implements Serializable {
  private final String filePath;
  private final String columnName;
  @Nullable
  private final T minValue;
  @Nullable
  private final T maxValue;
  private final long nullCount;
  private final long valueCount;
  private final long totalSize;
  private final long totalUncompressedSize;

  private final ValueMetadata valueMetadata;

  private HoodieColumnRangeMetadata(String filePath,
                                    String columnName,
                                    @Nullable T minValue,
                                    @Nullable T maxValue,
                                    long nullCount,
                                    long valueCount,
                                    long totalSize,
                                    long totalUncompressedSize,
                                    ValueMetadata valueMetadata) {
    this.filePath = filePath;
    this.columnName = columnName;
    this.minValue = minValue;
    this.maxValue = maxValue;
    this.nullCount = nullCount;
    this.valueCount = valueCount;
    this.totalSize = totalSize;
    this.totalUncompressedSize = totalUncompressedSize;
    this.valueMetadata = valueMetadata;
  }

  public String getFilePath() {
    return this.filePath;
  }

  public String getColumnName() {
    return this.columnName;
  }

  @Nullable
  public T getMinValue() {
    return this.minValue;
  }

  @Nullable
  public T getMaxValue() {
    return this.maxValue;
  }

  public long getNullCount() {
    return nullCount;
  }

  public long getValueCount() {
    return valueCount;
  }

  public long getTotalSize() {
    return totalSize;
  }

  public long getTotalUncompressedSize() {
    return totalUncompressedSize;
  }

  public ValueMetadata getValueMetadata() {
    return valueMetadata;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final HoodieColumnRangeMetadata<?> that = (HoodieColumnRangeMetadata<?>) o;
    return Objects.equals(getFilePath(), that.getFilePath())
        && Objects.equals(getColumnName(), that.getColumnName())
        && Objects.equals(getMinValue(), that.getMinValue())
        && Objects.equals(getMaxValue(), that.getMaxValue())
        && Objects.equals(getNullCount(), that.getNullCount())
        && Objects.equals(getValueCount(), that.getValueCount())
        && Objects.equals(getTotalSize(), that.getTotalSize())
        && Objects.equals(getTotalUncompressedSize(), that.getTotalUncompressedSize());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getColumnName(), getMinValue(), getMaxValue(), getNullCount());
  }

  @Override
  public String toString() {
    return "HoodieColumnRangeMetadata{"
        + "filePath ='" + filePath + '\''
        + ", columnName='" + columnName + '\''
        + ", minValue=" + minValue
        + ", maxValue=" + maxValue
        + ", nullCount=" + nullCount
        + ", valueCount=" + valueCount
        + ", totalSize=" + totalSize
        + ", totalUncompressedSize=" + totalUncompressedSize
        + '}';
  }

  public static <T extends Comparable<T>> HoodieColumnRangeMetadata<T> create(String filePath,
                                                                              String columnName,
                                                                              @Nullable T minValue,
                                                                              @Nullable T maxValue,
                                                                              long nullCount,
                                                                              long valueCount,
                                                                              long totalSize,
                                                                              long totalUncompressedSize,
                                                                              ValueMetadata valueMetadata) {
    return new HoodieColumnRangeMetadata<>(filePath, columnName, minValue, maxValue, nullCount, valueCount, totalSize, totalUncompressedSize, valueMetadata);
  }

  /**
   * Converts instance of {@link HoodieMetadataColumnStats} to {@link HoodieColumnRangeMetadata}
   */
  public static HoodieColumnRangeMetadata<Comparable> fromColumnStats(HoodieMetadataColumnStats columnStats) {
    ValueMetadata valueMetadata = getValueMetadata(columnStats.getValueType());
    return HoodieColumnRangeMetadata.<Comparable>create(
        columnStats.getFileName(),
        columnStats.getColumnName(),
        unwrapAvroValueWrapper(columnStats.getMinValue(), valueMetadata),
        unwrapAvroValueWrapper(columnStats.getMaxValue(), valueMetadata),
        columnStats.getNullCount(),
        columnStats.getValueCount(),
        columnStats.getTotalSize(),
        columnStats.getTotalUncompressedSize(),
        valueMetadata);
  }

  @SuppressWarnings("rawtype")
  public static HoodieColumnRangeMetadata<Comparable> stub(String filePath,
                                                           String columnName) {
    return new HoodieColumnRangeMetadata<>(filePath, columnName, null, null, -1, -1, -1, -1, NoneMetadata.INSTANCE);
  }

  /**
   * Merges the given two column range metadata.
   */
  public static <T extends Comparable<T>> HoodieColumnRangeMetadata<T> merge(
      HoodieColumnRangeMetadata<T> left,
      HoodieColumnRangeMetadata<T> right) {
    if (left == null || right == null) {
      return left == null ? right : left;
    }

    ValidationUtils.checkArgument(left.getColumnName().equals(right.getColumnName()),
        "Column names should be the same for merging column ranges");
    String filePath = left.getFilePath();
    String columnName = left.getColumnName();
    T min = minVal(left.getMinValue(), right.getMinValue());
    T max = maxVal(left.getMaxValue(), right.getMaxValue());
    long nullCount = left.getNullCount() + right.getNullCount();
    long valueCount = left.getValueCount() + right.getValueCount();
    long totalSize = left.getTotalSize() + right.getTotalSize();
    long totalUncompressedSize = left.getTotalUncompressedSize() + right.getTotalUncompressedSize();

    return create(filePath, columnName, min, max, nullCount, valueCount, totalSize, totalUncompressedSize, left.getValueMetadata());
  }

  private static <T extends Comparable<T>> T minVal(T val1, T val2) {
    if (val1 == null) {
      return val2;
    }
    if (val2 == null) {
      return val1;
    }
    return val1.compareTo(val2) < 0 ? val1 : val2;
  }

  private static <T extends Comparable<T>> T maxVal(T val1, T val2) {
    if (val1 == null) {
      return val2;
    }
    if (val2 == null) {
      return val1;
    }
    return val1.compareTo(val2) > 0 ? val1 : val2;
  }

  public static ValueMetadata getValueMetadata(HoodieValueTypeInfo valueTypeInfo) {
    if (valueTypeInfo == null) {
      return NoneMetadata.INSTANCE;
    }

    ValueType valueType = ValueType.fromInt(valueTypeInfo.getTypeOrdinal());
    if (valueType == ValueType.NONE) {
      return NoneMetadata.INSTANCE;
    } else if (valueType == ValueType.DECIMAL) {
      return DecimalMetadata.create(valueTypeInfo.getAdditionalInfo());
    } else {
      return new ValueMetadata(valueType);
    }
  }

  public static ValueMetadata getValueMetadata(Schema fieldSchema) {
    if (fieldSchema == null) {
      return NoneMetadata.INSTANCE;
    }

    ValueType valueType = ValueType.fromSchema(fieldSchema);
    if (valueType == ValueType.NONE) {
      return NoneMetadata.INSTANCE;
    } else if (valueType == ValueType.DECIMAL) {
      return DecimalMetadata.create((LogicalTypes.Decimal) fieldSchema.getLogicalType());
    } else {
      return new ValueMetadata(valueType);
    }
  }

  public static ValueMetadata getValueMetadata(PrimitiveType primitiveType) {
    if (primitiveType == null) {
      return NoneMetadata.INSTANCE;
    }

    ValueType valueType = ValueType.fromPrimitiveType(primitiveType);
    if (valueType == ValueType.NONE) {
      return NoneMetadata.INSTANCE;
    } else if (valueType == ValueType.DECIMAL) {
      return DecimalMetadata.create(primitiveType);
    } else {
      return new ValueMetadata(valueType);
    }
  }

  public static class ValueMetadata {


    private final ValueType valueType;

    private ValueMetadata(ValueType valueType) {
      this.valueType = valueType;
    }

    public ValueType getValueType() {
      return valueType;
    }

    public HoodieValueTypeInfo getValueTypeInfo() {
      return new HoodieValueTypeInfo(valueType.ordinal(), getAdditionalInfo());
    }

    protected String getAdditionalInfo() {
      return null;
    }
  }

  public static class NoneMetadata extends ValueMetadata {
    public static final ValueMetadata INSTANCE = new NoneMetadata();
    private NoneMetadata() {
      super(ValueType.NONE);
    }

    @Override
    public HoodieValueTypeInfo getValueTypeInfo() {
      return null;
    }
  }

  public static class DecimalMetadata extends ValueMetadata {

    public static DecimalMetadata create(String additionalInfo) {
      if (additionalInfo == null) {
        throw new IllegalArgumentException("additionalInfo cannot be null");
      }
      //TODO: decide if we want to store things in a better way
      String[] splits = additionalInfo.split(",");
      return new DecimalMetadata(Integer.parseInt(splits[0]), Integer.parseInt(splits[1]));
    }

    public static DecimalMetadata create(LogicalTypes.Decimal decimal) {
      return new DecimalMetadata(decimal.getPrecision(), decimal.getScale());
    }

    public static DecimalMetadata create(PrimitiveType primitiveType) {
      return new DecimalMetadata(LogicalTypeTokenParser.getPrecision(primitiveType), LogicalTypeTokenParser.getScale(primitiveType));
    }

    private final int precision;
    private final int scale;

    private DecimalMetadata(int precision, int scale) {
      super(ValueType.DECIMAL);
      this.precision = precision;
      this.scale = scale;
    }

    public int getPrecision() {
      return precision;
    }

    public int getScale() {
      return scale;
    }

    @Override
    protected String getAdditionalInfo() {
      return String.format("%d,%d", precision, scale);
    }
  }

  public enum ValueType {
    NONE,
    // primitive types
    NULL,
    BOOLEAN,
    INT,
    LONG,
    FLOAT,
    DOUBLE,
    BYTES,
    STRING,
    // complex types
    RECORD,
    ENUM,
    ARRAY,
    MAP,
    UNION, // maybe get rid of this
    FIXED,
    // logical types
    DECIMAL,
    UUID,
    DATE,
    TIME_MILLIS,
    TIME_MICROS,
    TIMESTAMP_MILLIS,
    TIMESTAMP_MICROS,
    TIMESTAMP_NANOS,
    LOCAL_TIMESTAMP_MILLIS,
    LOCAL_TIMESTAMP_MICROS,
    LOCAL_TIMESTAMP_NANOS,
    DURATION;

    private static ValueType[] myEnumValues;

    public static ValueType fromInt(int i) {
      if (ValueType.myEnumValues == null) {
        ValueType.myEnumValues = ValueType.values();
      }
      return ValueType.myEnumValues[i];
    }

    public static ValueType fromPrimitiveType(PrimitiveType primitiveType) {
      if (primitiveType.getLogicalTypeAnnotation() != null) {
        return LogicalTypeTokenParser.fromLogicalTypeAnnotation(primitiveType);
      }
      switch (primitiveType.getPrimitiveTypeName()) {
        case INT64:
          return ValueType.LONG;
        case INT32:
          return ValueType.INT;
        case BOOLEAN:
          return ValueType.BOOLEAN;
        case BINARY:
          return ValueType.BYTES;
        case FLOAT:
          return ValueType.FLOAT;
        case DOUBLE:
          return ValueType.DOUBLE;
        case INT96:
          // TODO: probably wrong
          return ValueType.DECIMAL;
        case FIXED_LEN_BYTE_ARRAY:
          return ValueType.FIXED;
        default:
          throw new IllegalArgumentException("Unsupported primitive type: " + primitiveType.getPrimitiveTypeName());
      }
    }

    public static ValueType fromSchema(Schema schema) {
      switch (schema.getType()) {
        case NULL:
          if (schema.getLogicalType() == null) {
            return ValueType.NULL;
          }
          throw new IllegalArgumentException("Unsupported logical type for Null: " + schema.getLogicalType());
        case BOOLEAN:
          if (schema.getLogicalType() == null) {
            return ValueType.BOOLEAN;
          }
          throw new IllegalArgumentException("Unsupported logical type for Boolean: " + schema.getLogicalType());
        case INT:
          if (schema.getLogicalType() == null) {
            return ValueType.INT;
          } else if (schema.getLogicalType() instanceof LogicalTypes.Date) {
            return ValueType.DATE;
          } else if (schema.getLogicalType() instanceof LogicalTypes.TimeMillis) {
            return ValueType.TIME_MILLIS;
          }
          throw new IllegalArgumentException("Unsupported logical type for Int: " + schema.getLogicalType());
        case LONG:
          if (schema.getLogicalType() == null) {
            return ValueType.LONG;
          } else if (schema.getLogicalType() instanceof LogicalTypes.TimeMicros) {
            return ValueType.TIME_MICROS;
          } else if (schema.getLogicalType() instanceof LogicalTypes.TimestampMillis) {
            return ValueType.TIMESTAMP_MILLIS;
          } else if (schema.getLogicalType() instanceof LogicalTypes.TimestampMicros) {
            return ValueType.TIMESTAMP_MICROS;
          } else if (schema.getLogicalType() instanceof LogicalTypes.LocalTimestampMillis) {
            return ValueType.LOCAL_TIMESTAMP_MILLIS;
          } else if (schema.getLogicalType() instanceof LogicalTypes.LocalTimestampMicros) {
            return ValueType.LOCAL_TIMESTAMP_MICROS;
          }
          throw new IllegalArgumentException("Unsupported logical type for Long: " + schema.getLogicalType());
        case FLOAT:
          if (schema.getLogicalType() == null) {
            return ValueType.FLOAT;
          }
          throw new IllegalArgumentException("Unsupported logical type for Float: " + schema.getLogicalType());
        case DOUBLE:
          if (schema.getLogicalType() == null) {
            return ValueType.DOUBLE;
          }
          throw new IllegalArgumentException("Unsupported logical type for Double: " + schema.getLogicalType());
        case BYTES:
          if (schema.getLogicalType() == null) {
            return ValueType.BYTES;
          } else if (schema.getLogicalType() instanceof LogicalTypes.Decimal) {
            return ValueType.DECIMAL;
          }
          throw new IllegalArgumentException("Unsupported logical type for Bytes: " + schema.getLogicalType());
        case STRING:
          if (schema.getLogicalType() == null) {
            return ValueType.STRING;
          } else if (Objects.equals(schema.getLogicalType().getName(), LogicalTypes.uuid().getName())) {
            return ValueType.UUID;
          }
          throw new IllegalArgumentException("Unsupported logical type for String: " + schema.getLogicalType());
        default:
          return ValueType.NONE;
      }
    }
  }
}
