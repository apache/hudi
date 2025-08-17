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
import org.apache.hudi.common.util.DateTimeUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.metadata.HoodieIndexVersion;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeTokenParser;
import org.apache.parquet.schema.PrimitiveType;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.Objects;
import java.util.UUID;

import static org.apache.hudi.avro.AvroSchemaUtils.resolveNullableSchema;
import static org.apache.hudi.avro.HoodieAvroUtils.convertBytesToBigDecimal;
import static org.apache.hudi.avro.HoodieAvroUtils.unwrapAvroValueWrapper;
import static org.apache.hudi.common.util.DateTimeUtils.instantToMicros;
import static org.apache.hudi.common.util.DateTimeUtils.instantToNanos;
import static org.apache.hudi.common.util.DateTimeUtils.microsToInstant;
import static org.apache.hudi.common.util.DateTimeUtils.nanosToInstant;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.apache.hudi.metadata.HoodieMetadataPayload.COLUMN_STATS_FIELD_VALUE_TYPE;
import static org.apache.hudi.metadata.HoodieMetadataPayload.COLUMN_STATS_FIELD_VALUE_TYPE_ADDITIONAL_INFO;
import static org.apache.hudi.metadata.HoodieMetadataPayload.COLUMN_STATS_FIELD_VALUE_TYPE_ORDINAL;

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

  private static <T extends Comparable<T>> void validateTypes(HoodieIndexVersion indexVersion, ValueMetadata valueMetadata, T minValue, T maxValue) {
    if (indexVersion.lowerThan(HoodieIndexVersion.V2)) {
      if (valueMetadata.getValueType() != ValueType.NONE) {
        throw new IllegalArgumentException("Value type should be NONE");
      }
    } else {
      if (valueMetadata.getValueType() == ValueType.NONE) {
        throw new IllegalArgumentException("Value type should not be NONE");
      }
      switch (valueMetadata.getValueType()) {
        case BOOLEAN:
          validateMinMaxTypes(minValue, maxValue, Boolean.class);
          break;
        case INT:
          validateMinMaxTypes(minValue, maxValue, Integer.class);
          break;
        case LONG:
          validateMinMaxTypes(minValue, maxValue, Long.class);
          break;
        case FLOAT:
          validateMinMaxTypes(minValue, maxValue, Float.class);
          break;
        case DOUBLE:
          validateMinMaxTypes(minValue, maxValue, Double.class);
          break;
        case STRING:
          validateMinMaxTypes(minValue, maxValue, new Class<?>[]{String.class, Utf8.class}, "String or Utf8");
          break;
        case BYTES:
          validateMinMaxTypes(minValue, maxValue, ByteBuffer.class);
          break;
        case FIXED:
          validateMinMaxTypes(minValue, maxValue, GenericData.Fixed.class);
          break;
        case DECIMAL:
          validateMinMaxTypes(minValue, maxValue, BigDecimal.class);
          break;
        case TIMESTAMP_MILLIS:
        case TIMESTAMP_MICROS:
          validateMinMaxTypes(minValue, maxValue, Instant.class);
          break;
        case LOCAL_TIMESTAMP_MILLIS:
        case LOCAL_TIMESTAMP_MICROS:
          validateMinMaxTypes(minValue, maxValue, LocalDateTime.class);
          break;
        case DATE:
          validateMinMaxTypes(minValue, maxValue, Date.class);
          break;

        default:
          throw new IllegalArgumentException("Unsupported value type: " + valueMetadata.getValueType());
      }
    }
  }

  // Helper method to validate both min and max values
  private static void validateMinMaxTypes(Object minValue, Object maxValue, Class<?> expectedType) throws IllegalArgumentException {
    validateValueType(minValue, expectedType, "Min value");
    validateValueType(maxValue, expectedType, "Max value");
  }

  // Helper method to validate value types
  private static void validateValueType(Object value, Class<?> expectedType, String valueName) throws IllegalArgumentException {
    if (value != null && !expectedType.isInstance(value)) {
      throw new IllegalArgumentException(String.format(
          "%s should be %s, but got %s",
          valueName,
          expectedType.getSimpleName(),
          value.getClass().getSimpleName()
      ));
    }
  }

  // Helper method to validate both min and max values with multiple allowed types
  private static void validateMinMaxTypes(Object minValue, Object maxValue, Class<?>[] allowedTypes, String typeDescription) throws IllegalArgumentException {
    validateValueType(minValue, allowedTypes, "Min value", typeDescription);
    validateValueType(maxValue, allowedTypes, "Max value", typeDescription);
  }

  // Helper method to validate value types with multiple allowed types
  private static void validateValueType(Object value, Class<?>[] allowedTypes, String valueName, String typeDescription) throws IllegalArgumentException {
    if (value != null) {
      for (Class<?> allowedType : allowedTypes) {
        if (allowedType.isInstance(value)) {
          return; // Valid type found
        }
      }
      throw new IllegalArgumentException(String.format(
          "%s should be %s, but got %s",
          valueName,
          typeDescription,
          value.getClass().getSimpleName()
      ));
    }
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
                                                                              ValueMetadata valueMetadata,
                                                                              HoodieIndexVersion indexVersion) throws IllegalArgumentException {
    validateTypes(indexVersion, valueMetadata, minValue, maxValue);
    return new HoodieColumnRangeMetadata<>(filePath, columnName, minValue, maxValue, nullCount, valueCount, totalSize, totalUncompressedSize, valueMetadata);
  }

  /**
   * Converts instance of {@link HoodieMetadataColumnStats} to {@link HoodieColumnRangeMetadata}
   */
  public static HoodieColumnRangeMetadata<Comparable> fromColumnStats(HoodieMetadataColumnStats columnStats, HoodieIndexVersion indexVersion) {
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
        valueMetadata,
        indexVersion);
  }

  @SuppressWarnings("rawtype")
  public static HoodieColumnRangeMetadata<Comparable> stub(String filePath,
                                                           String columnName,
                                                           HoodieIndexVersion indexVersion) {
    ValueMetadata valueMetadata = indexVersion.greaterThanOrEquals(HoodieIndexVersion.V1) ? NULL_METADATA : NoneMetadata.INSTANCE;
    return new HoodieColumnRangeMetadata<>(filePath, columnName, null, null, -1, -1, -1, -1, valueMetadata);
  }

  public static HoodieColumnRangeMetadata<Comparable> createEmpty(String filePath,
                                                                  String columnName,
                                                                  HoodieIndexVersion indexVersion) {
    ValueMetadata valueMetadata = indexVersion.greaterThanOrEquals(HoodieIndexVersion.V1) ? NULL_METADATA : NoneMetadata.INSTANCE;
    return new HoodieColumnRangeMetadata(filePath, columnName, null, null, 0L, 0L, 0L, 0L, valueMetadata);
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

    if (left.getValueMetadata().getValueType() != right.getValueMetadata().getValueType()) {
      throw new IllegalArgumentException("Value types should be the same for merging column ranges");
    } else if (left.minValue != null && right.minValue != null && left.minValue.getClass() != right.minValue.getClass()) {
      throw new IllegalArgumentException("Value types should be the same for merging column ranges");
    } else if (left.maxValue != null && right.maxValue != null && left.maxValue.getClass() != right.maxValue.getClass()) {
      throw new IllegalArgumentException("Value types should be the same for merging column ranges");
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

    return new HoodieColumnRangeMetadata<>(filePath, columnName, min, max, nullCount, valueCount, totalSize, totalUncompressedSize, left.getValueMetadata());
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

  public static ValueMetadata getValueMetadata(GenericRecord columnStatsRecord) {
    if (columnStatsRecord == null) {
      return NoneMetadata.INSTANCE;
    }

    GenericRecord valueTypeInfo = (GenericRecord) columnStatsRecord.get(COLUMN_STATS_FIELD_VALUE_TYPE);
    if (valueTypeInfo == null) {
      return NoneMetadata.INSTANCE;
    }

    ValueType valueType = ValueType.fromInt((Integer) valueTypeInfo.get(COLUMN_STATS_FIELD_VALUE_TYPE_ORDINAL));
    if (valueType == ValueType.NONE) {
      return NoneMetadata.INSTANCE;
    } else if (valueType == ValueType.DECIMAL) {
      return DecimalMetadata.create((String) valueTypeInfo.get(COLUMN_STATS_FIELD_VALUE_TYPE_ADDITIONAL_INFO));
    } else {
      return new ValueMetadata(valueType);
    }
  }

  public static ValueMetadata getValueMetadata(Schema fieldSchema, HoodieIndexVersion indexVersion) {
    if (indexVersion.lowerThan(HoodieIndexVersion.V2)) {
      return NoneMetadata.INSTANCE;
    }
    if (fieldSchema == null) {
      return NULL_METADATA;
    }
    Schema valueSchema = resolveNullableSchema(fieldSchema);
    ValueType valueType = ValueType.fromSchema(valueSchema);
    if (valueType == ValueType.NONE) {
      return NoneMetadata.INSTANCE;
    } else if (valueType == ValueType.DECIMAL) {
      return DecimalMetadata.create((LogicalTypes.Decimal) valueSchema.getLogicalType());
    } else {
      return new ValueMetadata(valueType);
    }
  }

  public static ValueMetadata getValueMetadata(PrimitiveType primitiveType, HoodieIndexVersion indexVersion) {
    if (indexVersion.lowerThan(HoodieIndexVersion.V2)) {
      return NoneMetadata.INSTANCE;
    }
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

    public String getAdditionalInfo() {
      return null;
    }

    //TODO: Put the cases into the enums as lambda args

    public Comparable<?> standardizeJavaTypeAndPromote(Object val) {
      if (val == null) {
        return null;
      }

      switch (getValueType()) {
        case BOOLEAN:
          if (val instanceof Boolean) {
            return (Comparable<?>) val;
          } else {
            throw new UnsupportedOperationException("Unable to convert boolean: " + val.getClass());
          }

        case INT:
          return castToInteger(val);
        case LONG:
          return castToLong(val);
        case FLOAT:
          return castToFloat(val);
        case DOUBLE:
          return castToDouble(val);

        case STRING:
          if (val instanceof String) {
            return (Comparable<?>) val;
          } else if (val instanceof Utf8) {
            return val.toString();
          } else if (val instanceof Binary) {
            return ((Binary) val).toStringUsingUTF8();
          } else {
            throw new UnsupportedOperationException("Unable to convert string: " + val.getClass());
          }

        case FIXED:
        case BYTES:
          if (val instanceof ByteBuffer) {
            return (Comparable<?>) val;
          } else if (val instanceof GenericData.Fixed) {
            return ByteBuffer.wrap(((GenericData.Fixed) val).bytes());
          } else if (val instanceof byte[]) {
            return ByteBuffer.wrap((byte[]) val);
          } else if (val instanceof Binary) {
            return ((Binary) val).toByteBuffer();
          } else if (val instanceof String) {
            return ByteBuffer.wrap(getUTF8Bytes(val.toString()));
          } else {
            throw new UnsupportedOperationException("Unable to convert fixed to ByteBuffer: " + val.getClass());
          }

        case DECIMAL:
          // todo allow widening
          HoodieColumnRangeMetadata.DecimalMetadata decimalMetadata = (HoodieColumnRangeMetadata.DecimalMetadata) this;
          int precision = decimalMetadata.getPrecision();
          int scale = decimalMetadata.getScale();
          if (val instanceof BigDecimal) {
            return (Comparable<?>) val;
          } else if (val instanceof GenericData.Fixed) {
            return convertBytesToBigDecimal(((GenericData.Fixed) val).bytes(), precision, scale);
          } else if (val instanceof ByteBuffer) {
            return convertBytesToBigDecimal(((ByteBuffer) val).array(), precision, scale);
          } else if (val instanceof byte[]) {
            return convertBytesToBigDecimal((byte[]) val, precision, scale);
          } else if (val instanceof Integer) {
            return BigDecimal.valueOf((Integer) val, scale).round(new MathContext(precision, RoundingMode.HALF_UP));
          } else if (val instanceof Long) {
            return BigDecimal.valueOf((Long) val, scale).round(new MathContext(precision, RoundingMode.HALF_UP));
          } else if (val instanceof Binary) {
            // NOTE: Unscaled number is stored in BE format (most significant byte is 0th)
            return new BigDecimal(new BigInteger(((Binary) val).getBytesUnsafe()), scale,
                new MathContext(precision, RoundingMode.HALF_UP));
          } else {
            throw new UnsupportedOperationException("Unable to convert decimal: " + val.getClass());
          }

        case UUID:
          if (val instanceof UUID) {
            return (Comparable<?>) val;
          } else if (val instanceof String) {
            return UUID.fromString((String) val);
          } else {
            throw new UnsupportedOperationException("Unable to convert UUID: " + val.getClass());
          }

        case DATE:
          if (val instanceof java.sql.Date) {
            return (Comparable<?>) val;
          } else if (val instanceof Integer) {
            return java.sql.Date.valueOf(LocalDate.ofEpochDay((Integer) val));
          } else {
            throw new UnsupportedOperationException("Unable to convert date: " + val.getClass());
          }

        case TIME_MILLIS:
          if (val instanceof LocalTime) {
            return (Comparable<?>) val;
          } else if (val instanceof Integer) {
            return LocalTime.ofNanoOfDay((Integer) val * 1_000_000L);
          } else {
            throw new UnsupportedOperationException("Unable to convert time millis: " + val.getClass());
          }

        case TIME_MICROS:
          if (val instanceof LocalTime) {
            return (Comparable<?>) val;
          } else if (val instanceof Long) {
            return LocalTime.ofNanoOfDay((Long) val * 1_000L);
          } else {
            throw new UnsupportedOperationException("Unable to convert time micros: " + val.getClass());
          }

        case TIMESTAMP_MILLIS:
          if (val instanceof Instant) {
            return (Comparable<?>) val;
          } else if (val instanceof Timestamp) {
            return ((Timestamp) val).toInstant();
          } else if (val instanceof Long) {
            return Instant.ofEpochMilli((Long) val);
          } else {
            throw new UnsupportedOperationException("Unable to convert timestamp millis: " + val.getClass());
          }

        case TIMESTAMP_MICROS:
          if (val instanceof Instant) {
            return (Comparable<?>) val;
          } else if (val instanceof Timestamp) {
            return ((Timestamp) val).toInstant();
          } else if (val instanceof Long) {
            return DateTimeUtils.microsToInstant((Long) val);
          } else {
            throw new UnsupportedOperationException("Unable to convert timestamp micros: " + val.getClass());
          }

        case LOCAL_TIMESTAMP_MILLIS:
          if (val instanceof LocalDateTime) {
            return (Comparable<?>) val;
          } else if (val instanceof Long) {
            return LocalDateTime.ofInstant(Instant.ofEpochMilli((Long) val), ZoneOffset.UTC);
          } else {
            throw new UnsupportedOperationException("Unable to convert local timestamp millis: " + val.getClass());
          }

        case LOCAL_TIMESTAMP_MICROS:
          if (val instanceof LocalDateTime) {
            return (Comparable<?>) val;
          } else if (val instanceof Long) {
            return LocalDateTime.ofInstant(microsToInstant((Long) val), ZoneOffset.UTC);
          } else {
            throw new UnsupportedOperationException("Unable to convert local timestamp micros: " + val.getClass());
          }
        default:
          throw new IllegalStateException("Unexpected type: " + getValueType());
      }
    }

    public Comparable<?> convertIntoPrimitive(Comparable<?> value) {
      switch (getValueType()) {
        case NONE:
        case NULL:
        case BYTES:
        case FIXED:
        case BOOLEAN:
        case STRING:
        case ARRAY:
          return value;
        case INT:
          return castToInteger(value);
        case LONG:
          return castToLong(value);
        case FLOAT:
          return castToFloat(value);
        case DOUBLE:
          return castToDouble(value);
        case DECIMAL:
          return ByteBuffer.wrap(((BigDecimal) value).unscaledValue().toByteArray());
        case UUID:
          return ((UUID) value).toString();
        case DATE:
          return ((Long) ((java.sql.Date) value).toLocalDate().toEpochDay()).intValue();
        case TIME_MILLIS:
          return ((LocalTime) value).toSecondOfDay() * 1000 + (((LocalTime) value).getNano() / 1_000_000);
        case TIME_MICROS:
          return ((LocalTime) value).toSecondOfDay() * 1000_000L + (((LocalTime) value).getNano() / 1_000);
        case TIMESTAMP_MILLIS:
          return ((Instant) value).toEpochMilli();
        case TIMESTAMP_MICROS:
          return instantToMicros((Instant) value);
        case TIMESTAMP_NANOS:
          return instantToNanos((Instant) value);
        case LOCAL_TIMESTAMP_MILLIS:
          return ((LocalDateTime) value).toInstant(ZoneOffset.UTC).toEpochMilli();
        case LOCAL_TIMESTAMP_MICROS:
          return instantToMicros(((LocalDateTime) value).toInstant(ZoneOffset.UTC));
        case LOCAL_TIMESTAMP_NANOS:
          return instantToNanos(((LocalDateTime) value).toInstant(ZoneOffset.UTC));
        default:
          throw new UnsupportedOperationException("Unsupported type of the value " + value.getClass());
      }
    }

    private static Integer castToInteger(Object val) {
      if (val == null) {
        return null;
      }
      if (val instanceof Integer) {
        return (Integer) val;
      } else if (val instanceof Long) {
        return ((Long) val).intValue();
      } else if (val instanceof Float) {
        return ((Float)val).intValue();
      } else if (val instanceof Double) {
        return ((Double)val).intValue();
      } else if (val instanceof Boolean) {
        return ((Boolean) val) ? 1 : 0;
      }  else {
        // best effort casting
        return Integer.parseInt(val.toString());
      }
    }

    private static Long castToLong(Object val) {
      if (val == null) {
        return null;
      }
      if (val instanceof Integer) {
        return ((Integer) val).longValue();
      } else if (val instanceof Long) {
        return ((Long) val);
      } else if (val instanceof Float) {
        return ((Float)val).longValue();
      } else if (val instanceof Double) {
        return ((Double)val).longValue();
      } else if (val instanceof Boolean) {
        return ((Boolean) val) ? 1L : 0L;
      }  else {
        // best effort casting
        return Long.parseLong(val.toString());
      }
    }

    private static Float castToFloat(Object val) {
      if (val == null) {
        return null;
      }
      if (val instanceof Integer) {
        return ((Integer) val).floatValue();
      } else if (val instanceof Long) {
        return ((Long) val).floatValue();
      } else if (val instanceof Float) {
        return (Float) val;
      } else if (val instanceof Double) {
        return ((Double)val).floatValue();
      } else if (val instanceof Boolean) {
        return (Boolean) val ? 1.0f : 0.0f;
      }  else {
        // best effort casting
        return Float.parseFloat(val.toString());
      }
    }

    private static Double castToDouble(Object val) {
      if (val == null) {
        return null;
      }
      if (val instanceof Integer) {
        return ((Integer) val).doubleValue();
      } else if (val instanceof Long) {
        return ((Long) val).doubleValue();
      } else if (val instanceof Float) {
        return Double.valueOf(val + "");
      } else if (val instanceof Double) {
        return (Double) val;
      } else if (val instanceof Boolean) {
        return (Boolean) val ? 1.0d : 0.0d;
      }  else {
        // best effort casting
        return Double.parseDouble(val.toString());
      }
    }

    public Comparable<?> convertIntoComplex(Comparable<?> value) {
      switch (getValueType()) {
        case NONE:
        case NULL:
        case BYTES:
        case FIXED:
        case BOOLEAN:
        case STRING:
        case ARRAY:
          return value;
        case INT:
          return castToInteger(value);
        case LONG:
          return castToLong(value);
        case FLOAT:
          return castToFloat(value);
        case DOUBLE:
          return castToDouble(value);
        case DECIMAL:
          HoodieColumnRangeMetadata.DecimalMetadata decimalMetadata = (HoodieColumnRangeMetadata.DecimalMetadata) this;
          return convertBytesToBigDecimal(((ByteBuffer) value).array(), decimalMetadata.getPrecision(), decimalMetadata.getScale());
        case UUID:
          return UUID.fromString((String) value);
        case DATE:
          return java.sql.Date.valueOf(LocalDate.ofEpochDay(castToInteger(value)));
        case TIME_MILLIS:
          return LocalTime.ofNanoOfDay(castToInteger(value) * 1_000_000L);
        case TIME_MICROS:
          return LocalTime.ofNanoOfDay(castToLong(value) * 1000);
        case TIMESTAMP_MILLIS:
          return Instant.ofEpochMilli(castToLong(value));
        case TIMESTAMP_MICROS:
          return microsToInstant(castToLong(value));
        case TIMESTAMP_NANOS:
          return nanosToInstant(castToLong(value));
        case LOCAL_TIMESTAMP_MILLIS:
          return LocalDateTime.ofInstant(Instant.ofEpochMilli(castToLong(value)), ZoneOffset.UTC);
        case LOCAL_TIMESTAMP_MICROS:
          return LocalDateTime.ofInstant(microsToInstant(castToLong(value)), ZoneOffset.UTC);
        case LOCAL_TIMESTAMP_NANOS:
          return LocalDateTime.ofInstant(nanosToInstant(castToLong(value)), ZoneOffset.UTC);
        default:
          throw new UnsupportedOperationException("Unsupported type of the value " + value.getClass());
      }
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

  public static final ValueMetadata NULL_METADATA = new ValueMetadata(ValueType.NULL);

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
    public String getAdditionalInfo() {
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
        case FIXED:
          if (schema.getLogicalType() == null) {
            return ValueType.FIXED;
          } else if (schema.getLogicalType() instanceof LogicalTypes.Decimal) {
            return ValueType.DECIMAL;
          }
          throw new IllegalArgumentException("Unsupported logical type for Fixed: " + schema.getLogicalType());
        default:
          // TODO: decide if we want to throw or return NONE
          throw new IllegalArgumentException("Unsupported type: " + schema.getType());
          //return ValueType.NONE;
      }
    }
  }
}
