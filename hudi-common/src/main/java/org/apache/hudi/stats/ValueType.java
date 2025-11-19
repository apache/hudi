/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.stats;

import org.apache.hudi.ParquetAdapter;
import org.apache.hudi.avro.HoodieAvroWrapperUtils;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.util.DateTimeUtils;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;

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
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.apache.hudi.avro.HoodieAvroUtils.convertBytesToBigDecimal;
import static org.apache.hudi.common.util.DateTimeUtils.instantToMicros;
import static org.apache.hudi.common.util.DateTimeUtils.instantToNanos;
import static org.apache.hudi.common.util.DateTimeUtils.microsToInstant;
import static org.apache.hudi.common.util.DateTimeUtils.nanosToInstant;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;

public enum ValueType {
  V1(HoodieAvroWrapperUtils.PrimitiveWrapperType.V1.getClazz(), HoodieAvroWrapperUtils.PrimitiveWrapperType.V1,
      ValueType::passThrough, ValueType::passThrough, ValueType::passThrough),
  NULL(HoodieAvroWrapperUtils.PrimitiveWrapperType.NULL.getClazz(), HoodieAvroWrapperUtils.PrimitiveWrapperType.NULL,
      ValueType::passThrough, ValueType::passThrough, ValueType::passThrough),
  BOOLEAN(HoodieAvroWrapperUtils.PrimitiveWrapperType.BOOLEAN, ValueType::castToBoolean),
  INT(HoodieAvroWrapperUtils.PrimitiveWrapperType.INT, ValueType::castToInteger),
  LONG(HoodieAvroWrapperUtils.PrimitiveWrapperType.LONG, ValueType::castToLong),
  FLOAT(HoodieAvroWrapperUtils.PrimitiveWrapperType.FLOAT, ValueType::castToFloat),
  DOUBLE(HoodieAvroWrapperUtils.PrimitiveWrapperType.DOUBLE, ValueType::castToDouble),
  STRING(HoodieAvroWrapperUtils.PrimitiveWrapperType.STRING, ValueType::castToString),
  BYTES(HoodieAvroWrapperUtils.PrimitiveWrapperType.BYTES, ValueType::castToBytes),
  FIXED(HoodieAvroWrapperUtils.PrimitiveWrapperType.BYTES, ValueType::castToFixed),
  DECIMAL(BigDecimal.class, HoodieAvroWrapperUtils.PrimitiveWrapperType.BYTES,
      ValueType::castToDecimal, ValueType::toDecimal, ValueType::fromDecimal),
  UUID(UUID.class, HoodieAvroWrapperUtils.PrimitiveWrapperType.STRING,
      ValueType::castToUUID, ValueType::toUUID, ValueType::fromUUID),
  DATE(LocalDate.class, HoodieAvroWrapperUtils.PrimitiveWrapperType.INT,
      ValueType::castToDate, ValueType::toDate, ValueType::fromDate),
  TIME_MILLIS(LocalTime.class, HoodieAvroWrapperUtils.PrimitiveWrapperType.INT,
      ValueType::castToTimeMillis, ValueType::toTimeMillis, ValueType::fromTimeMillis),
  TIME_MICROS(LocalTime.class, HoodieAvroWrapperUtils.PrimitiveWrapperType.LONG,
      ValueType::castToTimeMicros, ValueType::toTimeMicros, ValueType::fromTimeMicros),
  TIMESTAMP_MILLIS(Instant.class, HoodieAvroWrapperUtils.PrimitiveWrapperType.LONG,
      ValueType::castToTimestampMillis, ValueType::toTimestampMillis, ValueType::fromTimestampMillis),
  TIMESTAMP_MICROS(Instant.class, HoodieAvroWrapperUtils.PrimitiveWrapperType.LONG,
      ValueType::castToTimestampMicros, ValueType::toTimestampMicros, ValueType::fromTimestampMicros),
  TIMESTAMP_NANOS(Instant.class, HoodieAvroWrapperUtils.PrimitiveWrapperType.LONG,
      ValueType::castToTimestampNanos, ValueType::toTimestampNanos, ValueType::fromTimestampNanos),
  LOCAL_TIMESTAMP_MILLIS(LocalDateTime.class, HoodieAvroWrapperUtils.PrimitiveWrapperType.LONG,
      ValueType::castToLocalTimestampMillis, ValueType::toLocalTimestampMillis, ValueType::fromLocalTimestampMillis),
  LOCAL_TIMESTAMP_MICROS(LocalDateTime.class, HoodieAvroWrapperUtils.PrimitiveWrapperType.LONG,
      ValueType::castToLocalTimestampMicros, ValueType::toLocalTimestampMicros, ValueType::fromLocalTimestampMicros),
  LOCAL_TIMESTAMP_NANOS(LocalDateTime.class, HoodieAvroWrapperUtils.PrimitiveWrapperType.LONG,
      ValueType::castToLocalTimestampNanos, ValueType::toLocalTimestampNanos, ValueType::fromLocalTimestampNanos);

  // java type to represent this data type while in memory
  private final Class<?> internalType;

  // primitive wrapper type to represent this data type when we write to the MDT
  private final HoodieAvroWrapperUtils.PrimitiveWrapperType primitiveWrapperType;

  // converts to the internal type if it is something else, and will also type promote
  // if the column has been promoted since it was written
  private final BiFunction<Object, ValueMetadata, Comparable<?>> standardize;

  // converts primitive value read from the mdt to the internalType java class
  private final BiFunction<Comparable<?>, ValueMetadata, Comparable<?>> toComposite;

  // converts value from internalType java class to primitive type for writing to MDT
  private final BiFunction<Comparable<?>, ValueMetadata, Comparable<?>> toPrimitive;

  ValueType(HoodieAvroWrapperUtils.PrimitiveWrapperType primitiveWrapperType, Function<Object, Object> single) {
    this(primitiveWrapperType.getClazz(),
        primitiveWrapperType,
        (val, meta) -> (Comparable<?>) single.apply(val),
        ValueType::passThrough,
        ValueType::passThrough);
  }

  ValueType(Class<?> internalType,
            HoodieAvroWrapperUtils.PrimitiveWrapperType primitiveWrapperType,
            BiFunction<Object, ValueMetadata, Comparable<?>> standardize,
            BiFunction<Comparable<?>, ValueMetadata, Comparable<?>> toComposite,
            BiFunction<Comparable<?>, ValueMetadata, Comparable<?>> toPrimitive) {
    this.internalType = internalType;
    this.primitiveWrapperType = primitiveWrapperType;
    this.standardize = standardize;
    this.toComposite = toComposite;
    this.toPrimitive = toPrimitive;
  }

  Comparable<?> standardizeJavaTypeAndPromote(Object val, ValueMetadata meta) {
    if (val == null) {
      return null;
    }
    return standardize.apply(val, meta);
  }

  private Comparable<?> convertIntoPrimitive(Comparable<?> val, ValueMetadata meta) {
    if (val == null) {
      return null;
    }
    return toPrimitive.apply(val, meta);
  }

  private Comparable<?> convertIntoComplex(Comparable<?> val, ValueMetadata meta) {
    if (val == null) {
      return null;
    }
    return toComposite.apply(val, meta);
  }

  void validate(Object val) {
    if (val == null) {
      return;
    }

    if (!internalType.isInstance(val)) {
      throw new IllegalArgumentException(String.format(
          "should be %s, but got %s",
          internalType.getSimpleName(),
          val.getClass().getSimpleName()
      ));
    }
  }

  public Object wrapValue(Comparable<?> val, ValueMetadata meta) {
    if (meta.getValueType() == V1) {
      return primitiveWrapperType.wrap(val);
    }

    if (val == null) {
      return null;
    }
    if (!this.internalType.isInstance(val)) {
      throw new IllegalArgumentException(String.format(
          "should be %s, but got %s",
          this.internalType.getSimpleName(),
          val.getClass().getSimpleName()
      ));
    }
    return primitiveWrapperType.wrap(convertIntoPrimitive(val, meta));
  }

  public Comparable<?> unwrapValue(Object val, ValueMetadata meta) {
    if (meta.getValueType() == V1) {
      return primitiveWrapperType.unwrap(val);
    }

    if (val == null) {
      return null;
    }

    if (!primitiveWrapperType.getWrapperClass().isInstance(val)) {
      if (!(val instanceof GenericRecord)) {
        throw new IllegalArgumentException(String.format(
            "should be %s, but got %s",
            primitiveWrapperType.getWrapperClass().getSimpleName(),
            val.getClass().getSimpleName()
        ));
      } else if (((GenericRecord) val).getSchema().getField("value") != null) {
        return standardizeJavaTypeAndPromote(HoodieAvroWrapperUtils.unwrapGenericRecord(val), meta);
      } else {
        throw new IllegalArgumentException(String.format(
            "should be %s, but got %s", 
            primitiveWrapperType.getWrapperClass().getSimpleName(),
            val.getClass().getSimpleName()
        ));
      }
    }
    return convertIntoComplex(primitiveWrapperType.unwrap(val), meta);
  }

  private static ValueType[] myEnumValues;

  public static ValueType fromOrdinal(int i) {
    if (ValueType.myEnumValues == null) {
      ValueType.myEnumValues = ValueType.values();
    }
    return ValueType.myEnumValues[i];
  }

  private static final ParquetAdapter PARQUET_ADAPTER = ParquetAdapter.getAdapter();

  public static ValueType fromParquetPrimitiveType(PrimitiveType primitiveType) {
    if (PARQUET_ADAPTER.hasAnnotation(primitiveType)) {
      return PARQUET_ADAPTER.getValueTypeFromAnnotation(primitiveType);
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
      case FIXED_LEN_BYTE_ARRAY:
        return ValueType.FIXED;
      default:
        throw new IllegalArgumentException("Unsupported primitive type: " + primitiveType.getPrimitiveTypeName());
    }
  }

  /**
   * Infers ValueType from HoodieSchema for type inference in column statistics.
   * Leverages specialized HoodieSchema subclasses to determine the appropriate ValueType.
   *
   * @param schema the HoodieSchema to infer type from
   * @return the corresponding ValueType
   * @throws IllegalArgumentException if the schema type is not supported
   * @since 1.2.0
   */
  public static ValueType fromSchema(HoodieSchema schema) {
    // Handle logical types first using instanceof checks on specialized classes
    if (schema instanceof HoodieSchema.Decimal) {
      return ValueType.DECIMAL;
    }

    if (schema instanceof HoodieSchema.Time) {
      HoodieSchema.Time time = (HoodieSchema.Time) schema;
      return time.getPrecision() == HoodieSchema.TimePrecision.MILLIS
          ? ValueType.TIME_MILLIS
          : ValueType.TIME_MICROS;
    }

    if (schema instanceof HoodieSchema.Timestamp) {
      HoodieSchema.Timestamp timestamp = (HoodieSchema.Timestamp) schema;
      if (timestamp.isUtcAdjusted()) {
        return timestamp.getPrecision() == HoodieSchema.TimePrecision.MILLIS
            ? ValueType.TIMESTAMP_MILLIS
            : ValueType.TIMESTAMP_MICROS;
      } else {
        return timestamp.getPrecision() == HoodieSchema.TimePrecision.MILLIS
            ? ValueType.LOCAL_TIMESTAMP_MILLIS
            : ValueType.LOCAL_TIMESTAMP_MICROS;
      }
    }

    // Handle primitive types and remaining logical types
    HoodieSchemaType type = schema.getType();
    switch (type) {
      case NULL:
        return ValueType.NULL;
      case BOOLEAN:
        return ValueType.BOOLEAN;
      case INT:
        return ValueType.INT;
      case LONG:
        return ValueType.LONG;
      case FLOAT:
        return ValueType.FLOAT;
      case DOUBLE:
        return ValueType.DOUBLE;
      case BYTES:
        return ValueType.BYTES;
      case STRING:
        return ValueType.STRING;
      case FIXED:
        return ValueType.FIXED;
      case DATE:
        return ValueType.DATE;
      case UUID:
        return ValueType.UUID;
      case UNION:
        return fromSchema(HoodieSchemaUtils.getNonNullTypeFromUnion(schema));
      default:
        throw new IllegalArgumentException("Unsupported type: " + type);
    }
  }

  // Casting to standardize types and also type promotion
  private static Comparable<?> passThrough(Object val, ValueMetadata meta) {
    return (Comparable<?>) val;
  }

  private static Boolean castToBoolean(Object val) {
    if (val instanceof Boolean) {
      return (Boolean) val;
    } else {
      throw new UnsupportedOperationException("Unable to convert boolean: " + val.getClass());
    }
  }

  private static Integer castToInteger(Object val) {
    if (val == null) {
      return null;
    }
    if (val instanceof Integer) {
      return (Integer) val;
    } else if (val instanceof Boolean) {
      return ((Boolean) val) ? 1 : 0;
    } else {
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
    } else if (val instanceof Boolean) {
      return ((Boolean) val) ? 1L : 0L;
    } else {
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
    } else if (val instanceof Boolean) {
      return (Boolean) val ? 1.0f : 0.0f;
    } else {
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
    } else {
      // best effort casting
      return Double.parseDouble(val.toString());
    }
  }

  public static String castToString(Object val) {
    if (val instanceof String) {
      return (String) val;
    } else if (val instanceof Utf8 || val instanceof Boolean || val instanceof Integer || val instanceof Long || val instanceof Float || val instanceof Double) {
      return val.toString();
    } else if (val instanceof Binary) {
      return ((Binary) val).toStringUsingUTF8();
    } else {
      throw new UnsupportedOperationException("Unable to convert string: " + val.getClass());
    }
  }

  public static ByteBuffer castToBytes(Object val) {
    if (val instanceof ByteBuffer) {
      return (ByteBuffer) val;
    } else if (val instanceof GenericData.Fixed) {
      return ByteBuffer.wrap(((GenericData.Fixed) val).bytes());
    } else if (val instanceof byte[]) {
      return ByteBuffer.wrap((byte[]) val);
    } else if (val instanceof Binary) {
      return ((Binary) val).toByteBuffer();
    } else if (val instanceof String) {
      return ByteBuffer.wrap(getUTF8Bytes(val.toString()));
    } else {
      throw new UnsupportedOperationException("Unable to convert bytes: " + val.getClass());
    }
  }

  public static ByteBuffer castToFixed(Object val) {
    if (val instanceof ByteBuffer) {
      return (ByteBuffer) val;
    } else if (val instanceof GenericData.Fixed) {
      return ByteBuffer.wrap(((GenericData.Fixed) val).bytes());
    } else if (val instanceof byte[]) {
      return ByteBuffer.wrap((byte[]) val);
    } else if (val instanceof Binary) {
      return ((Binary) val).toByteBuffer();
    } else {
      throw new UnsupportedOperationException("Unable to convert fixed: " + val.getClass());
    }
  }

  public static BigDecimal castToDecimal(Object val, ValueMetadata meta) {
    ValueMetadata.DecimalValueMetadata decimalMetadata = (ValueMetadata.DecimalValueMetadata) meta;
    int precision = decimalMetadata.getPrecision();
    int scale = decimalMetadata.getScale();
    if (val instanceof BigDecimal) {
      return (BigDecimal) val;
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
      return new BigDecimal(new BigInteger(((Binary) val).getBytesUnsafe()), scale, new MathContext(precision, RoundingMode.HALF_UP));
    } else {
      throw new UnsupportedOperationException("Unable to convert decimal: " + val.getClass());
    }
  }

  public static UUID castToUUID(Object val, ValueMetadata meta) {
    if (val instanceof UUID) {
      return (UUID) val;
    } else if (val instanceof String) {
      return java.util.UUID.fromString((String) val);
    } else {
      throw new UnsupportedOperationException("Unable to convert UUID: " + val.getClass());
    }
  }

  public static LocalDate castToDate(Object val, ValueMetadata meta) {
    if (val instanceof LocalDate) {
      return (LocalDate) val;
    } else if (val instanceof java.sql.Date) {
      return ((java.sql.Date) val).toLocalDate();
    } else if (val instanceof Integer) {
      return LocalDate.ofEpochDay((Integer) val);
    } else {
      throw new UnsupportedOperationException("Unable to convert date: " + val.getClass());
    }
  }

  public static LocalTime castToTimeMillis(Object val, ValueMetadata meta) {
    if (val instanceof LocalTime) {
      return (LocalTime) val;
    } else if (val instanceof Integer) {
      return LocalTime.ofNanoOfDay((Integer) val * 1_000_000L);
    } else {
      throw new UnsupportedOperationException("Unable to convert time millis: " + val.getClass());
    }
  }

  public static LocalTime castToTimeMicros(Object val, ValueMetadata meta) {
    if (val instanceof LocalTime) {
      return (LocalTime) val;
    } else if (val instanceof Long) {
      return LocalTime.ofNanoOfDay((Long) val * 1000);
    } else {
      throw new UnsupportedOperationException("Unable to convert time micros: " + val.getClass());
    }
  }

  public static Instant castToTimestampMillis(Object val, ValueMetadata meta) {
    if (val instanceof Instant) {
      return (Instant) val;
    } else if (val instanceof Timestamp) {
      return ((Timestamp) val).toInstant();
    } else if (val instanceof Long) {
      return Instant.ofEpochMilli((Long) val);
    } else {
      throw new UnsupportedOperationException("Unable to convert timestamp millis: " + val.getClass());
    }
  }

  public static Instant castToTimestampMicros(Object val, ValueMetadata meta) {
    if (val instanceof Instant) {
      return (Instant) val;
    } else if (val instanceof Timestamp) {
      return ((Timestamp) val).toInstant();
    } else if (val instanceof Long) {
      return DateTimeUtils.microsToInstant((Long) val);
    } else {
      throw new UnsupportedOperationException("Unable to convert timestamp micros: " + val.getClass());
    }
  }

  public static Instant castToTimestampNanos(Object val, ValueMetadata meta) {
    if (val instanceof Instant) {
      return (Instant) val;
    } else if (val instanceof Long) {
      return nanosToInstant((Long) val);
    } else {
      throw new UnsupportedOperationException("Unable to convert timestamp nanos: " + val.getClass());
    }
  }

  public static LocalDateTime castToLocalTimestampMillis(Object val, ValueMetadata meta) {
    if (val instanceof LocalDateTime) {
      return (LocalDateTime) val;
    } else if (val instanceof Long) {
      return LocalDateTime.ofInstant(Instant.ofEpochMilli((Long) val), ZoneOffset.UTC);
    } else {
      throw new UnsupportedOperationException("Unable to convert local timestamp millis: " + val.getClass());
    }
  }

  public static LocalDateTime castToLocalTimestampMicros(Object val, ValueMetadata meta) {
    if (val instanceof LocalDateTime) {
      return (LocalDateTime) val;
    } else if (val instanceof Long) {
      return LocalDateTime.ofInstant(microsToInstant((Long) val), ZoneOffset.UTC);
    } else {
      throw new UnsupportedOperationException("Unable to convert local timestamp micros: " + val.getClass());
    }
  }

  public static LocalDateTime castToLocalTimestampNanos(Object val, ValueMetadata meta) {
    if (val instanceof LocalDateTime) {
      return (LocalDateTime) val;
    } else if (val instanceof Long) {
      return LocalDateTime.ofInstant(nanosToInstant((Long) val), ZoneOffset.UTC);
    } else {
      throw new UnsupportedOperationException("Unable to convert local timestamp nanos: " + val.getClass());
    }
  }

  // Conversion to and from primitive types and complex types

  public static BigDecimal toDecimal(Comparable<?> val, ValueMetadata meta) {
    ValueMetadata.DecimalMetadata decimalMeta = (ValueMetadata.DecimalMetadata) meta;
    return convertBytesToBigDecimal(((ByteBuffer) val).array(), decimalMeta.getPrecision(), decimalMeta.getScale());
  }

  public static ByteBuffer fromDecimal(Comparable<?> val, ValueMetadata meta) {
    return ByteBuffer.wrap(((BigDecimal) val).unscaledValue().toByteArray());
  }

  public static UUID toUUID(Comparable<?> val, ValueMetadata meta) {
    return java.util.UUID.fromString((String) val);
  }

  public static String fromUUID(Comparable<?> val, ValueMetadata meta) {
    return ((UUID) val).toString();
  }

  public static LocalDate toDate(Comparable<?> val, ValueMetadata meta) {
    return LocalDate.ofEpochDay((Integer) val);
  }

  public static Integer fromDate(Comparable<?> val, ValueMetadata meta) {
    return ((Long) ((LocalDate) val).toEpochDay()).intValue();
  }

  public static LocalTime toTimeMillis(Comparable<?> val, ValueMetadata meta) {
    return LocalTime.ofNanoOfDay((Integer) val * 1_000_000L);
  }

  public static Integer fromTimeMillis(Comparable<?> val, ValueMetadata meta) {
    return ((LocalTime) val).toSecondOfDay() * 1000 + (((LocalTime) val).getNano() / 1_000_000);
  }

  public static LocalTime toTimeMicros(Comparable<?> val, ValueMetadata meta) {
    return LocalTime.ofNanoOfDay((Long) val * 1000);
  }

  public static Long fromTimeMicros(Comparable<?> val, ValueMetadata meta) {
    return ((LocalTime) val).toSecondOfDay() * 1_000_000L + (((LocalTime) val).getNano() / 1_000);
  }

  public static Instant toTimestampMillis(Comparable<?> val, ValueMetadata meta) {
    return Instant.ofEpochMilli((Long) val);
  }

  public static Long fromTimestampMillis(Comparable<?> val, ValueMetadata meta) {
    return ((Instant) val).toEpochMilli();
  }

  public static Instant toTimestampMicros(Comparable<?> val, ValueMetadata meta) {
    return microsToInstant((Long) val);
  }

  public static Long fromTimestampMicros(Comparable<?> val, ValueMetadata meta) {
    return instantToMicros((Instant) val);
  }

  public static Instant toTimestampNanos(Comparable<?> val, ValueMetadata meta) {
    return nanosToInstant((Long) val);
  }

  public static Long fromTimestampNanos(Comparable<?> val, ValueMetadata meta) {
    return instantToNanos((Instant) val);
  }

  public static LocalDateTime toLocalTimestampMillis(Comparable<?> val, ValueMetadata meta) {
    return LocalDateTime.ofInstant(Instant.ofEpochMilli((Long) val), ZoneOffset.UTC);
  }

  public static Long fromLocalTimestampMillis(Comparable<?> val, ValueMetadata meta) {
    return ((LocalDateTime) val).toInstant(ZoneOffset.UTC).toEpochMilli();
  }

  public static LocalDateTime toLocalTimestampMicros(Comparable<?> val, ValueMetadata meta) {
    return LocalDateTime.ofInstant(microsToInstant((Long) val), ZoneOffset.UTC);
  }

  public static Long fromLocalTimestampMicros(Comparable<?> val, ValueMetadata meta) {
    return instantToMicros(((LocalDateTime) val).toInstant(ZoneOffset.UTC));
  }

  public static LocalDateTime toLocalTimestampNanos(Comparable<?> val, ValueMetadata meta) {
    return LocalDateTime.ofInstant(nanosToInstant((Long) val), ZoneOffset.UTC);
  }

  public static Long fromLocalTimestampNanos(Comparable<?> val, ValueMetadata meta) {
    return instantToNanos(((LocalDateTime) val).toInstant(ZoneOffset.UTC));
  }
}


