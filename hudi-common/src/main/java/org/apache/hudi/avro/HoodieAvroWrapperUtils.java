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

package org.apache.hudi.avro;

import org.apache.hudi.avro.model.ArrayWrapper;
import org.apache.hudi.avro.model.BooleanWrapper;
import org.apache.hudi.avro.model.BytesWrapper;
import org.apache.hudi.avro.model.DateWrapper;
import org.apache.hudi.avro.model.DecimalWrapper;
import org.apache.hudi.avro.model.DoubleWrapper;
import org.apache.hudi.avro.model.FloatWrapper;
import org.apache.hudi.avro.model.IntWrapper;
import org.apache.hudi.avro.model.LocalDateWrapper;
import org.apache.hudi.avro.model.LongWrapper;
import org.apache.hudi.avro.model.StringWrapper;
import org.apache.hudi.avro.model.TimestampMicrosWrapper;
import org.apache.hudi.common.util.OrderingValues;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ArrayComparable;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.util.Lazy;

import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.DateTimeUtils.instantToMicros;
import static org.apache.hudi.common.util.DateTimeUtils.microsToInstant;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.tryUpcastDecimal;

public class HoodieAvroWrapperUtils {

  private static final Conversions.DecimalConversion AVRO_DECIMAL_CONVERSION = new Conversions.DecimalConversion();
  /**
   * NOTE: PLEASE READ CAREFULLY
   * <p>
   * In Avro 1.10 generated builders rely on {@code SpecificData.getForSchema} invocation that in turn
   * does use reflection to load the code-gen'd class corresponding to the Avro record model. This has
   * serious adverse effects in terms of performance when gets executed on the hot-path (both, in terms
   * of runtime and efficiency).
   * <p>
   * To work this around instead of using default code-gen'd builder invoking {@code SpecificData.getForSchema},
   * we instead rely on overloaded ctor accepting another instance of the builder: {@code Builder(Builder)},
   * which bypasses such invocation. Following corresponding builder's stubs are statically initialized
   * to be used exactly for that purpose.
   * <p>
   * You can find more details in HUDI-3834.
   */
  private static final Lazy<StringWrapper.Builder> STRING_WRAPPER_BUILDER_STUB = Lazy.lazily(StringWrapper::newBuilder);
  private static final Lazy<BytesWrapper.Builder> BYTES_WRAPPER_BUILDER_STUB = Lazy.lazily(BytesWrapper::newBuilder);
  private static final Lazy<DoubleWrapper.Builder> DOUBLE_WRAPPER_BUILDER_STUB = Lazy.lazily(DoubleWrapper::newBuilder);
  private static final Lazy<FloatWrapper.Builder> FLOAT_WRAPPER_BUILDER_STUB = Lazy.lazily(FloatWrapper::newBuilder);
  private static final Lazy<LongWrapper.Builder> LONG_WRAPPER_BUILDER_STUB = Lazy.lazily(LongWrapper::newBuilder);
  private static final Lazy<IntWrapper.Builder> INT_WRAPPER_BUILDER_STUB = Lazy.lazily(IntWrapper::newBuilder);
  private static final Lazy<BooleanWrapper.Builder> BOOLEAN_WRAPPER_BUILDER_STUB = Lazy.lazily(BooleanWrapper::newBuilder);
  private static final Lazy<TimestampMicrosWrapper.Builder> TIMESTAMP_MICROS_WRAPPER_BUILDER_STUB = Lazy.lazily(TimestampMicrosWrapper::newBuilder);
  private static final Lazy<DecimalWrapper.Builder> DECIMAL_WRAPPER_BUILDER_STUB = Lazy.lazily(DecimalWrapper::newBuilder);
  private static final Lazy<DateWrapper.Builder> DATE_WRAPPER_BUILDER_STUB = Lazy.lazily(DateWrapper::newBuilder);
  private static final Lazy<LocalDateWrapper.Builder> LOCAL_DATE_WRAPPER_BUILDER_STUB = Lazy.lazily(LocalDateWrapper::newBuilder);
  private static final Lazy<ArrayWrapper.Builder> ARRAY_WRAPPER_BUILDER_STUB = Lazy.lazily(ArrayWrapper::newBuilder);

  /**
   * Wraps a value into Avro type wrapper.
   *
   * @param value Java value.
   * @return A wrapped value with Avro type wrapper.
   */
  public static Object wrapValueIntoAvro(Comparable<?> value) {
    if (value == null) {
      return null;
    } else if (value instanceof Date) {
      // NOTE: Due to breaking changes in code-gen b/w Avro 1.8.2 and 1.10, we can't
      //       rely on logical types to do proper encoding of the native Java types,
      //       and hereby have to encode value manually
      LocalDate localDate = ((Date) value).toLocalDate();
      return DateWrapper.newBuilder(DATE_WRAPPER_BUILDER_STUB.get())
          .setValue((int) localDate.toEpochDay())
          .build();
    } else if (value instanceof LocalDate) {
      // NOTE: Due to breaking changes in code-gen b/w Avro 1.8.2 and 1.10, we can't
      //       rely on logical types to do proper encoding of the native Java types,
      //       and hereby have to encode value manually
      LocalDate localDate = (LocalDate) value;
      return LocalDateWrapper.newBuilder(LOCAL_DATE_WRAPPER_BUILDER_STUB.get())
          .setValue((int) localDate.toEpochDay())
          .build();
    } else if (value instanceof BigDecimal) {
      Schema valueSchema = DecimalWrapper.SCHEMA$.getField("value").schema();
      BigDecimal upcastDecimal = tryUpcastDecimal((BigDecimal) value, (LogicalTypes.Decimal) valueSchema.getLogicalType());
      return DecimalWrapper.newBuilder(DECIMAL_WRAPPER_BUILDER_STUB.get())
          .setValue(AVRO_DECIMAL_CONVERSION.toBytes(upcastDecimal, valueSchema, valueSchema.getLogicalType()))
          .build();
    } else if (value instanceof Timestamp) {
      // NOTE: Due to breaking changes in code-gen b/w Avro 1.8.2 and 1.10, we can't
      //       rely on logical types to do proper encoding of the native Java types,
      //       and hereby have to encode value manually
      Instant instant = ((Timestamp) value).toInstant();
      return TimestampMicrosWrapper.newBuilder(TIMESTAMP_MICROS_WRAPPER_BUILDER_STUB.get())
          .setValue(instantToMicros(instant))
          .build();
    } else if (value instanceof Boolean) {
      return wrapBoolean(value);
    } else if (value instanceof Integer) {
      return wrapInt(value);
    } else if (value instanceof Long) {
      return wrapLong(value);
    } else if (value instanceof Float) {
      return wrapFloat(value);
    } else if (value instanceof Double) {
      return wrapDouble(value);
    } else if (value instanceof ByteBuffer) {
      return wrapBytes(value);
    } else if (value instanceof String || value instanceof Utf8) {
      return wrapString(value.toString());
    } else if (value instanceof ArrayComparable) {
      return wrapArray(value, HoodieAvroWrapperUtils::wrapValueIntoAvro);
    } else {
      throw new UnsupportedOperationException(String.format("Unsupported type of the value (%s)", value.getClass()));
    }
  }

  /**
   * Unwraps Avro value wrapper into Java value.
   *
   * @param avroValueWrapper A wrapped value with Avro type wrapper.
   * @return Java value.
   */
  public static Comparable<?> unwrapAvroValueWrapper(Object avroValueWrapper) {
    if (avroValueWrapper == null) {
      return null;
    }

    Pair<Boolean, String> isValueWrapperObfuscated = getIsValueWrapperObfuscated(avroValueWrapper);
    if (isValueWrapperObfuscated.getKey()) {
      return unwrapAvroValueWrapper(avroValueWrapper, isValueWrapperObfuscated.getValue());
    }

    if (avroValueWrapper instanceof DateWrapper) {
      return Date.valueOf(LocalDate.ofEpochDay(((DateWrapper) avroValueWrapper).getValue()));
    } else if (avroValueWrapper instanceof LocalDateWrapper) {
      return LocalDate.ofEpochDay(((LocalDateWrapper) avroValueWrapper).getValue());
    } else if (avroValueWrapper instanceof DecimalWrapper) {
      Schema valueSchema = DecimalWrapper.SCHEMA$.getField("value").schema();
      return AVRO_DECIMAL_CONVERSION.fromBytes(((DecimalWrapper) avroValueWrapper).getValue(), valueSchema, valueSchema.getLogicalType());
    } else if (avroValueWrapper instanceof TimestampMicrosWrapper) {
      return microsToInstant(((TimestampMicrosWrapper) avroValueWrapper).getValue());
    } else if (avroValueWrapper instanceof BooleanWrapper) {
      return unwrapBoolean(avroValueWrapper);
    } else if (avroValueWrapper instanceof IntWrapper) {
      return unwrapInt(avroValueWrapper);
    } else if (avroValueWrapper instanceof LongWrapper) {
      return unwrapLong(avroValueWrapper);
    } else if (avroValueWrapper instanceof FloatWrapper) {
      return unwrapFloat(avroValueWrapper);
    } else if (avroValueWrapper instanceof DoubleWrapper) {
      return unwrapDouble(avroValueWrapper);
    } else if (avroValueWrapper instanceof BytesWrapper) {
      return unwrapBytes(avroValueWrapper);
    } else if (avroValueWrapper instanceof StringWrapper) {
      return unwrapString(avroValueWrapper);
    } else if (avroValueWrapper instanceof ArrayWrapper) {
      return unwrapArray(avroValueWrapper, HoodieAvroWrapperUtils::unwrapAvroValueWrapper);
    } else if (avroValueWrapper instanceof GenericRecord) {
      return unwrapGenericRecord(avroValueWrapper);
    } else {
      throw new UnsupportedOperationException(String.format("Unsupported type of the value (%s)", avroValueWrapper.getClass()));
    }
  }

  public static Comparable<?> unwrapAvroValueWrapper(Object avroValueWrapper, String wrapperClassName) {
    if (avroValueWrapper == null) {
      return null;
    } else if (DateWrapper.class.getSimpleName().equals(wrapperClassName)) {
      ValidationUtils.checkArgument(avroValueWrapper instanceof GenericRecord);
      return Date.valueOf(LocalDate.ofEpochDay((Integer) ((GenericRecord) avroValueWrapper).get(0)));
    } else if (LocalDateWrapper.class.getSimpleName().equals(wrapperClassName)) {
      ValidationUtils.checkArgument(avroValueWrapper instanceof GenericRecord);
      return LocalDate.ofEpochDay((Integer) ((GenericRecord) avroValueWrapper).get(0));
    } else if (TimestampMicrosWrapper.class.getSimpleName().equals(wrapperClassName)) {
      ValidationUtils.checkArgument(avroValueWrapper instanceof GenericRecord);
      Instant instant = microsToInstant((Long) ((GenericRecord) avroValueWrapper).get(0));
      return Timestamp.from(instant);
    } else if (DecimalWrapper.class.getSimpleName().equals(wrapperClassName)) {
      Schema valueSchema = DecimalWrapper.SCHEMA$.getField("value").schema();
      ValidationUtils.checkArgument(avroValueWrapper instanceof GenericRecord);
      return AVRO_DECIMAL_CONVERSION.fromBytes((ByteBuffer)((GenericRecord) avroValueWrapper).get(0), valueSchema, valueSchema.getLogicalType());
    } else {
      throw new UnsupportedOperationException(String.format("Unsupported type of the value (%s)", avroValueWrapper.getClass()));
    }
  }

  private static Pair<Boolean, String> getIsValueWrapperObfuscated(Object statsValue) {
    if (statsValue != null) {
      String statsValueSchemaClassName = ((GenericRecord) statsValue).getSchema().getName();
      boolean toReturn = statsValueSchemaClassName.equals(DateWrapper.class.getSimpleName())
          || statsValueSchemaClassName.equals(LocalDateWrapper.class.getSimpleName())
          || statsValueSchemaClassName.equals(TimestampMicrosWrapper.class.getSimpleName())
          || statsValueSchemaClassName.equals(DecimalWrapper.class.getSimpleName());
      if (toReturn) {
        return Pair.of(true, ((GenericRecord) statsValue).getSchema().getName());
      }
    }
    return Pair.of(false, null);
  }

  public enum PrimitiveWrapperType {
    V1(Object.class, HoodieAvroWrapperUtils::wrapValueIntoAvro, HoodieAvroWrapperUtils::unwrapAvroValueWrapper, GenericRecord.class),
    NULL(Void.class, HoodieAvroWrapperUtils::wrapNull, HoodieAvroWrapperUtils::unwrapNull, Void.class),
    BOOLEAN(Boolean.class, HoodieAvroWrapperUtils::wrapBoolean, HoodieAvroWrapperUtils::unwrapBoolean, BooleanWrapper.class),
    INT(Integer.class, HoodieAvroWrapperUtils::wrapInt, HoodieAvroWrapperUtils::unwrapInt, IntWrapper.class),
    LONG(Long.class, HoodieAvroWrapperUtils::wrapLong, HoodieAvroWrapperUtils::unwrapLong, LongWrapper.class),
    FLOAT(Float.class, HoodieAvroWrapperUtils::wrapFloat, HoodieAvroWrapperUtils::unwrapFloat, FloatWrapper.class),
    DOUBLE(Double.class, HoodieAvroWrapperUtils::wrapDouble, HoodieAvroWrapperUtils::unwrapDouble, DoubleWrapper.class),
    STRING(String.class, HoodieAvroWrapperUtils::wrapString, HoodieAvroWrapperUtils::unwrapString, StringWrapper.class),
    BYTES(ByteBuffer.class, HoodieAvroWrapperUtils::wrapBytes, HoodieAvroWrapperUtils::unwrapBytes, BytesWrapper.class);

    private final Class<?> clazz;
    private final Function<Comparable<?>, Object> wrapper;
    private final Function<Object, Comparable<?>> unwrapper;
    private final Class<?> wrapperClass;

    PrimitiveWrapperType(Class<?> clazz, Function<Comparable<?>, Object> wrapper, Function<Object, Comparable<?>> unwrapper, Class<?> wrapperClass) {
      this.clazz = clazz;
      this.wrapper = wrapper;
      this.unwrapper = unwrapper;
      this.wrapperClass = wrapperClass;
    }

    Class<?> getClazz() {
      return clazz;
    }

    Object wrap(Comparable<?> value) {
      return wrapper.apply(value);
    }

    Comparable<?> unwrap(Object value) {
      return unwrapper.apply(value);
    }

    Class<?> getWrapperClass() {
      return wrapperClass;
    }
  }

  private static Object wrapNull(Comparable<?> value) {
    return value;
  }

  private static Object wrapBoolean(Comparable<?> value) {
    return BooleanWrapper.newBuilder(BOOLEAN_WRAPPER_BUILDER_STUB.get()).setValue((Boolean) value).build();
  }

  private static Object wrapInt(Comparable<?> value) {
    return IntWrapper.newBuilder(INT_WRAPPER_BUILDER_STUB.get()).setValue((Integer) value).build();
  }

  private static Object wrapLong(Comparable<?> value) {
    return LongWrapper.newBuilder(LONG_WRAPPER_BUILDER_STUB.get()).setValue((Long) value).build();
  }

  private static Object wrapFloat(Comparable<?> value) {
    return FloatWrapper.newBuilder(FLOAT_WRAPPER_BUILDER_STUB.get()).setValue((Float) value).build();
  }

  private static Object wrapDouble(Comparable<?> value) {
    return DoubleWrapper.newBuilder(DOUBLE_WRAPPER_BUILDER_STUB.get()).setValue((Double) value).build();
  }

  private static Object wrapString(Comparable<?> value) {
    return StringWrapper.newBuilder(STRING_WRAPPER_BUILDER_STUB.get()).setValue((String) value).build();
  }

  private static Object wrapBytes(Comparable<?> value) {
    return BytesWrapper.newBuilder(BYTES_WRAPPER_BUILDER_STUB.get()).setValue((ByteBuffer) value).build();
  }

  public static Object wrapArray(Comparable<?> value, Function<Comparable<?>, Object> wrapper) {
    List<Object> avroValues = OrderingValues.getValues((ArrayComparable) value).stream().map(wrapper::apply).collect(Collectors.toList());
    return ArrayWrapper.newBuilder(ARRAY_WRAPPER_BUILDER_STUB.get()).setWrappedValues(avroValues).build();
  }

  private static Comparable<?> unwrapNull(Object val) {
    return (Comparable<?>) val;
  }

  private static Comparable<?> unwrapBoolean(Object val) {
    return ((BooleanWrapper) val).getValue();
  }

  private static Comparable<?> unwrapInt(Object val) {
    return ((IntWrapper) val).getValue();
  }

  private static Comparable<?> unwrapLong(Object val) {
    return ((LongWrapper) val).getValue();
  }

  private static Comparable<?> unwrapFloat(Object val) {
    return ((FloatWrapper) val).getValue();
  }

  private static Comparable<?> unwrapDouble(Object val) {
    return ((DoubleWrapper) val).getValue();
  }

  private static Comparable<?> unwrapString(Object val) {
    return ((StringWrapper) val).getValue();
  }

  private static Comparable<?> unwrapBytes(Object val) {
    return ((BytesWrapper) val).getValue();
  }

  public static Comparable<?> unwrapArray(Object val, Function<Object, Comparable<?>> unwrapper) {
    ArrayWrapper arrayWrapper = (ArrayWrapper) val;
    return OrderingValues.create(arrayWrapper.getWrappedValues().stream()
        .map(unwrapper::apply)
        .toArray(Comparable[]::new));
  }

  // NOTE: This branch could be hit b/c Avro records could be reconstructed
  //       as {@code GenericRecord)
  public static Comparable<?> unwrapGenericRecord(Object val) {
    GenericRecord genRec = (GenericRecord) val;
    return (Comparable<?>) genRec.get("value");
  }
}
