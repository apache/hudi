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

package org.apache.hudi.internal.schema;

import org.apache.hudi.common.util.PartitionPathEncodeUtils;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * The type of a schema, reference avro schema.
 * now avro version used by hoodie, not support localTime.
 * to do add support for localTime if avro version is updated
 */
public interface Type extends Serializable {

  OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
  LocalDate EPOCH_DAY = EPOCH.toLocalDate();

  /**
   * Enums for type names.
   */
  enum TypeID {
    RECORD(Types.RecordType.class),
    ARRAY(List.class),
    MAP(Map.class),
    FIXED(ByteBuffer.class),
    STRING(String.class),
    BINARY(ByteBuffer.class),
    INT(Integer.class),
    LONG(Long.class),
    FLOAT(Float.class),
    DOUBLE(Double.class),
    DATE(Integer.class),
    BOOLEAN(Boolean.class),
    TIME(Long.class),
    TIMESTAMP(Long.class),
    DECIMAL(BigDecimal.class),
    UUID(UUID.class),
    DECIMAL_BYTES(BigDecimal.class),
    DECIMAL_FIXED(BigDecimal.class),
    TIME_MILLIS(Integer.class),
    TIMESTAMP_MILLIS(Long.class),
    LOCAL_TIMESTAMP_MILLIS(Long.class),
    LOCAL_TIMESTAMP_MICROS(Long.class);

    private final String name;
    private final Class<?> classTag;

    TypeID(Class<?> classTag) {
      this.name = this.name().toLowerCase(Locale.ROOT);
      this.classTag = classTag;
    }

    public String getName() {
      return name;
    }

    public Class<?> getClassTag() {
      return classTag;
    }
  }

  static TypeID fromValue(String value) {
    try {
      return TypeID.valueOf(value.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(String.format("Invalid value of Type: %s", value));
    }
  }

  static Object fromPartitionString(String partitionValue, Type type) {
    if (partitionValue == null
        || PartitionPathEncodeUtils.DEFAULT_PARTITION_PATH.equals(partitionValue)
        || PartitionPathEncodeUtils.DEPRECATED_DEFAULT_PARTITION_PATH.equals(partitionValue)) {
      return null;
    }

    switch (type.typeId()) {
      case INT:
        return Integer.parseInt(partitionValue);
      case LONG:
        return Long.parseLong(partitionValue);
      case BOOLEAN:
        return Boolean.parseBoolean(partitionValue);
      case FLOAT:
        return Float.parseFloat(partitionValue);
      case DECIMAL:
      case DECIMAL_BYTES:
      case DECIMAL_FIXED:
        return new BigDecimal(partitionValue);
      case DOUBLE:
        return Double.parseDouble(partitionValue);
      case UUID:
        return UUID.fromString(partitionValue);
      case DATE:
        // TODO Support different date format
        return Math.toIntExact(ChronoUnit.DAYS.between(
            EPOCH_DAY, LocalDate.parse(partitionValue, DateTimeFormatter.ISO_LOCAL_DATE)));
      case STRING:
        return partitionValue;
      default:
        throw new UnsupportedOperationException("Cast value " + partitionValue
            + " to type " + type + " is not supported yet");
    }
  }

  TypeID typeId();

  default boolean isNestedType() {
    return false;
  }

  /**
   * The type of a schema for primitive fields.
   */
  abstract class PrimitiveType implements Type {
    @Override
    public boolean isNestedType() {
      return false;
    }

    /**
     * We need to override equals because the check {@code intType1 == intType2} can return {@code false}.
     * Despite the fact that most subclasses look like singleton with static field {@code INSTANCE},
     * they can still be created by deserializer.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (!(o instanceof PrimitiveType)) {
        return false;
      }
      PrimitiveType that = (PrimitiveType) o;
      return typeId().equals(that.typeId());
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(typeId());
    }
  }

  /**
   * The type of a schema for nested fields.
   */
  abstract class NestedType implements Type {

    @Override
    public boolean isNestedType() {
      return true;
    }

    public abstract List<Types.Field> fields();

    public abstract Type fieldType(String name);

    public abstract Types.Field field(int id);
  }
}
