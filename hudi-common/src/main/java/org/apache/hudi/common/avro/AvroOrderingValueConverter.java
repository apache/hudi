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

package org.apache.hudi.common.avro;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.util.collection.ArrayComparable;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Conversions.DecimalConversion;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.util.Utf8;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.common.avro.HoodieAvroUtils.extractEpochDay;
import static org.apache.hudi.common.avro.HoodieAvroUtils.extractEpochMicros;
import static org.apache.hudi.common.avro.HoodieAvroUtils.extractEpochMillis;
import static org.apache.hudi.common.avro.HoodieAvroUtils.extractLocalEpochMicros;
import static org.apache.hudi.common.avro.HoodieAvroUtils.extractLocalEpochMillis;

/**
 * Prebuilds a converter for each delete ordering field to match the comparison representation of
 * {@link HoodieAvroUtils#convertValueForAvroLogicalTypes(Schema, Object, boolean)}.
 * Preserves null field values and converts strings to Avro's comparable representation.
 */
public class AvroOrderingValueConverter {
  private final List<Function<Comparable, Comparable>> converters;

  private AvroOrderingValueConverter(HoodieSchema dataSchema, List<String> orderingFieldNames, boolean consistentLogicalTimestampEnabled) {
    this.converters = orderingFieldNames.stream()
        .map(field -> HoodieSchemaUtils.getNestedField(dataSchema, field)
            .map(Pair::getRight).map(HoodieSchemaField::getNonNullSchema)
            .orElseThrow(() -> new IllegalArgumentException("Ordering field '" + field + "' not found in table schema")))
        .map(schema -> createConverter(schema.toAvroSchema(), consistentLogicalTimestampEnabled))
        .map(converter -> (Function<Comparable, Comparable>) value -> value == null ? null : converter.apply(value))
        .collect(Collectors.toList());
  }

  public static AvroOrderingValueConverter create(HoodieSchema dataSchema, List<String> orderingFieldNames, boolean consistentLogicalTimestampEnabled) {
    return new AvroOrderingValueConverter(dataSchema, orderingFieldNames, consistentLogicalTimestampEnabled);
  }

  public Comparable convert(Comparable value) {
    if (converters.isEmpty()) {
      return value;
    }
    return value instanceof ArrayComparable
        ? ((ArrayComparable) value).apply(converters)
        : converters.get(0).apply(value);
  }

  private static Function<Comparable, Comparable> createConverter(Schema schema, boolean consistentLogicalTimestampEnabled) {
    LogicalType logicalType = schema.getLogicalType();
    if (logicalType == LogicalTypes.date()) {
      // Spark/Flink supply epoch days; Avro supplies LocalDate. Callers may supply java.sql.Date.
      return value -> value instanceof Date ? ((Date) value).toLocalDate() : LocalDate.ofEpochDay(extractEpochDay(value));
    }
    if (logicalType == LogicalTypes.timestampMillis()) {
      return consistentLogicalTimestampEnabled
          ? value -> new Timestamp(toEpochMillis(value)) : AvroOrderingValueConverter::toEpochMillis;
    }
    if (logicalType == LogicalTypes.timestampMicros()) {
      // Match the shared normalizer's millisecond truncation, including negative timestamps.
      return consistentLogicalTimestampEnabled
          ? value -> new Timestamp(toEpochMicros(value) / 1000) : AvroOrderingValueConverter::toEpochMicros;
    }
    // A caller-supplied Timestamp represents wall-clock fields for a local timestamp schema.
    if (logicalType == LogicalTypes.localTimestampMillis()) {
      return value -> extractLocalEpochMillis(value instanceof Timestamp ? ((Timestamp) value).toLocalDateTime() : value);
    }
    if (logicalType == LogicalTypes.localTimestampMicros()) {
      return value -> extractLocalEpochMicros(value instanceof Timestamp ? ((Timestamp) value).toLocalDateTime() : value);
    }
    if (logicalType instanceof LogicalTypes.Decimal) {
      // Spark/Flink and Avro extraction produce BigDecimal, preserved by DecimalWrapper decoding.
      DecimalConversion decimalConversion = new DecimalConversion();
      if (schema.getType() == Schema.Type.FIXED) {
        return value -> value instanceof BigDecimal ? value
            : decimalConversion.fromFixed((GenericFixed) value, schema, logicalType);
      } else if (schema.getType() == Schema.Type.BYTES) {
        return value -> {
          if (value instanceof BigDecimal) {
            return value;
          }
          ByteBuffer buffer = (ByteBuffer) value;
          BigDecimal convertedValue = decimalConversion.fromBytes(buffer, schema, logicalType);
          buffer.rewind();
          return convertedValue;
        };
      }
    }
    if (schema.getType() == Schema.Type.STRING) {
      // Spark/Flink Java value extraction and StringWrapper decoding produce String.
      return value -> value instanceof String ? new Utf8((String) value) : value;
    }
    return value -> value;
  }

  private static long toEpochMillis(Comparable value) {
    // Avro extraction with consistent logical timestamps enabled and wrapper decoding yield Timestamp.
    return value instanceof Timestamp ? ((Timestamp) value).getTime() : extractEpochMillis(value);
  }

  private static long toEpochMicros(Comparable value) {
    return extractEpochMicros(value instanceof Timestamp ? ((Timestamp) value).toInstant() : value);
  }
}
