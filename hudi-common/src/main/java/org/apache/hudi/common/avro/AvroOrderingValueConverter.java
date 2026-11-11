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

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Converts delete ordering values using each field's schema and
 * {@link HoodieAvroUtils#convertValueForAvroLogicalTypes(Schema, Object, boolean)}.
 * Preserves null field values and converts strings to Avro's comparable representation.
 */
public class AvroOrderingValueConverter implements Serializable {
  private static final long serialVersionUID = 1L;

  private final List<HoodieSchema> orderingFieldSchemas;
  private final boolean consistentLogicalTimestampEnabled;

  private AvroOrderingValueConverter(HoodieSchema dataSchema, List<String> orderingFieldNames, boolean consistentLogicalTimestampEnabled) {
    this.orderingFieldSchemas = orderingFieldNames.stream()
        .map(field -> HoodieSchemaUtils.getNestedField(dataSchema, field)
            .map(Pair::getRight).map(HoodieSchemaField::getNonNullSchema)
            .orElseThrow(() -> new IllegalArgumentException("Ordering field '" + field + "' not found in table schema")))
        .collect(Collectors.toList());
    this.consistentLogicalTimestampEnabled = consistentLogicalTimestampEnabled;
  }

  public static AvroOrderingValueConverter create(HoodieSchema dataSchema, List<String> orderingFieldNames, boolean consistentLogicalTimestampEnabled) {
    return new AvroOrderingValueConverter(dataSchema, orderingFieldNames, consistentLogicalTimestampEnabled);
  }

  public Comparable convert(Comparable value) {
    if (orderingFieldSchemas.isEmpty()) {
      return value;
    }
    if (value instanceof ArrayComparable) {
      List<Comparable> values = ((ArrayComparable) value).getValues();
      Comparable[] converted = new Comparable[values.size()];
      for (int i = 0; i < values.size(); i++) {
        converted[i] = convertFieldValue(values.get(i), orderingFieldSchemas.get(i));
      }
      return new ArrayComparable(converted);
    }
    return convertFieldValue(value, orderingFieldSchemas.get(0));
  }

  private Comparable convertFieldValue(Comparable value, HoodieSchema fieldSchema) {
    // Decoded decimals already use the representation needed for comparison.
    if (value == null || value instanceof BigDecimal) {
      return value;
    }
    Schema avroSchema = fieldSchema.toAvroSchema();
    LogicalType logicalType = avroSchema.getLogicalType();
    // Normalize decoded delete values here. Timestamp precision and configuration
    // handling remain shared with data records.
    if (value instanceof String) {
      return new Utf8((String) value);
    }
    if (value instanceof Date) {
      return ((Date) value).toLocalDate();
    }
    if (value instanceof Timestamp) {
      if (logicalType == LogicalTypes.localTimestampMillis() || logicalType == LogicalTypes.localTimestampMicros()) {
        value = ((Timestamp) value).toLocalDateTime();
      } else if (logicalType == LogicalTypes.timestampMillis() || logicalType == LogicalTypes.timestampMicros()) {
        value = ((Timestamp) value).toInstant();
      }
    }
    return (Comparable) HoodieAvroUtils.convertValueForAvroLogicalTypes(
        avroSchema, value, consistentLogicalTimestampEnabled);
  }
}
