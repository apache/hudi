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

package org.apache.hudi.util;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.OrderingValues;
import org.apache.hudi.common.util.collection.ArrayComparable;
import org.apache.hudi.common.util.collection.Pair;

import lombok.AccessLevel;
import lombok.Getter;
import org.apache.flink.table.types.DataType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Converts ordering values between native Java types and Flink engine types.
 */
public class OrderingValueEngineTypeConverter {
  @Getter(value = AccessLevel.PACKAGE)
  private volatile List<Function<Comparable, Comparable>> engineTypeConverters;
  @Getter(value = AccessLevel.PACKAGE)
  private volatile List<Function<Comparable, Comparable>> javaConverters;
  private final HoodieSchema dataSchema;
  private final List<String> orderingFieldNames;
  private final boolean readUtcTimezone;
  private final boolean writeUtcTimezone;

  private OrderingValueEngineTypeConverter(HoodieSchema dataSchema, List<String> orderingFieldNames,
                                          boolean readUtcTimezone, boolean writeUtcTimezone) {
    this.dataSchema = dataSchema;
    this.orderingFieldNames = new ArrayList<>(orderingFieldNames);
    this.readUtcTimezone = readUtcTimezone;
    this.writeUtcTimezone = writeUtcTimezone;
  }

  public Comparable convertToEngineType(Comparable value) {
    if (engineTypeConverters == null) {
      engineTypeConverters = createConverters(dataSchema, orderingFieldNames, readUtcTimezone);
    }
    return value instanceof ArrayComparable
        ? ((ArrayComparable) value).apply(engineTypeConverters)
        : engineTypeConverters.get(0).apply(value);
  }

  public Comparable convertToJava(Comparable value) {
    if (javaConverters == null) {
      // Delete ordering values must use the same timestamp encoding as written data rows.
      javaConverters = createJavaConverters(dataSchema, orderingFieldNames, writeUtcTimezone);
    }
    // With no ordering fields, apply identity to the whole value, including composite values.
    return value instanceof ArrayComparable && !orderingFieldNames.isEmpty()
        ? ((ArrayComparable) value).apply(javaConverters)
        : javaConverters.get(0).apply(value);
  }

  public static List<Function<Comparable, Comparable>> createConverters(HoodieSchema dataSchema, List<String> orderingFieldNames, boolean utcTimezone) {
    if (orderingFieldNames.isEmpty()) {
      return Collections.singletonList(Function.identity());
    }
    return orderingFieldNames.stream().map(f -> {
      Option<HoodieSchema> fieldSchemaOpt = HoodieSchemaUtils.getNestedField(dataSchema, f).map(Pair::getValue).map(HoodieSchemaField::getNonNullSchema);
      if (fieldSchemaOpt.isEmpty()) {
        return Function.<Comparable>identity();
      } else {
        DataType fieldType = HoodieSchemaConverter.convertToDataType(fieldSchemaOpt.get());
        return RowDataUtils.flinkValFunc(fieldType.getLogicalType(), utcTimezone);
      }
    }).collect(Collectors.toList());
  }

  private static List<Function<Comparable, Comparable>> createJavaConverters(HoodieSchema dataSchema, List<String> orderingFieldNames, boolean utcTimezone) {
    if (orderingFieldNames.isEmpty()) {
      return Collections.singletonList(Function.identity());
    }
    return orderingFieldNames.stream().map(f -> {
      Option<HoodieSchema> fieldSchemaOpt = HoodieSchemaUtils.getNestedField(dataSchema, f).map(Pair::getValue).map(HoodieSchemaField::getNonNullSchema);
      if (fieldSchemaOpt.isEmpty()) {
        return Function.<Comparable>identity();
      }
      DataType fieldType = HoodieSchemaConverter.convertToDataType(fieldSchemaOpt.get());
      Function<Object, Object> converter = RowDataUtils.javaValFunc(fieldType.getLogicalType(), utcTimezone);
      return (Function<Comparable, Comparable>) value -> {
        // Default sentinels are Integers regardless of field type.
        if (value == null || OrderingValues.isDefault(value)) {
          return value;
        }
        return (Comparable) converter.apply(value);
      };
    }).collect(Collectors.toList());
  }

  public static OrderingValueEngineTypeConverter create(HoodieSchema dataSchema, List<String> orderingFieldNames,
                                                       boolean readUtcTimezone, boolean writeUtcTimezone) {
    return new OrderingValueEngineTypeConverter(dataSchema, orderingFieldNames, readUtcTimezone, writeUtcTimezone);
  }
}
