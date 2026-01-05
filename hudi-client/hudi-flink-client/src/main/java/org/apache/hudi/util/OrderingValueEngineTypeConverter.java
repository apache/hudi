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
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.ArrayComparable;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.flink.table.types.DataType;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A converter that converts ordering value from native Java type to Flink engine type.
 */
public class OrderingValueEngineTypeConverter {
  private final List<Function<Comparable, Comparable>> converters;
  private OrderingValueEngineTypeConverter(HoodieSchema dataSchema, List<String> orderingFieldNames, boolean utcTimezone) {
    this.converters = createConverters(dataSchema, orderingFieldNames, utcTimezone);
  }

  public Comparable convert(Comparable value) {
    return value instanceof ArrayComparable
        ? ((ArrayComparable) value).apply(this.converters)
        : this.converters.get(0).apply(value);
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

  @VisibleForTesting
  public List<Function<Comparable, Comparable>> getConverters() {
    return converters;
  }

  public static OrderingValueEngineTypeConverter create(HoodieSchema dataSchema, List<String> orderingFieldNames, boolean utcTimezone) {
    return new OrderingValueEngineTypeConverter(dataSchema, orderingFieldNames, utcTimezone);
  }
}
