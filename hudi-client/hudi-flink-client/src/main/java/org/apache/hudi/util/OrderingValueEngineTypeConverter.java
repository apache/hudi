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

import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ArrayComparable;

import org.apache.avro.Schema;
import org.apache.flink.table.types.DataType;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A converter that converts ordering value from native Java type to Flink engine type.
 */
public class OrderingValueEngineTypeConverter {
  private final List<Function<Comparable, Comparable>> converters;
  private OrderingValueEngineTypeConverter(Schema dataSchema, List<String> orderingFieldNames, boolean utcTimezone) {
    this.converters = createConverters(dataSchema, orderingFieldNames, utcTimezone);
  }

  public Comparable convert(Comparable value) {
    return value instanceof ArrayComparable
        ? ((ArrayComparable) value).apply(this.converters)
        : this.converters.get(0).apply(value);
  }

  public static List<Function<Comparable, Comparable>> createConverters(Schema dataSchema, List<String> orderingFieldNames, boolean utcTimezone) {
    return orderingFieldNames.stream().map(f -> {
      Schema fieldSchema = dataSchema.getField(f).schema();
      ValidationUtils.checkArgument(fieldSchema != null, "ordering field " + f + " should be included in data schema: " + dataSchema);
      DataType fieldType =  AvroSchemaConverter.convertToDataType(fieldSchema);
      return RowDataUtils.flinkValFunc(fieldType.getLogicalType(), utcTimezone);
    }).collect(Collectors.toList());

  }

  public static OrderingValueEngineTypeConverter create(Schema dataSchema, List<String> orderingFieldNames, boolean utcTimezone) {
    return new OrderingValueEngineTypeConverter(dataSchema, orderingFieldNames, utcTimezone);
  }
}
