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

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.avro.AvroSchemaUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ArrayComparable;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.unsafe.types.UTF8String;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A converter that converts ordering value from native Java type to Spark type.
 */
public class OrderingValueEngineTypeConverter {
  private final List<Function<Comparable, Comparable>> converters;
  private OrderingValueEngineTypeConverter(Schema dataSchema, List<String> orderingFieldNames) {
    this.converters = createConverters(dataSchema, orderingFieldNames);
  }

  public Comparable convert(Comparable value) {
    return value instanceof ArrayComparable
        ? ((ArrayComparable) value).apply(this.converters)
        : this.converters.get(0).apply(value);
  }

  public static OrderingValueEngineTypeConverter create(Schema dataSchema, List<String> orderingFieldNames) {
    return new OrderingValueEngineTypeConverter(dataSchema, orderingFieldNames);
  }

  private static List<Function<Comparable, Comparable>> createConverters(Schema dataSchema, List<String> orderingFieldNames) {
    if (orderingFieldNames.isEmpty()) {
      return Collections.singletonList(Function.identity());
    }
    return orderingFieldNames.stream().map(f -> {
      Option<Schema> fieldSchemaOpt = AvroSchemaUtils.findNestedFieldSchema(dataSchema, f, true);
      if (fieldSchemaOpt.isEmpty()) {
        return Function.<Comparable>identity();
      } else {
        DataType fieldType = AvroConversionUtils.convertAvroSchemaToDataType(fieldSchemaOpt.get());
        return createConverter(fieldType, fieldSchemaOpt.get());
      }
    }).collect(Collectors.toList());
  }

  public static Function<Comparable, Comparable> createConverter(DataType fieldType, Schema fieldSchema) {
    if (fieldType instanceof TimestampType) {
      LogicalType logicalType = fieldSchema.getLogicalType();
      if (logicalType == null || logicalType instanceof LogicalTypes.TimestampMillis) {
        return comparable -> formatAsMicros((long) comparable);
      }
    } else if (fieldType instanceof StringType) {
      // Spark reads String field values as UTF8String.
      // To foster value comparison, if the value is of String type, e.g., from
      // the delete record, we convert it to UTF8String type.
      return comparable -> comparable instanceof String
          ? SparkAdapterSupport$.MODULE$.sparkAdapter().getUTF8StringFactory().wrapUTF8String(UTF8String.fromString((String) comparable))
          : comparable;
    } else if (fieldType instanceof DecimalType) {
      return comparable -> comparable instanceof BigDecimal ? Decimal.apply((BigDecimal) comparable) : comparable;
    }
    return comparable -> comparable;
  }

  /**
   * Since the value can be either milliseconds or microseconds (from 1.1), we normalize the value into millis' form.
   */
  private static long formatAsMicros(long value) {
    // since millis for 2286/11/21 is 9_999_999_999_999L, the check logic is safe enough.
    if (value <= 9_999_999_999_999L) {
      // convert to microseconds to align with internal representation of spark timestamp
      return value * 1000;
    }
    return value;
  }
}
