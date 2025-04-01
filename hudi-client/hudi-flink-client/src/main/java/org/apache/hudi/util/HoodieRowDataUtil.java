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

package org.apache.hudi.util;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.util.RowDataToAvroConverters.RowDataToAvroConverter;

import org.apache.avro.Schema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

/**
 * Utils for get/set operations on {@link RowData}.
 */
public class HoodieRowDataUtil {
  // key: <record_schema, field_name>
  private static final Map<Pair<Schema, String>, RowData.FieldGetter> FIELD_GETTER_CACHE = new ConcurrentHashMap<>();
  private static final Map<Pair<Schema, String>, UnaryOperator<Object>> FIELD_CONVERTER_CACHE = new ConcurrentHashMap<>();

  /**
   * Utils to get FieldGetter from cache.
   *
   * @param schema schema of record
   * @param fieldName name of the field
   * @return FieldGetter from cache or newly created one if not existed in cache.
   */
  public static Option<RowData.FieldGetter> getFieldGetter(Schema schema, String fieldName) {
    Pair<Schema, String> cacheKey = Pair.of(schema, fieldName);
    RowData.FieldGetter fieldGetter = FIELD_GETTER_CACHE.get(cacheKey);
    if (fieldGetter == null) {
      Schema.Field field = schema.getField(fieldName);
      if (field == null) {
        return Option.empty();
      }
      int fieldPos = schema.getFields().stream().map(Schema.Field::name).collect(Collectors.toList()).indexOf(fieldName);
      LogicalType fieldType = AvroSchemaConverter.convertToDataType(field.schema()).getLogicalType();
      fieldGetter = RowData.createFieldGetter(fieldType, fieldPos);
      FIELD_GETTER_CACHE.put(cacheKey, fieldGetter);
    }
    return Option.of(fieldGetter);
  }

  /**
   * Utils to get RowData to Avro value converter from cache.
   *
   * @param schema schema of record
   * @param fieldName name of the field
   * @param conf flink configuration
   * @return RowData converter from cache or newly created one if not existed in cache.
   */
  public static UnaryOperator<Object> getFieldConverter(Schema schema, String fieldName, Configuration conf) {
    Pair<Schema, String> cacheKey = Pair.of(schema, fieldName);
    UnaryOperator<Object> fieldConverter = FIELD_CONVERTER_CACHE.get(cacheKey);
    if (fieldConverter == null) {
      Schema.Field field = schema.getField(fieldName);
      if (field == null) {
        throw new HoodieException(String.format("Field %s is not in schema: %s.", fieldName, schema));
      }
      LogicalType fieldType = AvroSchemaConverter.convertToDataType(field.schema()).getLogicalType();
      RowDataToAvroConverter avroConverter = RowDataToAvroConverters.createConverter(
          fieldType, conf.getBoolean("read.utc-timezone", true));
      fieldConverter = new UnaryOperator<Object>() {
        @Override
        public Object apply(Object val) {
          return HoodieAvroUtils.convertValueForSpecificDataTypes(
              field.schema(), avroConverter.convert(field.schema(), val), false);
        }
      };
      FIELD_CONVERTER_CACHE.put(cacheKey, fieldConverter);
    }
    return fieldConverter;
  }
}
