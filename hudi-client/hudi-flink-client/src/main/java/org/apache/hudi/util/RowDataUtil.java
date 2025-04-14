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
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.util.RowDataToAvroConverters.RowDataToAvroConverter;

import org.apache.avro.Schema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

/**
 * Utils for get/set operations on {@link RowData}.
 */
public class RowDataUtil {
  // <record_schema, field_name> -> field_getter
  private static final Map<Pair<Schema, String>, RowData.FieldGetter> FIELD_GETTER_CACHE = new ConcurrentHashMap<>();
  // <record_schema, field_name> -> field_converter
  private static final Map<Pair<Schema, String>, UnaryOperator<Object>> FIELD_CONVERTER_CACHE = new ConcurrentHashMap<>();
  // record_schema -> field_converter[]
  private static final Map<Schema, RowData.FieldGetter[]> ALL_FIELD_GETTERS_CACHE = new ConcurrentHashMap<>();

  /**
   * An implementation of {@code FieldGetter} which always return NULL.
   */
  public static final RowData.FieldGetter NULL_GETTER = new RowData.FieldGetter() {
    private static final long serialVersionUID = 1L;

    @Override
    public @Nullable Object getFieldOrNull(RowData rowData) {
      return null;
    }
  };

  /**
   * Utils to get FieldGetter from cache.
   *
   * @param schema schema of record
   * @param fieldName name of the field
   * @return FieldGetter from cache or newly created one if not existed in cache.
   */
  public static RowData.FieldGetter internFieldGetter(Schema schema, String fieldName) {
    Schema.Field field = schema.getField(fieldName);
    if (field == null) {
      return NULL_GETTER;
    }
    Pair<Schema, String> cacheKey = Pair.of(schema, fieldName);
    return FIELD_GETTER_CACHE.computeIfAbsent(cacheKey, key -> {
      int fieldPos = schema.getFields().stream().map(Schema.Field::name).collect(Collectors.toList()).indexOf(fieldName);
      LogicalType fieldType = AvroSchemaConverter.convertToDataType(field.schema()).getLogicalType();
      return RowData.createFieldGetter(fieldType, fieldPos);
    });
  }

  /**
   * Utils to get RowData to Avro value converter from cache.
   *
   * @param schema schema of record
   * @param fieldName name of the field
   * @param useUTCTimezone whether to use UTC timezone to create converter
   * @return RowData converter from cache or newly created one if not existed in cache.
   */
  public static UnaryOperator<Object> internFieldConverter(Schema schema, String fieldName, boolean useUTCTimezone) {
    Pair<Schema, String> cacheKey = Pair.of(schema, fieldName);
    return FIELD_CONVERTER_CACHE.computeIfAbsent(cacheKey, key -> {
      Schema.Field field = schema.getField(fieldName);
      if (field == null) {
        throw new HoodieException(String.format("Field: %s does not exist in Schema: %s", fieldName, schema));
      }
      LogicalType fieldType = AvroSchemaConverter.convertToDataType(field.schema()).getLogicalType();
      RowDataToAvroConverter avroConverter = RowDataToAvroConverters.createConverter(fieldType, useUTCTimezone);
      return new UnaryOperator<Object>() {
        @Override
        public Object apply(Object val) {
          return HoodieAvroUtils.convertValueForSpecificDataTypes(
              field.schema(), avroConverter.convert(field.schema(), val), false);
        }
      };
    });
  }

  /**
   * Utils to get full field getters from cache.
   *
   * @param schema schema of record
   * @return All field getters from cache or newly created one if not existed in cache.
   */
  public static RowData.FieldGetter[] internAllFieldGetters(Schema schema) {
    return ALL_FIELD_GETTERS_CACHE.computeIfAbsent(schema, key -> {
      RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[schema.getFields().size()];
      int idx = 0;
      for (Schema.Field field: schema.getFields()) {
        fieldGetters[idx++] = internFieldGetter(schema, field.name());
      }
      return fieldGetters;
    });
  }
}
