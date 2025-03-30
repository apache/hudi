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

import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Utils for get/set operations on {@link RowData}.
 */
public class HoodieRowDataUtil {
  // key: <record_schema, field_name>
  private static final Map<Pair<Schema, String>, RowData.FieldGetter> FIELD_GETTER_CACHE = new ConcurrentHashMap<>();

  public static RowData.FieldGetter getFieldGetter(Schema schema, String fieldName) {
    Pair<Schema, String> cacheKey = Pair.of(schema, fieldName);
    RowData.FieldGetter fieldGetter = FIELD_GETTER_CACHE.get(cacheKey);
    if (fieldGetter == null) {
      Schema fieldSchema = schema.getField(fieldName).schema();
      int fieldPos = schema.getFields().stream().map(Schema.Field::name).collect(Collectors.toList()).indexOf(fieldName);
      LogicalType fieldType = AvroSchemaConverter.convertToDataType(fieldSchema).getLogicalType();
      fieldGetter = RowData.createFieldGetter(fieldType, fieldPos);
      FIELD_GETTER_CACHE.put(cacheKey, fieldGetter);
    }
    return fieldGetter;
  }
}
