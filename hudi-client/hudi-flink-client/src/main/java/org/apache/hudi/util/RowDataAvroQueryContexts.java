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

import org.apache.avro.Schema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hudi.util.RowDataToAvroConverters.RowDataToAvroConverter;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Maintains auxiliary utilities for row data fields handling.
 */
public class RowDataAvroQueryContexts {
  private static final Map<Schema, RowDataQueryContext> QUERY_CONTEXT_MAP = new ConcurrentHashMap<>();

  public static RowDataQueryContext fromAvroSchema(Schema avroSchema, boolean utcTimezone) {
    return QUERY_CONTEXT_MAP.computeIfAbsent(avroSchema, k -> {
      DataType dataType = AvroSchemaConverter.convertToDataType(avroSchema);
      RowType rowType = (RowType) dataType.getLogicalType();
      RowType.RowField[] rowFields = rowType.getFields().toArray(new RowType.RowField[0]);
      RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[rowFields.length];
      Map<String, FieldQueryContext> contextMap = new HashMap<>();
      for (int i = 0; i < rowFields.length; i++) {
        LogicalType fieldType = rowFields[i].getType();
        RowData.FieldGetter fieldGetter = RowData.createFieldGetter(rowFields[i].getType(), i);
        fieldGetters[i] = fieldGetter;
        contextMap.put(rowFields[i].getName(), FieldQueryContext.create(fieldType, fieldGetter, utcTimezone));
      }
      RowDataToAvroConverter rowDataToAvroConverter = RowDataToAvroConverters.createConverter(rowType, utcTimezone);
      return RowDataQueryContext.create(contextMap, fieldGetters, rowDataToAvroConverter);
    });
  }

  public static class RowDataQueryContext {
    private final Map<String, FieldQueryContext> contextMap;
    private final RowData.FieldGetter[] fieldGetters;
    private final RowDataToAvroConverter rowDataToAvroConverter;
    private RowDataQueryContext(Map<String, FieldQueryContext> contextMap, RowData.FieldGetter[] fieldGetters, RowDataToAvroConverter rowDataAvroConverter) {
      this.contextMap = contextMap;
      this.fieldGetters = fieldGetters;
      this.rowDataToAvroConverter = rowDataAvroConverter;
    }

    public static RowDataQueryContext create(
        Map<String, FieldQueryContext> contextMap,
        RowData.FieldGetter[] fieldGetters,
        RowDataToAvroConverter rowDataToAvroConverter) {
      return new RowDataQueryContext(contextMap, fieldGetters, rowDataToAvroConverter);
    }

    public FieldQueryContext getFieldQueryContext(String fieldName) {
      return contextMap.get(fieldName);
    }

    public RowData.FieldGetter[] fieldGetters() {
      return fieldGetters;
    }

    public RowDataToAvroConverter getRowDataToAvroConverter() {
      return rowDataToAvroConverter;
    }
  }

  public static class FieldQueryContext {
    private final LogicalType logicalType;
    private final RowData.FieldGetter fieldGetter;
    private final Function<Object, Object> javaTypeConverter;
    private FieldQueryContext(LogicalType logicalType, RowData.FieldGetter fieldGetter, boolean utcTimezone) {
      this.logicalType = logicalType;
      this.fieldGetter = fieldGetter;
      this.javaTypeConverter = RowDataUtils.orderingValFunc(logicalType, utcTimezone);
    }

    public static FieldQueryContext create(LogicalType logicalType, RowData.FieldGetter fieldGetter, boolean utcTimezone) {
      return new FieldQueryContext(logicalType, fieldGetter, utcTimezone);
    }

    public LogicalType getLogicalType() {
      return logicalType;
    }

    public RowData.FieldGetter getFieldGetter() {
      return fieldGetter;
    }

    public Object getValAsJava(RowData rowData) {
      return this.javaTypeConverter.apply(fieldGetter.getFieldOrNull(rowData));
    }
  }
}