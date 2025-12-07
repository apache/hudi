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
import org.apache.hudi.common.util.collection.Triple;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.util.AvroToRowDataConverters.AvroToRowDataConverter;
import org.apache.hudi.util.RowDataToAvroConverters.RowDataToAvroConverter;

import lombok.Getter;
import org.apache.avro.Schema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Maintains auxiliary utilities for row data fields handling.
 */
public class RowDataAvroQueryContexts {
  private static final Map<Pair<Schema, Boolean>, RowDataQueryContext> QUERY_CONTEXT_MAP = new ConcurrentHashMap<>();

  // BinaryRowWriter in RowDataSerializer are reused, and it's not thread-safe.
  private static final ThreadLocal<Map<Schema, RowDataSerializer>> ROWDATA_SERIALIZER_CACHE = ThreadLocal.withInitial(HashMap::new);

  private static final Map<Triple<Schema, Schema, Map<String, String>>, RowProjection> ROW_PROJECTION_CACHE = new ConcurrentHashMap<>();

  public static RowDataQueryContext fromAvroSchema(Schema avroSchema) {
    return fromAvroSchema(avroSchema, true);
  }

  public static RowDataQueryContext fromAvroSchema(Schema avroSchema, boolean utcTimezone) {
    return QUERY_CONTEXT_MAP.computeIfAbsent(Pair.of(avroSchema, utcTimezone), k -> {
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
      AvroToRowDataConverter avroToRowDataConverter = AvroToRowDataConverters.createRowConverter(rowType, utcTimezone);
      return RowDataQueryContext.create(dataType, contextMap, fieldGetters, rowDataToAvroConverter, avroToRowDataConverter);
    });
  }

  public static RowDataSerializer getRowDataSerializer(Schema avroSchema) {
    return ROWDATA_SERIALIZER_CACHE.get().computeIfAbsent(avroSchema, schema -> {
      RowType rowType = (RowType) fromAvroSchema(schema).getRowType().getLogicalType();
      return new RowDataSerializer(rowType);
    });
  }

  public static RowProjection getRowProjection(Schema from, Schema to, Map<String, String> renameCols) {
    Triple<Schema, Schema, Map<String, String>> cacheKey = Triple.of(from, to, renameCols);
    return ROW_PROJECTION_CACHE.computeIfAbsent(cacheKey, key -> {
      RowType fromType = (RowType) RowDataAvroQueryContexts.fromAvroSchema(from).getRowType().getLogicalType();
      RowType toType =  (RowType) RowDataAvroQueryContexts.fromAvroSchema(to).getRowType().getLogicalType();
      return SchemaEvolvingRowDataProjection.instance(fromType, toType, renameCols);
    });
  }

  public static class RowDataQueryContext {
    @Getter
    private final DataType rowType;
    private final Map<String, FieldQueryContext> contextMap;
    private final RowData.FieldGetter[] fieldGetters;
    @Getter
    private final RowDataToAvroConverter rowDataToAvroConverter;
    @Getter
    private final AvroToRowDataConverter avroToRowDataConverter;

    private RowDataQueryContext(
        DataType rowType,
        Map<String, FieldQueryContext> contextMap,
        RowData.FieldGetter[] fieldGetters,
        RowDataToAvroConverter rowDataAvroConverter,
        AvroToRowDataConverter avroToRowDataConverter) {
      this.rowType = rowType;
      this.contextMap = contextMap;
      this.fieldGetters = fieldGetters;
      this.rowDataToAvroConverter = rowDataAvroConverter;
      this.avroToRowDataConverter = avroToRowDataConverter;
    }

    public static RowDataQueryContext create(
        DataType rowType,
        Map<String, FieldQueryContext> contextMap,
        RowData.FieldGetter[] fieldGetters,
        RowDataToAvroConverter rowDataToAvroConverter,
        AvroToRowDataConverter avroToRowDataConverter) {
      return new RowDataQueryContext(rowType, contextMap, fieldGetters, rowDataToAvroConverter, avroToRowDataConverter);
    }

    public FieldQueryContext getFieldQueryContext(String fieldName) {
      return contextMap.get(fieldName);
    }

    public RowData.FieldGetter[] fieldGetters() {
      return fieldGetters;
    }
  }

  public static class FieldQueryContext {
    @Getter
    private final LogicalType logicalType;
    @Getter
    private final RowData.FieldGetter fieldGetter;
    private final Function<Object, Object> javaTypeConverter;
    private FieldQueryContext(LogicalType logicalType, RowData.FieldGetter fieldGetter, boolean utcTimezone) {
      this.logicalType = logicalType;
      this.fieldGetter = fieldGetter;
      this.javaTypeConverter = RowDataUtils.javaValFunc(logicalType, utcTimezone);
    }

    public static FieldQueryContext create(LogicalType logicalType, RowData.FieldGetter fieldGetter, boolean utcTimezone) {
      return new FieldQueryContext(logicalType, fieldGetter, utcTimezone);
    }

    public Object getValAsJava(RowData rowData) {
      return getValAsJava(rowData, true);
    }

    public Object getValAsJava(RowData rowData, boolean allowsNull) {
      Object val = this.javaTypeConverter.apply(fieldGetter.getFieldOrNull(rowData));
      if (val == null && !allowsNull) {
        throw new HoodieException("The field value can not be null");
      }
      return val;
    }
  }
}