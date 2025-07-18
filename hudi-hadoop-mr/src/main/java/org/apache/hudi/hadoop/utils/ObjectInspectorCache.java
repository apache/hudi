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

package org.apache.hudi.hadoop.utils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hive.ql.io.parquet.serde.ArrayWritableObjectInspector;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapred.JobConf;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * To read value from an ArrayWritable, an ObjectInspector is needed.
 * Object inspectors are cached here or created using the column type map.
 */
public class ObjectInspectorCache {
  private final Map<String, TypeInfo> columnTypeMap = new HashMap<>();
  private final Map<Schema, ArrayWritableObjectInspector> objectInspectorCache = new HashMap<>();
  private final Map<Schema, HiveAvroSerializer> serializerCache = new HashMap<>();

  public Map<String, TypeInfo> getColumnTypeMap() {
    return columnTypeMap;
  }

  public ObjectInspectorCache(Schema tableSchema, JobConf jobConf) {
    //From AbstractRealtimeRecordReader#prepareHiveAvroSerializer
    // hive will append virtual columns at the end of column list. we should remove those columns.
    // eg: current table is col1, col2, col3; jobConf.get(serdeConstants.LIST_COLUMNS): col1, col2, col3 ,BLOCK__OFFSET__INSIDE__FILE ...
    Set<String> writerSchemaColNames = tableSchema.getFields().stream().map(f -> f.name().toLowerCase(Locale.ROOT)).collect(Collectors.toSet());
    List<String> columnNameList = Arrays.stream(jobConf.get(serdeConstants.LIST_COLUMNS).split(",")).collect(Collectors.toList());
    List<TypeInfo> columnTypeList = TypeInfoUtils.getTypeInfosFromTypeString(jobConf.get(serdeConstants.LIST_COLUMN_TYPES));

    int columnNameListLen = columnNameList.size() - 1;
    for (int i = columnNameListLen; i >= 0; i--) {
      String lastColName = columnNameList.get(columnNameList.size() - 1);
      // virtual columns will only append at the end of column list. it will be ok to break the loop.
      if (writerSchemaColNames.contains(lastColName)) {
        break;
      }
      columnNameList.remove(columnNameList.size() - 1);
      columnTypeList.remove(columnTypeList.size() - 1);
    }

    //Use columnNameList.size() instead of columnTypeList because the type list is longer for some reason
    IntStream.range(0, columnNameList.size()).boxed().forEach(i -> columnTypeMap.put(columnNameList.get(i),
        TypeInfoUtils.getTypeInfosFromTypeString(columnTypeList.get(i).getQualifiedName()).get(0)));

    StructTypeInfo rowTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(columnNameList, columnTypeList);
    ArrayWritableObjectInspector objectInspector = new ArrayWritableObjectInspector(rowTypeInfo);
    objectInspectorCache.put(tableSchema, objectInspector);
  }

  public Object getValue(ArrayWritable record, Schema schema, String fieldName) {
    ArrayWritable currentRecord = record;
    String[] path = fieldName.split("\\.");
    StructField structFieldRef;
    ArrayWritableObjectInspector objectInspector = getObjectInspector(schema);
    for (int i = 0; i < path.length; i++) {
      String field = path[i];
      structFieldRef = objectInspector.getStructFieldRef(field);
      Object value = structFieldRef == null ? null : objectInspector.getStructFieldData(currentRecord, structFieldRef);
      if (i == path.length - 1) {
        return value;
      }
      currentRecord = (ArrayWritable) value;
      objectInspector = (ArrayWritableObjectInspector) structFieldRef.getFieldObjectInspector();
    }
    return null;
  }

  public ArrayWritableObjectInspector getObjectInspector(Schema schema) {
    return objectInspectorCache.computeIfAbsent(schema, s -> {
      List<String> columnNameList = s.getFields().stream().map(Schema.Field::name).map(String::toLowerCase).collect(Collectors.toList());
      List<TypeInfo> columnTypeList = columnNameList.stream().map(columnTypeMap::get).collect(Collectors.toList());
      StructTypeInfo rowTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(columnNameList, columnTypeList);
      return new ArrayWritableObjectInspector(rowTypeInfo);
    });
  }

  public GenericRecord serialize(ArrayWritable record, Schema schema) {
    return serializerCache.computeIfAbsent(schema, s -> {
      List<String> columnNameList = s.getFields().stream().map(Schema.Field::name).map(String::toLowerCase).collect(Collectors.toList());
      List<TypeInfo> columnTypeList = columnNameList.stream().map(columnTypeMap::get).collect(Collectors.toList());
      StructTypeInfo rowTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(columnNameList, columnTypeList);
      return new HiveAvroSerializer(new ArrayWritableObjectInspector(rowTypeInfo), columnNameList, columnTypeList);
    }).serialize(record, schema);
  }
}
