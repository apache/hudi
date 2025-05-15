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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An implementation of {@link RowProjection} which supports schema evolving projection on {@link RowData}.
 */
public class SchemaEvolvingRowDataProjection implements RowProjection {
  private static final long serialVersionUID = 1L;

  private final TypeConverters.TypeConverter innerProjection;

  public SchemaEvolvingRowDataProjection(RowType from, RowType to, Map<String, String> renamedColumns) {
    this.innerProjection = createProjection(from, to, renamedColumns, new ArrayDeque<>());
  }

  public RowData project(RowData rowData) {
    return (RowData) this.innerProjection.convert(rowData);
  }

  public static SchemaEvolvingRowDataProjection instance(RowType from, RowType to, Map<String, String> renamedColumns) {
    return new SchemaEvolvingRowDataProjection(from, to, renamedColumns);
  }

  private TypeConverters.TypeConverter createProjection(LogicalType fromType, LogicalType toType, Map<String, String> renamedColumns, Deque<String> fieldNameStack) {
    LogicalTypeRoot to = toType.getTypeRoot();
    switch (to) {
      case MAP:
        return createMapProjection(fromType, toType, renamedColumns, fieldNameStack);
      case ARRAY:
        return createArrayProjection(fromType, toType, renamedColumns, fieldNameStack);
      case ROW:
        return createRowProjection(fromType, toType, renamedColumns, fieldNameStack);
      default:
        if (fromType.equals(toType)) {
          return TypeConverters.NOOP_CONVERTER;
        } else {
          // return TypeConverter directly for non-composite type
          return o -> Option.ofNullable(TypeConverters.getInstance(fromType, toType))
              .orElseThrow(() -> new HoodieException(String.format("Do not support casting type from: %s to %s.", fromType, toType)))
              .convert(o);
        }
    }
  }

  private TypeConverters.TypeConverter createArrayProjection(LogicalType fromType, LogicalType toType, Map<String, String> renamedColumns, Deque<String> fieldNameStack) {
    final LogicalType elementFromType = fromType.getChildren().get(0);
    final LogicalType elementToType = toType.getChildren().get(0);
    final ArrayData.ElementGetter elementGetter = ArrayData.createElementGetter(elementFromType);
    fieldNameStack.push("element");
    final TypeConverters.TypeConverter elementConverter = createProjection(elementFromType, elementToType, renamedColumns, fieldNameStack);
    fieldNameStack.pop();
    return obj -> {
      ArrayData array = (ArrayData) obj;
      Object[] objects = new Object[array.size()];
      for (int i = 0; i < array.size(); i++) {
        Object fromObj = elementGetter.getElementOrNull(array, i);
        Object toObj = fromObj == null ? null : elementConverter.convert(fromObj);
        objects[i] = toObj;
      }
      return new GenericArrayData(objects);
    };
  }

  private TypeConverters.TypeConverter createMapProjection(LogicalType fromType, LogicalType toType, Map<String, String> renamedColumns, Deque<String> fieldNameStack) {
    // no schema evolution is allowed on the keyType, hence, we only need to care about the valueType
    final LogicalType keyType = fromType.getChildren().get(0);
    final ArrayData.ElementGetter keyElementGetter = ArrayData.createElementGetter(keyType);
    final LogicalType fromValueType = fromType.getChildren().get(1);
    final LogicalType toValueType = toType.getChildren().get(1);
    final ArrayData.ElementGetter valueElementGetter = ArrayData.createElementGetter(fromValueType);
    fieldNameStack.push("value");
    final TypeConverters.TypeConverter valueConverter = createProjection(fromValueType, toValueType, renamedColumns, fieldNameStack);
    fieldNameStack.pop();

    return obj -> {
      final Map<Object, Object> result = new HashMap<>();
      MapData map = (MapData) obj;
      for (int i = 0; i < map.size(); i++) {
        Object keyObj = keyElementGetter.getElementOrNull(map.keyArray(), i);
        Object fromObj = valueElementGetter.getElementOrNull(map.valueArray(), i);
        // need to handle nulls to prevent NullPointerException in #getConversion()
        Object toObj = fromObj == null ? null : valueConverter.convert(fromObj);
        result.put(keyObj, toObj);
      }
      return new GenericMapData(result);
    };
  }

  private TypeConverters.TypeConverter createRowProjection(LogicalType fromType, LogicalType toType, Map<String, String> renamedColumns, Deque<String> fieldNameStack) {
    RowType fromRowType = (RowType) fromType;
    RowType toRowType = (RowType) toType;
    List<String> fromFieldNames = fromRowType.getFieldNames();
    // index in `from` type for fields of `to`, -1 if the field is new added field, as it does not exist in `from`
    RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[toRowType.getFieldCount()];
    TypeConverters.TypeConverter[] objectConverters = new TypeConverters.TypeConverter[toRowType.getFieldCount()];
    for (int i = 0; i < toRowType.getFieldCount(); i++) {
      RowField field = toRowType.getFields().get(i);
      fieldNameStack.push(field.getName());
      // trying to get origin name for the field, if not found in mapping, no renaming happens
      String fieldName = renamedColumns.getOrDefault(HoodieAvroUtils.createFullName(fieldNameStack), field.getName());
      int fieldIdxInFrom = fromFieldNames.indexOf(fieldName);
      // this is a new added column
      if (fieldIdxInFrom == -1) {
        fieldGetters[i] = RowDataUtils.NULL_GETTER;
        objectConverters[i] = TypeConverters.NOOP_CONVERTER;
      } else {
        fieldGetters[i] = RowData.createFieldGetter(fromRowType.getTypeAt(fieldIdxInFrom), fieldIdxInFrom);
        final LogicalType fromFieldType = fromRowType.getTypeAt(fieldIdxInFrom);
        final LogicalType toFieldType = toRowType.getTypeAt(i);
        objectConverters[i] = createProjection(fromFieldType, toFieldType, renamedColumns, fieldNameStack);
      }
      fieldNameStack.pop();
    }
    return obj -> {
      RowData rowData = (RowData) obj;
      GenericRowData result = new GenericRowData(toRowType.getFieldCount());
      result.setRowKind(rowData.getRowKind());
      for (int i = 0; i < fieldGetters.length; i++) {
        Object fieldVal = fieldGetters[i].getFieldOrNull(rowData);
        final Object val = fieldVal == null ? null : objectConverters[i].convert(fieldVal);
        result.setField(i, val);
      }
      return result;
    };
  }
}
