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

package org.apache.hudi.internal.schema.utils;

import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.internal.schema.HoodieSchemaException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Type;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.internal.schema.Types.Field;

import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Util methods to help us do some operations on InternalSchema.
 * eg: column prune, filter rebuild for query engine...
 */
public class InternalSchemaUtils {

  private InternalSchemaUtils() {
  }

  /**
   * Create project internalSchema, based on the project names which produced by query engine.
   * support nested project.
   *
   * @param schema a internal schema.
   * @param names project names produced by query engine.
   * @return a project internalSchema.
   */
  public static InternalSchema pruneInternalSchema(InternalSchema schema, List<String> names) {
    // do check
    List<Integer> prunedIds = names.stream().map(name -> {
      int id = schema.findIdByName(name);
      if (id == -1) {
        throw new IllegalArgumentException(String.format("cannot prune col: %s which does not exist in hudi table", name));
      }
      return id;
    }).collect(Collectors.toList());
    // find top parent field ID. eg: a.b.c, f.g.h, only collect id of a and f ignore all child field.
    List<Integer> topParentFieldIds = new ArrayList<>();
    names.stream().forEach(f -> {
      int id = schema.findIdByName(f.split("\\.")[0]);
      if (!topParentFieldIds.contains(id)) {
        topParentFieldIds.add(id);
      }
    });
    return pruneInternalSchemaByID(schema, prunedIds, topParentFieldIds);
  }

  /**
   * Create project internalSchema.
   * support nested project.
   *
   * @param schema a internal schema.
   * @param fieldIds project col field_ids.
   * @return a project internalSchema.
   */
  public static InternalSchema pruneInternalSchemaByID(InternalSchema schema, List<Integer> fieldIds, List<Integer> topParentFieldIds) {
    Types.RecordType recordType = (Types.RecordType)pruneType(schema.getRecord(), fieldIds);
    // reorder top parent fields, since the recordType.fields() produced by pruneType maybe out of order.
    List<Types.Field> newFields = new ArrayList<>();
    if (topParentFieldIds != null && !topParentFieldIds.isEmpty()) {
      for (int id : topParentFieldIds) {
        Types.Field f = recordType.field(id);
        if (f != null) {
          newFields.add(f);
        } else {
          throw new HoodieSchemaException(String.format("cannot find pruned id %s in currentSchema %s", id, schema));
        }
      }
    }
    return new InternalSchema(newFields.isEmpty() ? recordType : Types.RecordType.get(newFields));
  }

  /**
   * Project hudi type by projected cols field_ids
   * this is auxiliary function used by pruneInternalSchema.
   */
  private static Type pruneType(Type type, List<Integer> fieldIds) {
    switch (type.typeId()) {
      case RECORD:
        Types.RecordType record = (Types.RecordType) type;
        List<Types.Field> fields = record.fields();
        List<Type> newTypes = new ArrayList<>();
        for (Types.Field f : fields) {
          Type newType = pruneType(f.type(), fieldIds);
          if (fieldIds.contains(f.fieldId())) {
            newTypes.add(f.type());
          } else {
            newTypes.add(newType);
          }
        }
        boolean changed = false;
        List<Field> newFields = new ArrayList<>();
        for (int i = 0; i < fields.size(); i++) {
          Types.Field oldField = fields.get(i);
          Type newType = newTypes.get(i);
          if (oldField.type() == newType) {
            newFields.add(oldField);
          } else if (newType != null) {
            changed = true;
            newFields.add(Types.Field.get(oldField.fieldId(), oldField.isOptional(), oldField.name(), newType, oldField.doc()));
          }
        }
        if (newFields.isEmpty()) {
          return null;
        }
        if (newFields.size() == fields.size() && !changed) {
          return record;
        } else {
          return Types.RecordType.get(newFields);
        }
      case ARRAY:
        Types.ArrayType array = (Types.ArrayType) type;
        Type newElementType = pruneType(array.elementType(), fieldIds);
        if (fieldIds.contains(array.elementId())) {
          return array;
        } else if (newElementType != null) {
          if (array.elementType() == newElementType) {
            return array;
          }
          return Types.ArrayType.get(array.elementId(), array.isElementOptional(), newElementType);
        }
        return null;
      case MAP:
        Types.MapType map = (Types.MapType) type;
        Type newValueType = pruneType(map.valueType(), fieldIds);
        if (fieldIds.contains(map.valueId())) {
          return map;
        } else if (newValueType != null) {
          if (map.valueType() == newValueType) {
            return map;
          }
          return Types.MapType.get(map.keyId(), map.valueId(), map.keyType(), newValueType, map.isValueOptional());
        }
        return null;
      default:
        return null;
    }
  }

  /**
   * A helper function to help correct the colName of pushed filters.
   *
   * @param name origin col name from pushed filters.
   * @param fileSchema the real schema of avro/parquet file.
   * @param querySchema the query schema which query engine produced.
   * @return a corrected name.
   */
  public static String reBuildFilterName(String name, InternalSchema fileSchema, InternalSchema querySchema) {
    int nameId = querySchema.findIdByName(name);
    if (nameId == -1) {
      throw new IllegalArgumentException(String.format("cannot find filter col name：%s from querySchema: %s", name, querySchema));
    }
    if (fileSchema.findField(nameId) == null) {
      // added operation found
      // the read file does not contain current col, so current colFilter is invalid
      return "";
    } else {
      if (name.equals(fileSchema.findFullName(nameId))) {
        // no change happened on current col
        return name;
      } else {
        // find rename operation on current col
        // return the name from fileSchema
        return fileSchema.findFullName(nameId);
      }
    }
  }

  /**
   * Collect all type changed cols to build a colPosition -> (newColType, oldColType) map.
   * only collect top level col changed. eg: a is a nest field(record(b int, d long), now a.b is changed from int to long,
   * only a will be collected, a.b will excluded.
   *
   * @param schema a type changed internalSchema
   * @param oldSchema an old internalSchema.
   * @return a map.
   */
  public static Map<Integer, Pair<Type, Type>> collectTypeChangedCols(InternalSchema schema, InternalSchema oldSchema) {
    Set<Integer> ids = schema.getAllIds();
    Set<Integer> otherIds = oldSchema.getAllIds();
    Map<Integer, Pair<Type, Type>> result = new HashMap<>();
    ids.stream().filter(f -> otherIds.contains(f)).forEach(f -> {
      if (!schema.findType(f).equals(oldSchema.findType(f))) {
        String[] fieldNameParts = schema.findFullName(f).split("\\.");
        String[] otherFieldNameParts = oldSchema.findFullName(f).split("\\.");
        String parentName = fieldNameParts[0];
        String otherParentName = otherFieldNameParts[0];
        if (fieldNameParts.length == otherFieldNameParts.length && schema.findIdByName(parentName) == oldSchema.findIdByName(otherParentName)) {
          int index = schema.findIdByName(parentName);
          int position = schema.getRecord().fields().stream().map(s -> s.fieldId()).collect(Collectors.toList()).indexOf(index);
          if (!result.containsKey(position)) {
            result.put(position, Pair.of(schema.findType(parentName), oldSchema.findType(otherParentName)));
          }
        }
      }
    });
    return result;
  }

  /**
   * Search target internalSchema by version number.
   *
   * @param versionId the internalSchema version to be search.
   * @param internalSchemas internalSchemas to be searched.
   * @return a internalSchema.
   */
  public static InternalSchema searchSchema(long versionId, List<InternalSchema> internalSchemas) {
    TreeMap<Long, InternalSchema> treeMap = new TreeMap<>();
    internalSchemas.forEach(s -> treeMap.put(s.schemaId(), s));
    return searchSchema(versionId, treeMap);
  }

  /**
   * Search target internalSchema by version number.
   *
   * @param versionId the internalSchema version to be search.
   * @param treeMap internalSchemas collections to be searched.
   * @return a internalSchema.
   */
  public static InternalSchema searchSchema(long versionId, TreeMap<Long, InternalSchema> treeMap) {
    if (treeMap.containsKey(versionId)) {
      return treeMap.get(versionId);
    } else {
      SortedMap<Long, InternalSchema> headMap = treeMap.headMap(versionId);
      if (!headMap.isEmpty()) {
        return headMap.get(headMap.lastKey());
      }
    }
    return InternalSchema.getEmptyInternalSchema();
  }

  public static String createFullName(String name, Deque<String> fieldNames) {
    String result = name;
    if (!fieldNames.isEmpty()) {
      List<String> parentNames = new ArrayList<>();
      fieldNames.descendingIterator().forEachRemaining(parentNames::add);
      result = parentNames.stream().collect(Collectors.joining(".")) + "." + result;
    }
    return result;
  }

  /**
   * Try to find all renamed cols between oldSchema and newSchema.
   *
   * @param oldSchema oldSchema
   * @param newSchema newSchema which modified from oldSchema
   * @return renameCols Map. (k, v) -> (colNameFromNewSchema, colNameLastPartFromOldSchema)
   */
  public static Map<String, String> collectRenameCols(InternalSchema oldSchema, InternalSchema newSchema) {
    List<String> colNamesFromWriteSchema = oldSchema.getAllColsFullName();
    return colNamesFromWriteSchema.stream().filter(f -> {
      int fieldIdFromWriteSchema = oldSchema.findIdByName(f);
      // try to find the cols which has the same id, but have different colName;
      return newSchema.getAllIds().contains(fieldIdFromWriteSchema) && !newSchema.findFullName(fieldIdFromWriteSchema).equalsIgnoreCase(f);
    }).collect(Collectors.toMap(e -> newSchema.findFullName(oldSchema.findIdByName(e)), e -> {
      int lastDotIndex = e.lastIndexOf(".");
      return e.substring(lastDotIndex == -1 ? 0 : lastDotIndex + 1);
    }));
  }
}
