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

import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.internal.schema.HoodieSchemaException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Type;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.internal.schema.Types.Field;
import org.apache.hudi.internal.schema.Types.RecordType;
import org.apache.hudi.internal.schema.action.SchemaMerger;
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter;
import org.apache.hudi.internal.schema.visitor.InternalSchemaVisitor;
import org.apache.hudi.internal.schema.visitor.NameToIDVisitor;

import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class InternalSchemaUtils {

  private InternalSchemaUtils() {
  }

  /**
   * build a mapping from id to full field name for a internal Type.
   * if a field y belong to a struct filed x, then the full name of y is x.y
   *
   * @param type hoodie internal type
   * @return a mapping from id to full field name
   */
  public static Map<Integer, String> buildIdToName(Type type) {
    Map<Integer, String> result = new HashMap<>();
    buildNameToId(type).forEach((k, v) -> result.put(v, k));
    return result;
  }

  /**
   * build a mapping from full field name to id for a internal Type.
   * if a field y belong to a struct filed x, then the full name of y is x.y
   *
   * @param type hoodie internal type
   * @return a mapping from full field name to id
   */
  public static Map<String, Integer> buildNameToId(Type type) {
    return visit(type, new NameToIDVisitor());
  }

  /**
   * use to traverse all types in internalSchema with visitor.
   *
   * @param schema hoodie internal schema
   * @return vistor expected result.
   */
  public static <T> T visit(InternalSchema schema, InternalSchemaVisitor<T> visitor) {
    return visitor.schema(schema, visit(schema.getRecord(), visitor));
  }

  public static <T> T visit(Type type, InternalSchemaVisitor<T> visitor) {
    switch (type.typeId()) {
      case RECORD:
        RecordType record = (RecordType) type;
        List<T> results = new ArrayList<>();
        for (Types.Field f : record.fields()) {
          visitor.beforeField(f);
          T result;
          try {
            result = visit(f.type(), visitor);
          } finally {
            visitor.afterField(f);
          }
          results.add(visitor.field(f, result));
        }
        return visitor.record(record, results);
      case ARRAY:
        Types.ArrayType array = (Types.ArrayType) type;
        T elementResult;
        Types.Field elementField = array.field(array.elementId());
        visitor.beforeArrayElement(elementField);
        try {
          elementResult = visit(elementField.type(), visitor);
        } finally {
          visitor.afterArrayElement(elementField);
        }
        return visitor.array(array, elementResult);
      case MAP:
        Types.MapType map = (Types.MapType) type;
        T keyResult;
        T valueResult;
        Types.Field keyField = map.field(map.keyId());
        visitor.beforeMapKey(keyField);
        try {
          keyResult = visit(map.keyType(), visitor);
        } finally {
          visitor.afterMapKey(keyField);
        }
        Types.Field valueField = map.field(map.valueId());
        visitor.beforeMapValue(valueField);
        try {
          valueResult = visit(map.valueType(), visitor);
        } finally {
          visitor.afterMapValue(valueField);
        }
        return visitor.map(map, keyResult, valueResult);
      default:
        return visitor.primitive((Type.PrimitiveType)type);
    }
  }

  /**
   * build a mapping from id to field for a internal Type.
   *
   * @param type hoodie internal type
   * @return a mapping from id to field
   */
  public static Map<Integer, Field> buildIdToField(Type type) {
    Map<Integer, Field> idToField = new HashMap<>();
    visitIdToField(type, idToField);
    return idToField;
  }

  private static void visitIdToField(Type type, Map<Integer, Field> index) {
    switch (type.typeId()) {
      case RECORD:
        RecordType record = (RecordType) type;
        for (Field field : record.fields()) {
          visitIdToField(field.type(), index);
          index.put(field.fieldId(), field);
        }
        return;
      case ARRAY:
        Types.ArrayType array = (Types.ArrayType) type;
        visitIdToField(array.elementType(), index);
        for (Field field : array.fields()) {
          index.put(field.fieldId(), field);
        }
        return;
      case MAP:
        Types.MapType map = (Types.MapType) type;
        visitIdToField(map.keyType(), index);
        visitIdToField(map.valueType(), index);
        for (Field field : map.fields()) {
          index.put(field.fieldId(), field);
        }
        return;
      default:
        return;
    }
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
   * build a mapping which maintain the relation between child field id and it's parent field id.
   * if a child field y(which id is 9) belong to a nest field x(which id is 6), then (9 -> 6) will be added to the result map.
   * if a field has no parent field, nothings will be added.
   *
   * @param record hoodie record type.
   * @return a mapping from id to parentId for a record Type
   */
  public static Map<Integer, Integer> index2Parents(Types.RecordType record) {
    Map<Integer, Integer> result = new HashMap<>();
    Deque<Integer> parentIds = new LinkedList<>();
    index2Parents(record, parentIds, result);
    return result;
  }

  private static void index2Parents(Type type, Deque<Integer> pids, Map<Integer, Integer> id2p) {
    switch (type.typeId()) {
      case RECORD:
        Types.RecordType record = (Types.RecordType)type;
        for (Field f : record.fields()) {
          pids.push(f.fieldId());
          index2Parents(f.type(), pids, id2p);
          pids.pop();
        }

        for (Field f : record.fields()) {
          // root record has no parent id.
          if (!pids.isEmpty()) {
            Integer pid = pids.peek();
            id2p.put(f.fieldId(), pid);
          }
        }
        return;
      case ARRAY:
        Types.ArrayType array = (Types.ArrayType) type;
        Types.Field elementField = array.field(array.elementId());
        pids.push(elementField.fieldId());
        index2Parents(elementField.type(), pids, id2p);
        pids.pop();
        id2p.put(array.elementId(), pids.peek());
        return;
      case MAP:
        Types.MapType map = (Types.MapType) type;
        Types.Field keyField = map.field(map.keyId());
        Types.Field valueField = map.field(map.valueId());
        // visit key
        pids.push(map.keyId());
        index2Parents(keyField.type(), pids, id2p);
        pids.pop();
        // visit value
        pids.push(map.valueId());
        index2Parents(valueField.type(), pids, id2p);
        pids.pop();
        id2p.put(map.keyId(), pids.peek());
        id2p.put(map.valueId(), pids.peek());
        return;
      default:
    }
  }

  /**
   * Assigns new ids for all fields in a Type, based on initial id.
   *
   * @param type a type.
   * @param nextId initial id which used to fresh ids for all fields in a type
   * @return a new type with new ids
   */
  public static Type refreshNewId(Type type, AtomicInteger nextId) {
    switch (type.typeId()) {
      case RECORD:
        RecordType record = (RecordType) type;
        List<Field> oldFields = record.fields();
        List<Type> fieldTypes = new ArrayList<>();
        int currentId = nextId.get();
        nextId.set(currentId + record.fields().size());
        for (Types.Field f : oldFields) {
          fieldTypes.add(refreshNewId(f.type(), nextId));
        }
        List<Types.Field> internalFields = new ArrayList<>();
        for (int i = 0; i < oldFields.size(); i++) {
          Field oldField = oldFields.get(i);
          internalFields.add(Types.Field.get(currentId++, oldField.isOptional(), oldField.name(), fieldTypes.get(i), oldField.doc()));
        }
        return Types.RecordType.get(internalFields);
      case ARRAY:
        Types.ArrayType array = (Types.ArrayType) type;
        int elementId = nextId.get();
        nextId.set(elementId + 1);
        Type elementType = refreshNewId(array.elementType(), nextId);
        return Types.ArrayType.get(elementId, array.isElementOptional(), elementType);
      case MAP:
        Types.MapType map = (Types.MapType) type;
        int keyId = nextId.get();
        int valueId = keyId + 1;
        nextId.set(keyId + 2);
        Type keyType = refreshNewId(map.keyType(), nextId);
        Type valueType = refreshNewId(map.valueType(), nextId);
        return Types.MapType.get(keyId, valueId, keyType, valueType, map.isValueOptional());
      default:
        return type;
    }
  }

  /**
   * build hudi type from avro schema.
   *
   * @param schema a avro schema.
   * @return a hudi type.
   */
  public static Type buildTypeFromAvroSchema(Schema schema) {
    // set flag to check this has not been visited.
    Deque<String> visited = new LinkedList();
    AtomicInteger nextId = new AtomicInteger(1);
    return visitAvroSchemaToBuildType(schema, visited, true, nextId);
  }

  /**
   * converts an avro schema into hudi type.
   *
   * @param schema a avro schema.
   * @param visited track the visit node when do traversal for avro schema; used to check if the name of avro record schema is correct.
   * @param firstVisitRoot track whether the current visited schema node is a root node.
   * @param nextId a initial id which used to create id for all fields.
   * @return a hudi type match avro schema.
   */
  private static Type visitAvroSchemaToBuildType(Schema schema, Deque<String> visited, Boolean firstVisitRoot, AtomicInteger nextId) {
    switch (schema.getType()) {
      case RECORD:
        String name = schema.getFullName();
        if (visited.contains(name)) {
          throw new HoodieSchemaException(String.format("cannot convert recursive avro record %s", name));
        }
        visited.push(name);
        List<Schema.Field> fields = schema.getFields();
        List<Type> fieldTypes = new ArrayList<>(fields.size());
        int nextAssignId = nextId.get();
        // when first visit root record, set nextAssignId = 0;
        if (firstVisitRoot) {
          nextAssignId = 0;
        }
        nextId.set(nextAssignId + fields.size());
        fields.stream().forEach(field -> {
          fieldTypes.add(visitAvroSchemaToBuildType(field.schema(), visited, false, nextId));
        });
        visited.pop();
        List<Types.Field> internalFields = new ArrayList<>(fields.size());

        for (int i  = 0; i < fields.size(); i++) {
          Schema.Field field = fields.get(i);
          Type fieldType = fieldTypes.get(i);
          internalFields.add(Types.Field.get(nextAssignId, AvroInternalSchemaConverter.isOptional(field.schema()), field.name(), fieldType, field.doc()));
          nextAssignId += 1;
        }
        return Types.RecordType.get(internalFields);
      case UNION:
        List<Type> fTypes = new ArrayList<>();
        schema.getTypes().stream().forEach(t -> {
          fTypes.add(visitAvroSchemaToBuildType(t, visited, false, nextId));
        });
        return fTypes.get(0) == null ? fTypes.get(1) : fTypes.get(0);
      case ARRAY:
        Schema elementSchema = schema.getElementType();
        int elementId = nextId.get();
        nextId.set(elementId + 1);
        Type elementType = visitAvroSchemaToBuildType(elementSchema, visited, false, nextId);
        return Types.ArrayType.get(elementId, AvroInternalSchemaConverter.isOptional(schema.getElementType()), elementType);
      case MAP:
        int keyId = nextId.get();
        int valueId = keyId + 1;
        nextId.set(valueId + 1);
        Type valueType = visitAvroSchemaToBuildType(schema.getValueType(),  visited, false, nextId);
        return Types.MapType.get(keyId, valueId, Types.StringType.get(), valueType, AvroInternalSchemaConverter.isOptional(schema.getValueType()));
      default:
        return visitAvroPrimitiveToBuildInternalType(schema);
    }
  }

  private static Type visitAvroPrimitiveToBuildInternalType(Schema primitive) {
    LogicalType logical = primitive.getLogicalType();
    if (logical != null) {
      String name = logical.getName();
      if (logical instanceof LogicalTypes.Decimal) {
        return Types.DecimalType.get(
            ((LogicalTypes.Decimal) logical).getPrecision(),
            ((LogicalTypes.Decimal) logical).getScale());

      } else if (logical instanceof LogicalTypes.Date) {
        return Types.DateType.get();

      } else if (
          logical instanceof LogicalTypes.TimeMillis
              || logical instanceof LogicalTypes.TimeMicros) {
        return Types.TimeType.get();

      } else if (
          logical instanceof LogicalTypes.TimestampMillis
              || logical instanceof LogicalTypes.TimestampMicros) {
        return Types.TimestampType.get();
      } else if (LogicalTypes.uuid().getName().equals(name)) {
        return Types.UUIDType.get();
      }
    }

    switch (primitive.getType()) {
      case BOOLEAN:
        return Types.BooleanType.get();
      case INT:
        return Types.IntType.get();
      case LONG:
        return Types.LongType.get();
      case FLOAT:
        return Types.FloatType.get();
      case DOUBLE:
        return Types.DoubleType.get();
      case STRING:
      case ENUM:
        return Types.StringType.get();
      case FIXED:
        return Types.FixedType.getFixed(primitive.getFixedSize());
      case BYTES:
        return Types.BinaryType.get();
      case NULL:
        return null;
      default:
        throw new UnsupportedOperationException("Unsupported primitive type: " + primitive);
    }
  }

  /**
   * Converts hudi type into an Avro Schema.
   *
   * @param type a hudi type.
   * @param recordName the record name
   * @return a Avro schema match this type
   */
  public static Schema buildAvroSchemaFromType(Type type, String recordName) {
    Map<Type, Schema> cache = new HashMap<>();
    return visitInternalSchemaToBuildAvroSchema(type, cache, recordName);
  }

  /**
   * Converts hudi internal Schema into an Avro Schema.
   *
   * @param schema a hudi internal Schema.
   * @param recordName the record name
   * @return a Avro schema match hudi internal schema.
   */
  public static Schema buildAvroSchemaFromInternalSchema(InternalSchema schema, String recordName) {
    Map<Type, Schema> cache = new HashMap<>();
    return visitInternalSchemaToBuildAvroSchema(schema.getRecord(), cache, recordName);
  }

  /**
   * Converts hudi type into an Avro Schema.
   *
   * @param type a hudi type.
   * @param cache use to cache intermediate convert result to save cost.
   * @param recordName the record name
   * @return a Avro schema match this type
   */
  private static Schema visitInternalSchemaToBuildAvroSchema(Type type, Map<Type, Schema> cache, String recordName) {
    switch (type.typeId()) {
      case RECORD:
        Types.RecordType record = (Types.RecordType) type;
        List<Schema> schemas = new ArrayList<>();
        record.fields().forEach(f -> {
          Schema tempSchema = visitInternalSchemaToBuildAvroSchema(f.type(), cache, recordName + "_" + f.name());
          // convert tempSchema
          Schema result = f.isOptional() ? AvroInternalSchemaConverter.nullableSchema(tempSchema) : tempSchema;
          schemas.add(result);
        });
        // check visited
        Schema recordSchema;
        recordSchema = cache.get(record);
        if (recordSchema != null) {
          return recordSchema;
        }
        recordSchema = visitInternalRecordToBuildAvroRecord(record, schemas, recordName);
        cache.put(record, recordSchema);
        return recordSchema;
      case ARRAY:
        Types.ArrayType array = (Types.ArrayType) type;
        Schema elementSchema;
        elementSchema = visitInternalSchemaToBuildAvroSchema(array.elementType(), cache, recordName);
        Schema arraySchema;
        arraySchema = cache.get(array);
        if (arraySchema != null) {
          return arraySchema;
        }
        arraySchema = visitInternalArrayToBuildAvroArray(array, elementSchema);
        cache.put(array, arraySchema);
        return arraySchema;
      case MAP:
        Types.MapType map = (Types.MapType) type;
        Schema keySchema;
        Schema valueSchema;
        keySchema = visitInternalSchemaToBuildAvroSchema(map.keyType(), cache, recordName);
        valueSchema = visitInternalSchemaToBuildAvroSchema(map.valueType(), cache, recordName);
        Schema mapSchema;
        mapSchema = cache.get(map);
        if (mapSchema != null) {
          return mapSchema;
        }
        mapSchema = visitInternalMapToBuildAvroMap(map, keySchema, valueSchema);
        cache.put(map, mapSchema);
        return mapSchema;
      default:
        Schema primitiveSchema = visitInternalPrimitiveToBuildAvroPrimitiveType((Type.PrimitiveType) type);
        cache.put(type, primitiveSchema);
        return primitiveSchema;
    }
  }

  /**
   * Converts hudi RecordType to Avro RecordType.
   * this is auxiliary function used by visitInternalSchemaToBuildAvroSchema
   */
  private static Schema visitInternalRecordToBuildAvroRecord(Types.RecordType record, List<Schema> fieldSchemas, String recordName) {
    List<Field> fields = record.fields();
    List<Schema.Field> avroFields = new ArrayList<>();
    for (int i = 0; i < fields.size(); i++) {
      Field f = fields.get(i);
      Schema.Field field = new Schema.Field(f.name(), fieldSchemas.get(i), f.doc(), f.isOptional() ? JsonProperties.NULL_VALUE : null);
      avroFields.add(field);
    }
    return Schema.createRecord(recordName, null, null, false, avroFields);
  }

  /**
   * Converts hudi ArrayType to Avro ArrayType.
   * this is auxiliary function used by visitInternalSchemaToBuildAvroSchema
   */
  private static Schema visitInternalArrayToBuildAvroArray(Types.ArrayType array, Schema elementSchema) {
    Schema result;
    if (array.isElementOptional()) {
      result = Schema.createArray(AvroInternalSchemaConverter.nullableSchema(elementSchema));
    } else {
      result = Schema.createArray(elementSchema);
    }
    return result;
  }

  /**
   * Converts hudi MapType to Avro MapType.
   * this is auxiliary function used by visitInternalSchemaToBuildAvroSchema
   */
  private static Schema visitInternalMapToBuildAvroMap(Types.MapType map, Schema keySchema, Schema valueSchema) {
    Schema mapSchema;
    if (keySchema.getType() == Schema.Type.STRING) {
      mapSchema = Schema.createMap(map.isValueOptional() ? AvroInternalSchemaConverter.nullableSchema(valueSchema) : valueSchema);
    } else {
      throw new HoodieSchemaException("only support StringType key for avro MapType");
    }
    return mapSchema;
  }

  /**
   * Converts hudi PrimitiveType to Avro PrimitiveType.
   * this is auxiliary function used by visitInternalSchemaToBuildAvroSchema
   */
  private static Schema visitInternalPrimitiveToBuildAvroPrimitiveType(Type.PrimitiveType primitive) {
    Schema primitiveSchema;
    switch (primitive.typeId()) {
      case BOOLEAN:
        primitiveSchema = Schema.create(Schema.Type.BOOLEAN);
        break;
      case INT:
        primitiveSchema = Schema.create(Schema.Type.INT);
        break;
      case LONG:
        primitiveSchema = Schema.create(Schema.Type.LONG);
        break;
      case FLOAT:
        primitiveSchema = Schema.create(Schema.Type.FLOAT);
        break;
      case DOUBLE:
        primitiveSchema = Schema.create(Schema.Type.DOUBLE);
        break;
      case DATE:
        primitiveSchema = LogicalTypes.date()
            .addToSchema(Schema.create(Schema.Type.INT));
        break;
      case TIME:
        primitiveSchema = LogicalTypes.timeMicros()
            .addToSchema(Schema.create(Schema.Type.LONG));
        break;
      case TIMESTAMP:
        primitiveSchema = LogicalTypes.timestampMicros()
            .addToSchema(Schema.create(Schema.Type.LONG));
        break;
      case STRING:
        primitiveSchema = Schema.create(Schema.Type.STRING);
        break;
      case UUID:
        primitiveSchema = LogicalTypes.uuid()
            .addToSchema(Schema.createFixed("uuid_fixed", null, null, 16));
        break;
      case FIXED:
        Types.FixedType fixed = (Types.FixedType) primitive;
        primitiveSchema = Schema.createFixed("fixed_" + fixed.getFixedSize(), null, null, fixed.getFixedSize());
        break;
      case BINARY:
        primitiveSchema = Schema.create(Schema.Type.BYTES);
        break;
      case DECIMAL:
        Types.DecimalType decimal = (Types.DecimalType) primitive;
        primitiveSchema = LogicalTypes.decimal(decimal.precision(), decimal.scale())
            .addToSchema(Schema.createFixed(
                "decimal_" + decimal.precision() + "_" + decimal.scale(),
                null, null, computeMinBytesForPrecision(decimal.precision())));
        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported type ID: " + primitive.typeId());
    }
    return primitiveSchema;
  }

  /**
   * return the minimum number of bytes needed to store a decimal with a give 'precision'.
   * reference from Spark release 3.1 .
   */
  private static int computeMinBytesForPrecision(int precision) {
    int numBytes = 1;
    while (Math.pow(2.0, 8 * numBytes - 1) < Math.pow(10.0, precision)) {
      numBytes += 1;
    }
    return numBytes;
  }

  /**
   * create final read schema to read avro/parquet file.
   *
   * @param fileSchema the real schema of avro/parquet file.
   * @param querySchema the query schema which query engine produced.
   * @return read schema to read avro/parquet file.
   */
  public static InternalSchema mergeSchema(InternalSchema fileSchema, InternalSchema querySchema) {
    return mergeSchema(fileSchema, querySchema, true);
  }

  /**
   * create final read schema to read avro/parquet file.
   *
   * @param fileSchema the real schema of avro/parquet file.
   * @param querySchema the query schema which query engine produced.
   * @param mergeRequiredFiledForce now sparksql will change col nullability attribute from optional to required which is a bug.
   *                                if this situation occur and mergeRequiredFiledForce is set to be true,
   *                                just ignore the nullability changes, and merge force.
   * @return read schema to read avro/parquet file.
   */
  public static InternalSchema mergeSchema(InternalSchema fileSchema, InternalSchema querySchema, Boolean mergeRequiredFiledForce) {
    SchemaMerger schemaMerger = new SchemaMerger(fileSchema, querySchema, mergeRequiredFiledForce);
    Types.RecordType record = (Types.RecordType) mergeType(schemaMerger, querySchema.getRecord());
    return new InternalSchema(record.fields());
  }


  /**
   * create final read schema to read avro/parquet file.
   * this is auxiliary function used by mergeSchema.
   */
  private static Type mergeType(SchemaMerger schemaMerger, Type type) {
    switch (type.typeId()) {
      case RECORD:
        Types.RecordType record = (Types.RecordType) type;
        List<Type> newTypes = new ArrayList<>();
        for (Types.Field f : record.fields()) {
          Type newType = mergeType(schemaMerger, f.type());
          newTypes.add(newType);
        }
        return Types.RecordType.get(schemaMerger.buildRecordType(record.fields(), newTypes));
      case ARRAY:
        Types.ArrayType array = (Types.ArrayType) type;
        Type newElementType;
        Types.Field elementField = array.fields().get(0);
        newElementType = mergeType(schemaMerger, elementField.type());
        return schemaMerger.buildArrayType(array, newElementType);
      case MAP:
        Types.MapType map = (Types.MapType) type;
        Type newValueType = mergeType(schemaMerger, map.valueType());
        return schemaMerger.buildMapType(map, newValueType);
      default:
        return type;
    }
  }

  /**
   * create project internalSchema, based on the project names which produced by query engine.
   * support nested project.
   *
   * @param schema a internal schema.
   * @param names project names produced by query engine.
   * @return a project internalSchema.
   */
  public static InternalSchema pruneInternalSchema(InternalSchema schema, List<String> names) {
    // do check
    List<Integer> prunedIds = names.stream().map(name -> schema.findIdByName(name.toLowerCase(Locale.ROOT)))
        .collect(Collectors.toList());
    if (prunedIds.contains(-1)) {
      throw new IllegalArgumentException("cannot prune col which not exisit in hudi table");
    }
    // find top parent field ID. eg: a.b.c, f.g.h, only collect id of a and f ignore all child field.
    List<Integer> topParentFieldIds = new ArrayList<>();
    names.stream().forEach(f -> {
      int id = schema.findIdByName(f.split("\\.")[0].toLowerCase(Locale.ROOT));
      if (!topParentFieldIds.contains(id)) {
        topParentFieldIds.add(id);
      }
    });
    return pruneInternalSchemaByID(schema, prunedIds, topParentFieldIds);
  }

  /**
   * create project internalSchema.
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
          throw new HoodieSchemaException(String.format("cannot find pruned id %s in currentSchema %s", id, schema.toString()));
        }
      }
    }
    return new InternalSchema(newFields.isEmpty() ? recordType.fields() : newFields);
  }

  /**
   * project hudi type by projected cols field_ids
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
          } else if (newType != null) {
            newTypes.add(newType);
          } else {
            newTypes.add(null);
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
   * a helper function to help correct the colName of pushed filters.
   *
   * @param name origin col name from pushed filters.
   * @param fileSchema the real schema of avro/parquet file.
   * @param querySchema the query schema which query engine produced.
   * @return a corrected name.
   */
  public static String reBuildFilterName(String name, InternalSchema fileSchema, InternalSchema querySchema) {
    int nameId = querySchema.findIdByName(name);
    if (nameId == -1) {
      throw new IllegalArgumentException(String.format("cannot found filter col name：%s from querySchema: %s", name, querySchema));
    }
    if (fileSchema.findField(nameId) == null) {
      // added operation found
      // the read file does not contain current col, so current colFilter is invalid
      return "";
    } else {
      if (name.equals(fileSchema.findfullName(nameId))) {
        // no change happened on current col
        return name;
      } else {
        // find rename operation on current col
        // return the name from fileSchema
        return fileSchema.findfullName(nameId);
      }
    }
  }

  /**
   * collect all the type changed cols to build a colPosition -> (newColType, oldColType) map.
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
      if (schema.findType(f) != oldSchema.findType(f)) {
        String[] fieldNameParts = schema.findfullName(f).split("\\.");
        String[] otherFieldNameParts = oldSchema.findfullName(f).split("\\.");
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
}
