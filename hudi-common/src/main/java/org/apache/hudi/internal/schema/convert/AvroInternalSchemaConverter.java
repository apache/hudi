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

package org.apache.hudi.internal.schema.convert;

import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.hudi.internal.schema.HoodieSchemaException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Type;
import org.apache.hudi.internal.schema.Types;

import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.avro.Schema.Type.UNION;

/**
 * Auxiliary class.
 * Converts an avro schema into InternalSchema, or convert InternalSchema to an avro schema
 */
public class AvroInternalSchemaConverter {

  /**
   * Convert internalSchema to avro Schema.
   *
   * @param internalSchema internal schema.
   * @param tableName the record name.
   * @return an avro Schema.
   */
  public static Schema convert(InternalSchema internalSchema, String tableName, String namespace) {
    return buildAvroSchemaFromInternalSchema(internalSchema, tableName, namespace);
  }

  public static Schema convert(InternalSchema internalSchema, String tableName) {
    return buildAvroSchemaFromInternalSchema(internalSchema, tableName, "");
  }

  /**
   * Convert RecordType to avro Schema.
   *
   * @param type internal schema.
   * @param name the record name.
   * @return an avro Schema.
   */
  public static Schema convert(Types.RecordType type, String name) {
    return buildAvroSchemaFromType(type, name);
  }

  /**
   * Convert internal type to avro Schema.
   *
   * @param type internal type.
   * @param name the record name.
   * @return an avro Schema.
   */
  public static Schema convert(Type type, String name) {
    return buildAvroSchemaFromType(type, name);
  }

  /** Convert an avro schema into internal type. */
  public static Type convertToField(Schema schema) {
    return buildTypeFromAvroSchema(schema);
  }

  /** Convert an avro schema into internalSchema. */
  public static InternalSchema convert(Schema schema) {
    List<Types.Field> fields = ((Types.RecordType) convertToField(schema)).fields();
    return new InternalSchema(fields);
  }

  /** Check whether current avro schema is optional?. */
  public static boolean isOptional(Schema schema) {
    if (schema.getType() == UNION && schema.getTypes().size() == 2) {
      return schema.getTypes().get(0).getType() == Schema.Type.NULL || schema.getTypes().get(1).getType() == Schema.Type.NULL;
    }
    return false;
  }

  /** Returns schema with nullable true. */
  public static Schema nullableSchema(Schema schema) {
    if (schema.getType() == UNION) {
      if (!isOptional(schema)) {
        throw new HoodieSchemaException(String.format("Union schemas are not supported: %s", schema));
      }
      return schema;
    } else {
      return Schema.createUnion(Schema.create(Schema.Type.NULL), schema);
    }
  }

  /**
   * Build hudi type from avro schema.
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
   * Converts an avro schema into hudi type.
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
    return visitInternalSchemaToBuildAvroSchema(type, cache, recordName, "");
  }

  /**
   * Converts hudi internal Schema into an Avro Schema.
   *
   * @param schema a hudi internal Schema.
   * @param recordName the record name
   * @return a Avro schema match hudi internal schema.
   */
  public static Schema buildAvroSchemaFromInternalSchema(InternalSchema schema, String recordName, String namespace) {
    Map<Type, Schema> cache = new HashMap<>();
    return visitInternalSchemaToBuildAvroSchema(schema.getRecord(), cache, recordName, namespace);
  }

  /**
   * Converts hudi type into an Avro Schema.
   *
   * @param type a hudi type.
   * @param cache use to cache intermediate convert result to save cost.
   * @param recordName the record name
   * @return a Avro schema match this type
   */
  private static Schema visitInternalSchemaToBuildAvroSchema(
      Type type, Map<Type, Schema> cache, String recordName, String namespace) {
    switch (type.typeId()) {
      case RECORD:
        Types.RecordType record = (Types.RecordType) type;
        List<Schema> schemas = new ArrayList<>();
        record.fields().forEach(f -> {
          Schema tempSchema = visitInternalSchemaToBuildAvroSchema(
              f.type(), cache, recordName + "_" + f.name(), namespace);
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
        recordSchema = visitInternalRecordToBuildAvroRecord(record, schemas, recordName, namespace);
        cache.put(record, recordSchema);
        return recordSchema;
      case ARRAY:
        Types.ArrayType array = (Types.ArrayType) type;
        Schema elementSchema;
        elementSchema = visitInternalSchemaToBuildAvroSchema(array.elementType(), cache, recordName, namespace);
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
        keySchema = visitInternalSchemaToBuildAvroSchema(map.keyType(), cache, recordName, namespace);
        valueSchema = visitInternalSchemaToBuildAvroSchema(map.valueType(), cache, recordName, namespace);
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
  private static Schema visitInternalRecordToBuildAvroRecord(
      Types.RecordType record, List<Schema> fieldSchemas, String recordName, String namespace) {
    List<Types.Field> fields = record.fields();
    List<Schema.Field> avroFields = new ArrayList<>();
    for (int i = 0; i < fields.size(); i++) {
      Types.Field f = fields.get(i);
      Schema.Field field = new Schema.Field(f.name(), fieldSchemas.get(i), f.doc(), f.isOptional() ? JsonProperties.NULL_VALUE : null);
      avroFields.add(field);
    }
    return Schema.createRecord(recordName, null, namespace, false, avroFields);
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
   * Return the minimum number of bytes needed to store a decimal with a give 'precision'.
   * reference from Spark release 3.1 .
   */
  private static int computeMinBytesForPrecision(int precision) {
    int numBytes = 1;
    while (Math.pow(2.0, 8 * numBytes - 1) < Math.pow(10.0, precision)) {
      numBytes += 1;
    }
    return numBytes;
  }
}
