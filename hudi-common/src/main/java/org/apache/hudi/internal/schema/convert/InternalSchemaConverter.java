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

import org.apache.hudi.common.schema.HoodieJsonProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.exception.HoodieNullSchemaTypeException;
import org.apache.hudi.internal.schema.HoodieSchemaException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Type;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.internal.schema.utils.InternalSchemaUtils;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Auxiliary class.
 * Converts a HoodieSchema into InternalSchema, or convert InternalSchema to a HoodieSchema.
 */
public class InternalSchemaConverter {

  // NOTE: We're using dot as field's name delimiter for nested fields
  //       so that Avro is able to interpret qualified name as rather
  //       the combination of the Avro's namespace and actual record's name.
  //       For example qualified nested field's name "trip.fare.amount",
  //       Avro will produce a record with
  //          - Namespace: "trip.fare"
  //          - Name: "amount"
  //
  //        This is crucial aspect of maintaining compatibility b/w schemas, after
  //        converting Avro [[Schema]]s to [[InternalSchema]]s and back
  private static final String FIELD_NAME_DELIMITER = ".";

  /**
   * Convert internalSchema to HoodieSchema.
   *
   * @param internalSchema internal schema.
   * @param name the record name.
   * @return an HoodieSchema.
   */
  public static HoodieSchema convert(InternalSchema internalSchema, String name) {
    return buildHoodieSchemaFromInternalSchema(internalSchema, name);
  }

  public static InternalSchema pruneHoodieSchemaToInternalSchema(HoodieSchema schema, InternalSchema originSchema) {
    List<String> pruneNames = collectColNamesFromSchema(schema);
    return InternalSchemaUtils.pruneInternalSchema(originSchema, pruneNames);
  }

  /**
   * Collect all the leaf nodes names.
   *
   * @param schema a HoodieSchema.
   * @return leaf nodes full names.
   */
  @VisibleForTesting
  static List<String> collectColNamesFromSchema(HoodieSchema schema) {
    List<String> result = new ArrayList<>();
    Deque<String> visited = new LinkedList<>();
    collectColNamesFromHoodieSchema(schema, visited, result);
    return result;
  }

  private static void collectColNamesFromHoodieSchema(HoodieSchema schema, Deque<String> visited, List<String> resultSet) {
    switch (schema.getType()) {
      case RECORD:
        List<HoodieSchemaField> fields = schema.getFields();
        for (HoodieSchemaField f : fields) {
          visited.push(f.name());
          collectColNamesFromHoodieSchema(f.schema(), visited, resultSet);
          visited.pop();
          addFullNameIfLeafNode(f.schema(), f.name(), visited, resultSet);
        }
        return;

      case UNION:
        collectColNamesFromHoodieSchema(schema.getUnderlyingType(), visited, resultSet);
        return;

      case ARRAY:
        visited.push("element");
        collectColNamesFromHoodieSchema(schema.getElementType(), visited, resultSet);
        visited.pop();
        addFullNameIfLeafNode(schema.getElementType(), "element", visited, resultSet);
        return;

      case MAP:
        addFullNameIfLeafNode(HoodieSchemaType.STRING, "key", visited, resultSet);
        visited.push("value");
        collectColNamesFromHoodieSchema(schema.getValueType(), visited, resultSet);
        visited.pop();
        addFullNameIfLeafNode(schema.getValueType(), "value", visited, resultSet);
        return;

      default:
    }
  }

  private static void addFullNameIfLeafNode(HoodieSchema schema, String name, Deque<String> visited, List<String> resultSet) {
    addFullNameIfLeafNode(schema.getUnderlyingType().getType(), name, visited, resultSet);
  }

  private static void addFullNameIfLeafNode(HoodieSchemaType type, String name, Deque<String> visited, List<String> resultSet) {
    switch (type) {
      case RECORD:
      case ARRAY:
      case MAP:
        return;
      default:
        resultSet.add(InternalSchemaUtils.createFullName(name, visited));
    }
  }

  /**
   * Converting from HoodieSchema -> internal schema -> HoodieSchema
   * causes null to always be first in unions.
   * if we compare a schema that has not been converted to internal schema
   * at any stage, the difference in ordering can cause issues. To resolve this,
   * we order null to be first for any HoodieSchema that enters into hudi.
   * AvroSchemaUtils.isProjectionOfInternal uses index based comparison for unions.
   * Spark and flink don't support complex unions so this would not be an issue
   * but for the metadata table HoodieMetadata.avsc uses a trick where we have a bunch of
   * different types wrapped in record for col stats.
   *
   * @param schema HoodieSchema.
   * @return a HoodieSchema where null is the first.
   */
  public static HoodieSchema fixNullOrdering(HoodieSchema schema) {
    if (schema == null) {
      return HoodieSchema.create(HoodieSchemaType.NULL);
    } else if (schema.getType() == HoodieSchemaType.NULL) {
      return schema;
    }
    return convert(convert(schema), schema.getFullName());
  }

  /**
   * Convert RecordType to HoodieSchema.
   *
   * @param type internal schema.
   * @param name the record name.
   * @return a HoodieSchema.
   */
  public static HoodieSchema convert(Types.RecordType type, String name) {
    return buildHoodieSchemaFromType(type, name);
  }

  /**
   * Convert internal type to HoodieSchema.
   *
   * @param type internal type.
   * @param name the record name.
   * @return a HoodieSchema.
   */
  public static HoodieSchema convert(Type type, String name) {
    return buildHoodieSchemaFromType(type, name);
  }

  /** Convert a HoodieSchema into internal type. */
  public static Type convertToField(HoodieSchema schema) {
    return buildTypeFromHoodieSchema(schema, Collections.emptyMap());
  }

  private static Type convertToField(HoodieSchema schema, Map<String, Integer> existingFieldNameToPositionMapping) {
    return buildTypeFromHoodieSchema(schema, existingFieldNameToPositionMapping);
  }

  /** Convert a HoodieSchema into internalSchema. */
  public static InternalSchema convert(HoodieSchema schema, Map<String, Integer> existingFieldNameToPositionMapping) {
    return new InternalSchema((Types.RecordType) convertToField(schema, existingFieldNameToPositionMapping));
  }

  public static InternalSchema convert(HoodieSchema schema) {
    return new InternalSchema((Types.RecordType) convertToField(schema));
  }

  /**
   * Build hudi type from HoodieSchema.
   *
   * @param schema a HoodieSchema.
   * @return a hudi type.
   */
  public static Type buildTypeFromHoodieSchema(HoodieSchema schema, Map<String, Integer> existingNameToPositions) {
    // set flag to check this has not been visited.
    Deque<String> visited = new LinkedList<>();
    AtomicInteger nextId = new AtomicInteger(0);
    return visitHoodieSchemaToBuildType(schema, visited, "", nextId, existingNameToPositions);
  }

  private static void checkNullType(Type fieldType, String fieldName, Deque<String> visited) {
    if (fieldType == null) {
      StringBuilder sb = new StringBuilder();
      sb.append("Field '");
      Iterator<String> visitedIterator = visited.descendingIterator();
      while (visitedIterator.hasNext()) {
        sb.append(visitedIterator.next());
        sb.append(".");
      }
      sb.append(fieldName);
      sb.append("' has type null");
      throw new HoodieNullSchemaTypeException(sb.toString());
    } else if (fieldType.typeId() == Type.TypeID.ARRAY) {
      visited.push(fieldName);
      checkNullType(((Types.ArrayType) fieldType).elementType(), InternalSchema.ARRAY_ELEMENT, visited);
      visited.pop();
    } else if (fieldType.typeId() == Type.TypeID.MAP) {
      visited.push(fieldName);
      checkNullType(((Types.MapType) fieldType).valueType(), InternalSchema.MAP_VALUE, visited);
      visited.pop();
    }
  }

  /**
   * Converts an HoodieSchema into internal type.
   *
   * @param schema a HoodieSchema.
   * @param visited track the visit node when do traversal for HoodieSchema; used to check if the name of record schema is correct.
   * @param currentFieldPath the dot-separated path to the current field; empty at the root and always ends in a '.' otherwise for ease of concatenation.
   * @param nextId an initial id which used to create id for all fields.
   * @return a hudi type match HoodieSchema.
   */
  private static Type visitHoodieSchemaToBuildType(HoodieSchema schema, Deque<String> visited, String currentFieldPath, AtomicInteger nextId, Map<String, Integer> existingNameToPosition) {
    switch (schema.getType()) {
      case RECORD:
        String name = schema.getFullName();
        if (visited.contains(name)) {
          throw new HoodieSchemaException(String.format("cannot convert recursive record %s", name));
        }
        visited.push(name);
        List<HoodieSchemaField> fields = existingNameToPosition.isEmpty() ? schema.getFields() :
            schema.getFields().stream()
                .sorted(Comparator.comparing(field -> existingNameToPosition.getOrDefault(currentFieldPath + field.name(), Integer.MAX_VALUE)))
                .collect(Collectors.toList());
        List<Type> fieldTypes = new ArrayList<>(fields.size());
        int nextAssignId = nextId.get();
        nextId.set(nextAssignId + fields.size());
        fields.forEach(field -> {
          Type fieldType = visitHoodieSchemaToBuildType(field.schema(), visited, currentFieldPath + field.name() + ".", nextId, existingNameToPosition);
          checkNullType(fieldType, field.name(), visited);
          fieldTypes.add(fieldType);
        });
        visited.pop();
        List<Types.Field> internalFields = new ArrayList<>(fields.size());

        for (int i  = 0; i < fields.size(); i++) {
          HoodieSchemaField field = fields.get(i);
          Type fieldType = fieldTypes.get(i);
          internalFields.add(Types.Field.get(nextAssignId, field.isNullable(), field.name(), fieldType, field.doc().orElse(null)));
          nextAssignId += 1;
        }
        // NOTE: We're keeping a tab of full-name here to make sure we stay
        //       compatible across various Spark (>= 2.4) and Avro (>= 1.8.2) versions;
        //       Avro will be properly handling fully-qualified names on its own (splitting
        //       them up into namespace/struct-name pair)
        return Types.RecordType.get(internalFields, name);
      case UNION:
        List<Type> fTypes = new ArrayList<>(2);
        schema.getTypes().forEach(t -> {
          fTypes.add(visitHoodieSchemaToBuildType(t, visited, currentFieldPath, nextId, existingNameToPosition));
        });
        return fTypes.get(0) == null ? fTypes.get(1) : fTypes.get(0);
      case ARRAY:
        String elementPath = currentFieldPath + InternalSchema.ARRAY_ELEMENT + ".";
        HoodieSchema elementSchema = schema.getElementType();
        int elementId = nextId.get();
        nextId.set(elementId + 1);
        Type elementType = visitHoodieSchemaToBuildType(elementSchema, visited, elementPath, nextId, existingNameToPosition);
        return Types.ArrayType.get(elementId, schema.getElementType().isNullable(), elementType);
      case MAP:
        int keyId = nextId.get();
        int valueId = keyId + 1;
        nextId.set(valueId + 1);
        String valuePath = currentFieldPath + InternalSchema.MAP_VALUE + ".";
        Type valueType = visitHoodieSchemaToBuildType(schema.getValueType(), visited, valuePath, nextId, existingNameToPosition);
        return Types.MapType.get(keyId, valueId, Types.StringType.get(), valueType, schema.getValueType().isNullable());
      default:
        return visitPrimitiveToBuildInternalType(schema);
    }
  }

  private static Type visitPrimitiveToBuildInternalType(HoodieSchema hoodieSchema) {
    Schema primitive = hoodieSchema.toAvroSchema();
    LogicalType logical = primitive.getLogicalType();
    if (logical != null) {
      String name = logical.getName();
      if (logical instanceof LogicalTypes.Decimal) {
        if (primitive.getType() == Schema.Type.FIXED) {
          return Types.DecimalTypeFixed.get(((LogicalTypes.Decimal) logical).getPrecision(),
                  ((LogicalTypes.Decimal) logical).getScale(), primitive.getFixedSize());
        } else if (primitive.getType() == Schema.Type.BYTES) {
          return Types.DecimalTypeBytes.get(
              ((LogicalTypes.Decimal) logical).getPrecision(),
              ((LogicalTypes.Decimal) logical).getScale());
        } else {
          throw new IllegalArgumentException("Unsupported primitive type for Decimal: " + primitive.getType().getName());
        }
      } else if (logical instanceof LogicalTypes.Date) {
        return Types.DateType.get();
      } else if (logical instanceof LogicalTypes.TimeMillis) {
        return Types.TimeMillisType.get();
      } else if (logical instanceof LogicalTypes.TimeMicros) {
        return Types.TimeType.get();
      } else if (logical instanceof LogicalTypes.TimestampMillis) {
        return Types.TimestampMillisType.get();
      } else if (logical instanceof LogicalTypes.TimestampMicros) {
        return Types.TimestampType.get();
      } else if (logical instanceof LogicalTypes.LocalTimestampMillis) {
        return Types.LocalTimestampMillisType.get();
      } else if (logical instanceof LogicalTypes.LocalTimestampMicros) {
        return Types.LocalTimestampMicrosType.get();
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
   * Converts hudi type into a HoodieSchema.
   *
   * @param type a hudi type.
   * @param recordName the record name
   * @return a HoodieSchema match this type
   */
  private static HoodieSchema buildHoodieSchemaFromType(Type type, String recordName) {
    Map<Type, HoodieSchema> cache = new HashMap<>();
    return visitInternalSchemaToBuildHoodieSchema(type, cache, recordName);
  }

  /**
   * Converts hudi internal Schema into a HoodieSchema.
   *
   * @param schema a hudi internal Schema.
   * @param recordName the record name
   * @return a HoodieSchema match hudi internal schema.
   */
  public static HoodieSchema buildHoodieSchemaFromInternalSchema(InternalSchema schema, String recordName) {
    Map<Type, HoodieSchema> cache = new HashMap<>();
    return visitInternalSchemaToBuildHoodieSchema(schema.getRecord(), cache, recordName);
  }

  /**
   * Converts hudi type into a HoodieSchema.
   *
   * @param type a hudi type.
   * @param cache use to cache intermediate convert result to save cost.
   * @param recordName auto-generated record name used as a fallback, in case
   * {@link org.apache.hudi.internal.schema.Types.RecordType} doesn't bear original record-name
   * @return a HoodieSchema match this type
   */
  private static HoodieSchema visitInternalSchemaToBuildHoodieSchema(Type type, Map<Type, HoodieSchema> cache, String recordName) {
    switch (type.typeId()) {
      case RECORD:
        Types.RecordType record = (Types.RecordType) type;
        List<HoodieSchema> schemas = new ArrayList<>();
        record.fields().forEach(f -> {
          String nestedRecordName = recordName + FIELD_NAME_DELIMITER + f.name();
          HoodieSchema tempSchema = visitInternalSchemaToBuildHoodieSchema(f.type(), cache, nestedRecordName);
          // convert tempSchema
          HoodieSchema result = f.isOptional() ? HoodieSchema.createNullable(tempSchema) : tempSchema;
          schemas.add(result);
        });
        // check visited
        HoodieSchema recordSchema;
        recordSchema = cache.get(record);
        if (recordSchema != null) {
          return recordSchema;
        }
        recordSchema = visitInternalRecordToBuildHoodieRecord(record, schemas, recordName);
        cache.put(record, recordSchema);
        return recordSchema;
      case ARRAY:
        Types.ArrayType array = (Types.ArrayType) type;
        HoodieSchema elementSchema;
        elementSchema = visitInternalSchemaToBuildHoodieSchema(array.elementType(), cache, recordName);
        HoodieSchema arraySchema;
        arraySchema = cache.get(array);
        if (arraySchema != null) {
          return arraySchema;
        }
        arraySchema = visitInternalArrayToBuildHoodieArray(array, elementSchema);
        cache.put(array, arraySchema);
        return arraySchema;
      case MAP:
        Types.MapType map = (Types.MapType) type;
        HoodieSchema keySchema = visitInternalSchemaToBuildHoodieSchema(map.keyType(), cache, recordName);
        HoodieSchema valueSchema = visitInternalSchemaToBuildHoodieSchema(map.valueType(), cache, recordName);
        HoodieSchema mapSchema = cache.get(map);
        if (mapSchema != null) {
          return mapSchema;
        }
        mapSchema = visitInternalMapToBuildHoodieMap(map, keySchema, valueSchema);
        cache.put(map, mapSchema);
        return mapSchema;
      default:
        HoodieSchema primitiveSchema = visitInternalPrimitiveToBuildHoodiePrimitiveType((Type.PrimitiveType) type, recordName);
        cache.put(type, primitiveSchema);
        return primitiveSchema;
    }
  }

  /**
   * Converts hudi RecordType to Hoodie RecordType.
   * this is auxiliary function used by visitInternalSchemaToBuildHoodieSchema
   */
  private static HoodieSchema visitInternalRecordToBuildHoodieRecord(Types.RecordType recordType, List<HoodieSchema> fieldSchemas, String recordNameFallback) {
    List<Types.Field> fields = recordType.fields();
    List<HoodieSchemaField> schemaFields = new ArrayList<>(fields.size());
    for (int i = 0; i < fields.size(); i++) {
      Types.Field f = fields.get(i);
      HoodieSchemaField field = HoodieSchemaField.of(f.name(), fieldSchemas.get(i), f.doc(), f.isOptional() ? HoodieJsonProperties.NULL_VALUE : null);
      schemaFields.add(field);
    }
    String recordName = Option.ofNullable(recordType.name()).orElse(recordNameFallback);
    return HoodieSchema.createRecord(recordName, null, null, false, schemaFields);
  }

  /**
   * Converts hudi ArrayType to Hoodie ArrayType.
   * this is auxiliary function used by visitInternalSchemaToBuildHoodieSchema
   */
  private static HoodieSchema visitInternalArrayToBuildHoodieArray(Types.ArrayType array, HoodieSchema elementSchema) {
    HoodieSchema result;
    if (array.isElementOptional()) {
      result = HoodieSchema.createArray(HoodieSchema.createNullable(elementSchema));
    } else {
      result = HoodieSchema.createArray(elementSchema);
    }
    return result;
  }

  /**
   * Converts hudi MapType to Hoodie MapType.
   * this is auxiliary function used by visitInternalSchemaToBuildHoodieSchema
   */
  private static HoodieSchema visitInternalMapToBuildHoodieMap(Types.MapType map, HoodieSchema keySchema, HoodieSchema valueSchema) {
    HoodieSchema mapSchema;
    if (keySchema.getType() == HoodieSchemaType.STRING) {
      mapSchema = HoodieSchema.createMap(map.isValueOptional() ? HoodieSchema.createNullable(valueSchema) : valueSchema);
    } else {
      throw new HoodieSchemaException("only support StringType key for Hoodie MapType");
    }
    return mapSchema;
  }

  /**
   * Converts hudi PrimitiveType to Hoodie PrimitiveType.
   * this is auxiliary function used by visitInternalSchemaToBuildHoodieSchema
   */
  private static HoodieSchema visitInternalPrimitiveToBuildHoodiePrimitiveType(Type.PrimitiveType primitive, String recordName) {
    switch (primitive.typeId()) {
      case BOOLEAN:
        return HoodieSchema.create(HoodieSchemaType.BOOLEAN);

      case INT:
        return HoodieSchema.create(HoodieSchemaType.INT);

      case LONG:
        return HoodieSchema.create(HoodieSchemaType.LONG);

      case FLOAT:
        return HoodieSchema.create(HoodieSchemaType.FLOAT);

      case DOUBLE:
        return HoodieSchema.create(HoodieSchemaType.DOUBLE);

      case DATE:
        return HoodieSchema.fromAvroSchema(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)));

      case TIME:
        return HoodieSchema.fromAvroSchema(LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG)));

      case TIME_MILLIS:
        return HoodieSchema.fromAvroSchema(LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT)));

      case TIMESTAMP:
        return HoodieSchema.fromAvroSchema(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG)));

      case TIMESTAMP_MILLIS:
        return HoodieSchema.fromAvroSchema(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)));

      case LOCAL_TIMESTAMP_MICROS:
        return HoodieSchema.fromAvroSchema(LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Schema.Type.LONG)));

      case LOCAL_TIMESTAMP_MILLIS:
        return HoodieSchema.fromAvroSchema(LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG)));

      case STRING:
        return HoodieSchema.create(HoodieSchemaType.STRING);

      case BINARY:
        return HoodieSchema.create(HoodieSchemaType.BYTES);

      case UUID: {
        // NOTE: All schemas corresponding to Hoodie's type [[FIXED]] are generated
        //       with the "fixed" name to stay compatible w/ [[SchemaConverters]]
        String name = recordName + FIELD_NAME_DELIMITER + "fixed";
        Schema fixedSchema = Schema.createFixed(name, null, null, 16);
        return HoodieSchema.fromAvroSchema(LogicalTypes.uuid().addToSchema(fixedSchema));
      }

      case FIXED: {
        Types.FixedType fixed = (Types.FixedType) primitive;
        // NOTE: All schemas corresponding to Hoodie's type [[FIXED]] are generated
        //       with the "fixed" name to stay compatible w/ [[SchemaConverters]]
        String name = recordName + FIELD_NAME_DELIMITER + "fixed";
        return HoodieSchema.createFixed(name, null, null, fixed.getFixedSize());
      }

      case DECIMAL:
      case DECIMAL_FIXED: {
        Types.DecimalTypeFixed decimal = (Types.DecimalTypeFixed) primitive;
        // NOTE: All schemas corresponding to Hoodie's type [[FIXED]] are generated
        //       with the "fixed" name to stay compatible w/ [[SchemaConverters]]
        String name = recordName + FIELD_NAME_DELIMITER + "fixed";
        Schema fixedSchema = Schema.createFixed(name,
            null, null, decimal.getFixedSize());
        return HoodieSchema.fromAvroSchema(LogicalTypes.decimal(decimal.precision(), decimal.scale())
            .addToSchema(fixedSchema));
      }

      case DECIMAL_BYTES: {
        Types.DecimalTypeBytes decimal = (Types.DecimalTypeBytes) primitive;
        return HoodieSchema.fromAvroSchema(LogicalTypes.decimal(decimal.precision(), decimal.scale())
            .addToSchema(Schema.create(Schema.Type.BYTES)));
      }

      default:
        throw new UnsupportedOperationException(
                "Unsupported type ID: " + primitive.typeId());
    }
  }
}
