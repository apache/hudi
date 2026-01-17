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

package org.apache.hudi.hadoop.utils;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieAvroSchemaException;
import org.apache.hudi.exception.HoodieException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.io.parquet.serde.ArrayWritableObjectInspector;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeException;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.apache.hadoop.hive.serde2.avro.InstanceCache;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableDateObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.io.ArrayWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.schema.HoodieSchemaUtils.isMetadataField;

/**
 * Helper class to serialize hive writable type to avro record.
 */
public class HiveAvroSerializer {

  private final List<String> columnNames;
  private final List<TypeInfo> columnTypes;
  private final ArrayWritableObjectInspector objectInspector;
  private final HoodieSchema recordSchema;

  private static final Logger LOG = LoggerFactory.getLogger(HiveAvroSerializer.class);

  public HiveAvroSerializer(HoodieSchema schema) {
    if (schema.getNonNullType().getType() != HoodieSchemaType.RECORD) {
      throw new IllegalArgumentException("Expected record schema, but got: " + schema);
    }
    this.recordSchema = schema;
    this.columnNames = schema.getFields().stream().map(HoodieSchemaField::name).map(String::toLowerCase).collect(Collectors.toList());
    try {
      this.columnTypes = HiveTypeUtils.generateColumnTypes(schema);
    } catch (AvroSerdeException e) {
      throw new HoodieAvroSchemaException(String.format("Failed to generate hive column types from avro schema: %s, due to %s", schema, e));
    }
    StructTypeInfo rowTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(this.columnNames, this.columnTypes);
    this.objectInspector = new ArrayWritableObjectInspector(rowTypeInfo);
  }

  public HiveAvroSerializer(ArrayWritableObjectInspector objectInspector, List<String> columnNames, List<TypeInfo> columnTypes) {
    this.columnNames = columnNames;
    this.columnTypes = columnTypes;
    this.objectInspector = objectInspector;
    this.recordSchema = null;
  }

  public Object getValue(ArrayWritable record, String fieldName) {
    if (StringUtils.isNullOrEmpty(fieldName)) {
      return null;
    }
    Object currentObject = record;
    ObjectInspector currentObjectInspector = this.objectInspector;
    String[] path = fieldName.split("\\.");

    for (int i = 0; i < path.length; i++) {
      String field = path[i];

      while (currentObjectInspector.getCategory() == ObjectInspector.Category.UNION) {
        UnionObjectInspector unionOI = (UnionObjectInspector) currentObjectInspector;
        byte tag = unionOI.getTag(currentObject);
        currentObject = unionOI.getField(currentObject);
        currentObjectInspector = unionOI.getObjectInspectors().get(tag);
      }

      if (!(currentObjectInspector instanceof ArrayWritableObjectInspector)) {
        throw new HoodieException("Expected struct (ArrayWritableObjectInspector) to access field '"
            + field + "', but found: " + currentObjectInspector.getClass().getSimpleName());
      }

      ArrayWritableObjectInspector structOI = (ArrayWritableObjectInspector) currentObjectInspector;
      StructField structFieldRef = structOI.getStructFieldRef(field);

      if (structFieldRef == null) {
        throw new HoodieException("Field '" + field + "' not found in current object inspector.");
      }

      Object fieldData = structOI.getStructFieldData(currentObject, structFieldRef);

      if (i == path.length - 1) {
        // Final field — return value as-is (possibly Writable or Java object)
        return fieldData;
      }

      currentObject = fieldData;
      currentObjectInspector = structFieldRef.getFieldObjectInspector();
    }

    return null;
  }

  public Object getValueAsJava(ArrayWritable record, String fieldName) {
    String[] path = fieldName.split("\\.");

    FieldContext context = extractFieldFromRecord(record,
        this.objectInspector,
        this.columnTypes,
        this.recordSchema,
        path[0]);

    for (int i = 1; i < path.length; i++) {
      if (!(context.object instanceof ArrayWritable)) {
        throw new HoodieException("Expected ArrayWritable while resolving '" + path[i]
            + "', but got " + context.object.getClass().getSimpleName());
      }

      if (context.objectInspector.getCategory() != ObjectInspector.Category.STRUCT) {
        throw new HoodieException("Expected StructObjectInspector to access field '" + path[i]
            + "', but found: " + context.objectInspector.getClass().getSimpleName());
      }

      if (!(context.typeInfo instanceof StructTypeInfo)) {
        throw new HoodieException("Expected StructTypeInfo while resolving '" + path[i]
            + "', but got " + context.typeInfo.getTypeName());
      }

      if (!(context.schema.getType() == HoodieSchemaType.RECORD)) {
        throw new HoodieException("Expected RecordSchema while resolving '" + path[i]
            + "', but got " + context.schema.getType());
      }

      context = extractFieldFromRecord((ArrayWritable) context.object, (StructObjectInspector) context.objectInspector,
          ((StructTypeInfo) context.typeInfo).getAllStructFieldTypeInfos(), context.schema, path[i]);
    }

    return serialize(context.typeInfo, context.objectInspector, context.object, context.schema);
  }

  private FieldContext extractFieldFromRecord(ArrayWritable record, StructObjectInspector structObjectInspector,
                                              List<TypeInfo> fieldTypes, HoodieSchema schema, String fieldName) {
    HoodieSchemaField schemaField = schema.getField(fieldName)
        .orElseThrow(() -> new HoodieException("Field '" + fieldName + "' not found in schema: " + schema));

    int fieldIdx = schemaField.pos();
    TypeInfo fieldTypeInfo = fieldTypes.get(fieldIdx);
    HoodieSchema fieldSchema = schemaField.schema().getNonNullType();

    StructField structField = structObjectInspector.getStructFieldRef(fieldName);
    if (structField == null) {
      throw new HoodieException("Field '" + fieldName + "' not found in ObjectInspector");
    }

    Object fieldData = structObjectInspector.getStructFieldData(record, structField);
    ObjectInspector fieldObjectInspector = structField.getFieldObjectInspector();

    if (fieldObjectInspector.getCategory() == ObjectInspector.Category.UNION) {
      UnionObjectInspector unionObjectInspector = (UnionObjectInspector) fieldObjectInspector;
      byte tag = unionObjectInspector.getTag(fieldData);
      fieldData = unionObjectInspector.getField(fieldData);
      fieldObjectInspector = unionObjectInspector.getObjectInspectors().get(tag);
    }

    return new FieldContext(fieldData, fieldObjectInspector, fieldTypeInfo, fieldSchema);
  }

  private static class FieldContext {
    final TypeInfo typeInfo;
    final ObjectInspector objectInspector;
    final Object object;
    final HoodieSchema schema;

    FieldContext(Object object, ObjectInspector objectInspector, TypeInfo typeInfo,  HoodieSchema schema) {
      this.object = object;
      this.objectInspector = objectInspector;
      this.typeInfo = typeInfo;
      this.schema = schema;
    }
  }

  private static final HoodieSchema STRING_SCHEMA = HoodieSchema.create(HoodieSchemaType.STRING);

  public GenericRecord serialize(Object o) {
    if (recordSchema == null) {
      throw new IllegalArgumentException("Cannot serialize without a record schema");
    }
    return serialize(o, recordSchema);
  }

  public GenericRecord serialize(Object o, HoodieSchema schema) {

    StructObjectInspector soi = objectInspector;
    GenericData.Record record = new GenericData.Record(schema.toAvroSchema());

    List<? extends StructField> outputFieldRefs = soi.getAllStructFieldRefs();
    if (outputFieldRefs.size() != columnNames.size()) {
      throw new HoodieException("Number of input columns was different than output columns (in = " + columnNames.size() + " vs out = " + outputFieldRefs.size());
    }

    int size = schema.getFields().size();

    List<? extends StructField> allStructFieldRefs = soi.getAllStructFieldRefs();
    List<Object> structFieldsDataAsList = soi.getStructFieldsDataAsList(o);

    for (int i = 0; i < size; i++) {
      HoodieSchemaField field = schema.getFields().get(i);
      if (i >= columnTypes.size()) {
        break;
      }
      try {
        setUpRecordFieldFromWritable(columnTypes.get(i), structFieldsDataAsList.get(i),
            allStructFieldRefs.get(i).getFieldObjectInspector(), record, field);
      } catch (Exception e) {
        LOG.error(String.format("current columnNames: %s", columnNames.stream().collect(Collectors.joining(","))));
        LOG.error(String.format("current type: %s", columnTypes.stream().map(f -> f.getTypeName()).collect(Collectors.joining(","))));
        LOG.error(String.format("current value: %s", HoodieRealtimeRecordReaderUtils.arrayWritableToString((ArrayWritable) o)));
        throw e;
      }
    }
    return record;
  }

  private void setUpRecordFieldFromWritable(TypeInfo typeInfo, Object structFieldData, ObjectInspector fieldOI, GenericData.Record record, HoodieSchemaField field) {
    // In Avro/HoodieSchema, field.defaultVal() returns:
    // - JsonProperties.Null / HoodieSchema.NULL_VALUE = if default is explicitly null
    // - null / isEmpty() = if field has NO default value
    // - some value = if field has an actual default
    Object val = serialize(typeInfo, fieldOI, structFieldData, field.schema());
    Object recordValue = val != null ? val : field.defaultVal()
        .map(defaultVal -> defaultVal == HoodieSchema.NULL_VALUE ? null : defaultVal)
        .orElse(null);
    record.put(field.name(), recordValue);
  }

  private Object serialize(TypeInfo typeInfo, ObjectInspector fieldOI, Object structFieldData, HoodieSchema schema) throws HoodieException {
    if (null == structFieldData) {
      return null;
    }

    schema = schema.getNonNullType();

    /* Because we use Hive's 'string' type when Avro calls for enum, we have to expressly check for enum-ness */
    if (HoodieSchemaType.ENUM == schema.getType()) {
      assert fieldOI instanceof PrimitiveObjectInspector;
      return serializeEnum((PrimitiveObjectInspector) fieldOI, structFieldData, schema);
    }

    switch (typeInfo.getCategory()) {
      case PRIMITIVE:
        assert fieldOI instanceof PrimitiveObjectInspector;
        return serializePrimitive((PrimitiveObjectInspector) fieldOI, structFieldData, schema);
      case MAP:
        assert fieldOI instanceof MapObjectInspector;
        assert typeInfo instanceof MapTypeInfo;
        return serializeMap((MapTypeInfo) typeInfo, (MapObjectInspector) fieldOI, structFieldData, schema);
      case LIST:
        assert fieldOI instanceof ListObjectInspector;
        assert typeInfo instanceof ListTypeInfo;
        return serializeList((ListTypeInfo) typeInfo, (ListObjectInspector) fieldOI, structFieldData, schema);
      case UNION:
        assert fieldOI instanceof UnionObjectInspector;
        assert typeInfo instanceof UnionTypeInfo;
        return serializeUnion((UnionTypeInfo) typeInfo, (UnionObjectInspector) fieldOI, structFieldData, schema);
      case STRUCT:
        assert fieldOI instanceof StructObjectInspector;
        assert typeInfo instanceof StructTypeInfo;
        return serializeStruct((StructTypeInfo) typeInfo, (StructObjectInspector) fieldOI, structFieldData, schema);
      default:
        throw new HoodieException("Ran out of TypeInfo Categories: " + typeInfo.getCategory());
    }
  }

  /**
   * private cache to avoid lots of EnumSymbol creation while serializing.
   * Two levels because the enum symbol is specific to a schema.
   * Object because we want to avoid the overhead of repeated toString calls while maintaining compatibility.
   * Provided there are few enum types per record, and few symbols per enum, memory use should be moderate.
   * eg 20 types with 50 symbols each as length-10 Strings should be on the order of 100KB per AvroSerializer.
   */
  final InstanceCache<Schema, InstanceCache<Object, GenericEnumSymbol>> enums = new InstanceCache<Schema, InstanceCache<Object, GenericEnumSymbol>>() {
    @Override
    protected InstanceCache<Object, GenericEnumSymbol> makeInstance(final Schema schema,
                                                                    Set<Schema> seenSchemas) {
      return new InstanceCache<Object, GenericEnumSymbol>() {
        @Override
        protected GenericEnumSymbol makeInstance(Object seed, Set<Object> seenSchemas) {
          return new GenericData.EnumSymbol(schema, seed.toString());
        }
      };
    }
  };

  private Object serializeEnum(PrimitiveObjectInspector fieldOI, Object structFieldData, HoodieSchema schema) throws HoodieException {
    try {
      return enums.retrieve(schema.toAvroSchema()).retrieve(serializePrimitive(fieldOI, structFieldData, schema));
    } catch (Exception e) {
      throw new HoodieException(e);
    }
  }

  private Object serializeStruct(StructTypeInfo typeInfo, StructObjectInspector ssoi, Object o, HoodieSchema schema) {
    int size = schema.getFields().size();
    List<? extends StructField> allStructFieldRefs = ssoi.getAllStructFieldRefs();
    List<Object> structFieldsDataAsList = ssoi.getStructFieldsDataAsList(o);
    GenericData.Record record = new GenericData.Record(schema.toAvroSchema());
    ArrayList<TypeInfo> allStructFieldTypeInfos = typeInfo.getAllStructFieldTypeInfos();

    for (int i = 0; i < size; i++) {
      HoodieSchemaField field = schema.getFields().get(i);
      setUpRecordFieldFromWritable(allStructFieldTypeInfos.get(i), structFieldsDataAsList.get(i),
          allStructFieldRefs.get(i).getFieldObjectInspector(), record, field);
    }
    return record;
  }

  private Object serializePrimitive(PrimitiveObjectInspector fieldOI, Object structFieldData, HoodieSchema schema) throws HoodieException {
    switch (fieldOI.getPrimitiveCategory()) {
      case BINARY:
        if (schema.getType() == HoodieSchemaType.BYTES) {
          return AvroSerdeUtils.getBufferFromBytes((byte[]) fieldOI.getPrimitiveJavaObject(structFieldData));
        } else if (schema.getType() == HoodieSchemaType.FIXED) {
          GenericData.Fixed fixed = new GenericData.Fixed(schema.toAvroSchema(), (byte[]) fieldOI.getPrimitiveJavaObject(structFieldData));
          return fixed;
        } else {
          throw new HoodieException("Unexpected Avro schema for Binary TypeInfo: " + schema.getType());
        }
      case DECIMAL:
        HiveDecimal dec = (HiveDecimal) fieldOI.getPrimitiveJavaObject(structFieldData);
        if (schema.getType() != HoodieSchemaType.DECIMAL) {
          throw new HoodieException("Unexpected schema type for DECIMAL: " + schema.getType());
        }
        HoodieSchema.Decimal decimal = (HoodieSchema.Decimal) schema;
        BigDecimal bd = new BigDecimal(dec.toString()).setScale(decimal.getScale());
        if (decimal.isFixed()) {
          return HoodieAvroUtils.DECIMAL_CONVERSION.toFixed(bd, schema.toAvroSchema(), decimal.toAvroSchema().getLogicalType());
        } else {
          return HoodieAvroUtils.DECIMAL_CONVERSION.toBytes(bd, schema.toAvroSchema(), decimal.toAvroSchema().getLogicalType());
        }
      case CHAR:
        HiveChar ch = (HiveChar) fieldOI.getPrimitiveJavaObject(structFieldData);
        return new Utf8(ch.getStrippedValue());
      case VARCHAR:
        HiveVarchar vc = (HiveVarchar) fieldOI.getPrimitiveJavaObject(structFieldData);
        return new Utf8(vc.getValue());
      case STRING:
        String string = (String) fieldOI.getPrimitiveJavaObject(structFieldData);
        return new Utf8(string);
      case DATE:
        return HoodieHiveUtils.getDays(structFieldData);
      case TIMESTAMP:
        return HoodieHiveUtils.getMills(structFieldData);
      case INT:
        if (schema.getType() == HoodieSchemaType.DATE) {
          return new WritableDateObjectInspector().getPrimitiveWritableObject(structFieldData).getDays();
        }
        return fieldOI.getPrimitiveJavaObject(structFieldData);
      case UNKNOWN:
        throw new HoodieException("Received UNKNOWN primitive category.");
      case VOID:
        return null;
      default: // All other primitive types are simple
        return fieldOI.getPrimitiveJavaObject(structFieldData);
    }
  }

  private Object serializeUnion(UnionTypeInfo typeInfo, UnionObjectInspector fieldOI, Object structFieldData, HoodieSchema schema) throws HoodieException {
    byte tag = fieldOI.getTag(structFieldData);

    // Invariant that Avro's tag ordering must match Hive's.
    return serialize(typeInfo.getAllUnionObjectTypeInfos().get(tag),
        fieldOI.getObjectInspectors().get(tag),
        fieldOI.getField(structFieldData),
        schema.getTypes().get(tag));
  }

  private Object serializeList(ListTypeInfo typeInfo, ListObjectInspector fieldOI, Object structFieldData, HoodieSchema schema) throws HoodieException {
    List<?> list = fieldOI.getList(structFieldData);
    List<Object> deserialized = new GenericData.Array<>(list.size(), schema.toAvroSchema());

    TypeInfo listElementTypeInfo = typeInfo.getListElementTypeInfo();
    ObjectInspector listElementObjectInspector = fieldOI.getListElementObjectInspector();
    // NOTE: We have to resolve nullable schema, since Avro permits array elements
    //       to be null
    HoodieSchema arrayNestedType = schema.getElementType().getNonNullType();
    HoodieSchema elementType;
    if (listElementObjectInspector.getCategory() == ObjectInspector.Category.PRIMITIVE) {
      elementType = arrayNestedType;
    } else {
      elementType =
          arrayNestedType.getField("element").map(HoodieSchemaField::schema).orElse(arrayNestedType);
    }
    for (int i = 0; i < list.size(); i++) {
      Object childFieldData = list.get(i);
      if (childFieldData instanceof ArrayWritable && ((ArrayWritable) childFieldData).get().length != ((StructTypeInfo) listElementTypeInfo).getAllStructFieldNames().size()) {
        deserialized.add(i, serialize(listElementTypeInfo, listElementObjectInspector, ((ArrayWritable) childFieldData).get()[0], elementType));
      } else {
        deserialized.add(i, serialize(listElementTypeInfo, listElementObjectInspector, childFieldData, elementType));
      }
    }
    return deserialized;
  }

  private Object serializeMap(MapTypeInfo typeInfo, MapObjectInspector fieldOI, Object structFieldData, HoodieSchema schema) throws HoodieException {
    // Avro only allows maps with string keys
    if (!mapHasStringKey(fieldOI.getMapKeyObjectInspector())) {
      throw new HoodieException("Avro only supports maps with keys as Strings.  Current Map is: " + typeInfo.toString());
    }

    ObjectInspector mapKeyObjectInspector = fieldOI.getMapKeyObjectInspector();
    ObjectInspector mapValueObjectInspector = fieldOI.getMapValueObjectInspector();
    TypeInfo mapKeyTypeInfo = typeInfo.getMapKeyTypeInfo();
    TypeInfo mapValueTypeInfo = typeInfo.getMapValueTypeInfo();
    Map<?, ?> map = fieldOI.getMap(structFieldData);
    HoodieSchema valueType = schema.getValueType();

    Map<Object, Object> deserialized = new LinkedHashMap<Object, Object>(fieldOI.getMapSize(structFieldData));

    for (Map.Entry<?, ?> entry : map.entrySet()) {
      deserialized.put(serialize(mapKeyTypeInfo, mapKeyObjectInspector, entry.getKey(), STRING_SCHEMA),
          serialize(mapValueTypeInfo, mapValueObjectInspector, entry.getValue(), valueType));
    }

    return deserialized;
  }

  private boolean mapHasStringKey(ObjectInspector mapKeyObjectInspector) {
    return mapKeyObjectInspector instanceof PrimitiveObjectInspector
        && ((PrimitiveObjectInspector) mapKeyObjectInspector).getPrimitiveCategory().equals(PrimitiveObjectInspector.PrimitiveCategory.STRING);
  }

  public static GenericRecord rewriteRecordIgnoreResultCheck(GenericRecord oldRecord, HoodieSchema newSchema) {
    GenericRecord newRecord = new GenericData.Record(newSchema.toAvroSchema());
    boolean isSpecificRecord = oldRecord instanceof SpecificRecordBase;
    for (HoodieSchemaField f : newSchema.getFields()) {
      if (!(isSpecificRecord && isMetadataField(f.name()))) {
        copyOldValueOrSetDefault(oldRecord, newRecord, f);
      }
    }
    return newRecord;
  }

  private static void copyOldValueOrSetDefault(GenericRecord oldRecord, GenericRecord newRecord, HoodieSchemaField field) {
    Schema oldSchema = oldRecord.getSchema();
    Object fieldValue = oldSchema.getField(field.name()) == null ? null : oldRecord.get(field.name());

    if (fieldValue != null) {
      // In case field's value is a nested record, we have to rewrite it as well
      Object newFieldValue;
      if (fieldValue instanceof GenericRecord) {
        GenericRecord record = (GenericRecord) fieldValue;
        HoodieSchema fieldSchema = HoodieSchemaUtils.resolveUnionSchema(field.schema(), record.getSchema().getFullName());
        newFieldValue = rewriteRecordIgnoreResultCheck(record, fieldSchema);
      } else {
        newFieldValue = fieldValue;
      }
      newRecord.put(field.name(), newFieldValue);
    } else if (field.defaultVal() == HoodieSchema.NULL_VALUE) {
      newRecord.put(field.name(), null);
    } else {
      newRecord.put(field.name(), field.defaultVal());
    }
  }
}

