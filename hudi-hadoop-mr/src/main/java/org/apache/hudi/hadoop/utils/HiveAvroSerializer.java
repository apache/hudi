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

import org.apache.hudi.avro.AvroSchemaUtils;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieAvroSchemaException;
import org.apache.hudi.exception.HoodieException;

import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalTypes;
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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableTimestampObjectInspector;
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

import static org.apache.hudi.avro.AvroSchemaUtils.getNonNullTypeFromUnion;
import static org.apache.hudi.avro.AvroSchemaUtils.resolveUnionSchema;
import static org.apache.hudi.avro.HoodieAvroUtils.isMetadataField;

/**
 * Helper class to serialize hive writable type to avro record.
 */
public class HiveAvroSerializer {

  private final List<String> columnNames;
  private final List<TypeInfo> columnTypes;
  private final ObjectInspector objectInspector;

  private static final Logger LOG = LoggerFactory.getLogger(HiveAvroSerializer.class);

  public HiveAvroSerializer(ArrayWritableObjectInspector objectInspector, List<String> columnNames, List<TypeInfo> columnTypes) {
    this.columnNames = columnNames;
    this.columnTypes = columnTypes;
    this.objectInspector = objectInspector;
  }

  private static final Schema STRING_SCHEMA = Schema.create(Schema.Type.STRING);

  public GenericRecord serialize(Object o, Schema schema) {

    StructObjectInspector soi = (StructObjectInspector) objectInspector;
    GenericData.Record record = new GenericData.Record(schema);

    List<? extends StructField> outputFieldRefs = soi.getAllStructFieldRefs();
    if (outputFieldRefs.size() != columnNames.size()) {
      throw new HoodieException("Number of input columns was different than output columns (in = " + columnNames.size() + " vs out = " + outputFieldRefs.size());
    }

    int size = schema.getFields().size();

    List<? extends StructField> allStructFieldRefs = soi.getAllStructFieldRefs();
    List<Object> structFieldsDataAsList = soi.getStructFieldsDataAsList(o);

    for (int i = 0; i < size; i++) {
      Schema.Field field = schema.getFields().get(i);
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

  private void setUpRecordFieldFromWritable(TypeInfo typeInfo, Object structFieldData, ObjectInspector fieldOI, GenericData.Record record, Schema.Field field) {
    Object val = serialize(typeInfo, fieldOI, structFieldData, field.schema());
    if (val == null) {
      if (field.defaultVal() instanceof JsonProperties.Null) {
        record.put(field.name(), null);
      } else {
        record.put(field.name(), field.defaultVal());
      }
    } else {
      record.put(field.name(), val);
    }
  }

  /**
   * Determine if an Avro schema is of type Union[T, NULL].  Avro supports nullable
   * types via a union of type T and null.  This is a very common use case.
   * As such, we want to silently convert it to just T and allow the value to be null.
   * <p>
   * When a Hive union type is used with AVRO, the schema type becomes
   * Union[NULL, T1, T2, ...]. The NULL in the union should be silently removed
   *
   * @return true if type represents Union[T, Null], false otherwise
   */
  public static boolean isNullableType(Schema schema) {
    if (!schema.getType().equals(Schema.Type.UNION)) {
      return false;
    }

    List<Schema> itemSchemas = schema.getTypes();
    if (itemSchemas.size() < 2) {
      return false;
    }

    for (Schema itemSchema : itemSchemas) {
      if (Schema.Type.NULL.equals(itemSchema.getType())) {
        return true;
      }
    }

    // [null, null] not allowed, so this check is ok.
    return false;
  }

  /**
   * If the union schema is a nullable union, get the schema for the non-nullable type.
   * This method does no checking that the provided Schema is nullable. If the provided
   * union schema is non-nullable, it simply returns the union schema
   */
  public static Schema getOtherTypeFromNullableType(Schema unionSchema) {
    final List<Schema> types = unionSchema.getTypes();
    if (types.size() == 2) { // most common scenario
      if (types.get(0).getType() == Schema.Type.NULL) {
        return types.get(1);
      }
      if (types.get(1).getType() == Schema.Type.NULL) {
        return types.get(0);
      }
      // not a nullable union
      return unionSchema;
    }

    final List<Schema> itemSchemas = new ArrayList<>();
    for (Schema itemSchema : types) {
      if (!Schema.Type.NULL.equals(itemSchema.getType())) {
        itemSchemas.add(itemSchema);
      }
    }

    if (itemSchemas.size() > 1) {
      return Schema.createUnion(itemSchemas);
    } else {
      return itemSchemas.get(0);
    }
  }

  private Object serialize(TypeInfo typeInfo, ObjectInspector fieldOI, Object structFieldData, Schema schema) throws HoodieException {
    if (null == structFieldData) {
      return null;
    }

    schema = getNonNullTypeFromUnion(schema);

    /* Because we use Hive's 'string' type when Avro calls for enum, we have to expressly check for enum-ness */
    if (Schema.Type.ENUM.equals(schema.getType())) {
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

  private Object serializeEnum(PrimitiveObjectInspector fieldOI, Object structFieldData, Schema schema) throws HoodieException {
    try {
      return enums.retrieve(schema).retrieve(serializePrimitive(fieldOI, structFieldData, schema));
    } catch (Exception e) {
      throw new HoodieException(e);
    }
  }

  private Object serializeStruct(StructTypeInfo typeInfo, StructObjectInspector ssoi, Object o, Schema schema) {
    int size = schema.getFields().size();
    List<? extends StructField> allStructFieldRefs = ssoi.getAllStructFieldRefs();
    List<Object> structFieldsDataAsList = ssoi.getStructFieldsDataAsList(o);
    GenericData.Record record = new GenericData.Record(schema);
    ArrayList<TypeInfo> allStructFieldTypeInfos = typeInfo.getAllStructFieldTypeInfos();

    for (int i = 0; i < size; i++) {
      Schema.Field field = schema.getFields().get(i);
      setUpRecordFieldFromWritable(allStructFieldTypeInfos.get(i), structFieldsDataAsList.get(i),
          allStructFieldRefs.get(i).getFieldObjectInspector(), record, field);
    }
    return record;
  }

  private Object serializePrimitive(PrimitiveObjectInspector fieldOI, Object structFieldData, Schema schema) throws HoodieException {
    switch (fieldOI.getPrimitiveCategory()) {
      case BINARY:
        if (schema.getType() == Schema.Type.BYTES) {
          return AvroSerdeUtils.getBufferFromBytes((byte[]) fieldOI.getPrimitiveJavaObject(structFieldData));
        } else if (schema.getType() == Schema.Type.FIXED) {
          GenericData.Fixed fixed = new GenericData.Fixed(schema, (byte[]) fieldOI.getPrimitiveJavaObject(structFieldData));
          return fixed;
        } else {
          throw new HoodieException("Unexpected Avro schema for Binary TypeInfo: " + schema.getType());
        }
      case DECIMAL:
        HiveDecimal dec = (HiveDecimal) fieldOI.getPrimitiveJavaObject(structFieldData);
        LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) schema.getLogicalType();
        BigDecimal bd = new BigDecimal(dec.toString()).setScale(decimal.getScale());
        if (schema.getType() == Schema.Type.BYTES) {
          return HoodieAvroUtils.DECIMAL_CONVERSION.toBytes(bd, schema, decimal);
        } else {
          return HoodieAvroUtils.DECIMAL_CONVERSION.toFixed(bd, schema, decimal);
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
        Object timestamp = ((WritableTimestampObjectInspector) fieldOI).getPrimitiveJavaObject(structFieldData);
        return HoodieHiveUtils.getMills(timestamp);
      case INT:
        if (schema.getLogicalType() != null && schema.getLogicalType().getName().equals("date")) {
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

  private Object serializeUnion(UnionTypeInfo typeInfo, UnionObjectInspector fieldOI, Object structFieldData, Schema schema) throws HoodieException {
    byte tag = fieldOI.getTag(structFieldData);

    // Invariant that Avro's tag ordering must match Hive's.
    return serialize(typeInfo.getAllUnionObjectTypeInfos().get(tag),
        fieldOI.getObjectInspectors().get(tag),
        fieldOI.getField(structFieldData),
        schema.getTypes().get(tag));
  }

  private Object serializeList(ListTypeInfo typeInfo, ListObjectInspector fieldOI, Object structFieldData, Schema schema) throws HoodieException {
    List<?> list = fieldOI.getList(structFieldData);
    List<Object> deserialized = new GenericData.Array<Object>(list.size(), schema);

    TypeInfo listElementTypeInfo = typeInfo.getListElementTypeInfo();
    ObjectInspector listElementObjectInspector = fieldOI.getListElementObjectInspector();
    // NOTE: We have to resolve nullable schema, since Avro permits array elements
    //       to be null
    Schema arrayNestedType = getNonNullTypeFromUnion(schema.getElementType());
    Schema elementType;
    if (listElementObjectInspector.getCategory() == ObjectInspector.Category.PRIMITIVE) {
      elementType = arrayNestedType;
    } else {
      elementType = arrayNestedType.getField("element") == null ? arrayNestedType : arrayNestedType.getField("element").schema();
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

  private Object serializeMap(MapTypeInfo typeInfo, MapObjectInspector fieldOI, Object structFieldData, Schema schema) throws HoodieException {
    // Avro only allows maps with string keys
    if (!mapHasStringKey(fieldOI.getMapKeyObjectInspector())) {
      throw new HoodieException("Avro only supports maps with keys as Strings.  Current Map is: " + typeInfo.toString());
    }

    ObjectInspector mapKeyObjectInspector = fieldOI.getMapKeyObjectInspector();
    ObjectInspector mapValueObjectInspector = fieldOI.getMapValueObjectInspector();
    TypeInfo mapKeyTypeInfo = typeInfo.getMapKeyTypeInfo();
    TypeInfo mapValueTypeInfo = typeInfo.getMapValueTypeInfo();
    Map<?, ?> map = fieldOI.getMap(structFieldData);
    Schema valueType = schema.getValueType();

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

  public static GenericRecord rewriteRecordIgnoreResultCheck(GenericRecord oldRecord, Schema newSchema) {
    GenericRecord newRecord = new GenericData.Record(newSchema);
    boolean isSpecificRecord = oldRecord instanceof SpecificRecordBase;
    for (Schema.Field f : newSchema.getFields()) {
      if (!(isSpecificRecord && isMetadataField(f.name()))) {
        copyOldValueOrSetDefault(oldRecord, newRecord, f);
      }
    }
    return newRecord;
  }

  private static void copyOldValueOrSetDefault(GenericRecord oldRecord, GenericRecord newRecord, Schema.Field field) {
    Schema oldSchema = oldRecord.getSchema();
    Object fieldValue = oldSchema.getField(field.name()) == null ? null : oldRecord.get(field.name());

    if (fieldValue != null) {
      // In case field's value is a nested record, we have to rewrite it as well
      Object newFieldValue;
      if (fieldValue instanceof GenericRecord) {
        GenericRecord record = (GenericRecord) fieldValue;
        newFieldValue = rewriteRecordIgnoreResultCheck(record, resolveUnionSchema(field.schema(), record.getSchema().getFullName()));
      } else {
        newFieldValue = fieldValue;
      }
      newRecord.put(field.name(), newFieldValue);
    } else if (field.defaultVal() instanceof JsonProperties.Null) {
      newRecord.put(field.name(), null);
    } else {
      newRecord.put(field.name(), field.defaultVal());
    }
  }
}

