/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utilities.sources.helpers;

import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;

import com.google.protobuf.BoolValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.FloatValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.google.protobuf.Message;
import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;
import com.google.protobuf.util.Timestamps;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.common.utils.CopyOnWriteMap;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * A utility class to help translate from Proto to Avro.
 */
public class ProtoConversionUtil {

  /**
   * Creates an Avro {@link Schema} for the provided class. Assumes that the class is a protobuf {@link Message}.
   * @param clazz The protobuf class
   * @param wrappedPrimitivesAsRecords set to true to treat wrapped primitives like record with a single "value" field instead of simply a nullable field
   * @param maxRecursionDepth the number of times to unravel a recursive proto schema before spilling the rest to bytes
   * @param timestampsAsRecords if true convert {@link Timestamp} to a Record with a seconds and nanos field, otherwise convert protobuf {@link Timestamp} to a long with the time-mircos logical type.
   * @return An Avro schema
   */
  public static Schema getAvroSchemaForMessageClass(Class clazz, boolean wrappedPrimitivesAsRecords, int maxRecursionDepth, boolean timestampsAsRecords) {
    return AvroSupport.get().getSchema(clazz, wrappedPrimitivesAsRecords, maxRecursionDepth, timestampsAsRecords);
  }

  /**
   * Converts the provided {@link Message} into an avro {@link GenericRecord} with the provided schema.
   * @param schema target schema to convert into
   * @param message the source message to convert
   * @return an Avro GenericRecord
   */
  public static GenericRecord convertToAvro(Schema schema, Message message) {
    return AvroSupport.get().convert(schema, message);
  }

  /**
   * This class provides support for generating schemas and converting from proto to avro. We don't directly use Avro's ProtobufData class so we can:
   * 1. Customize how schemas are generated for protobufs. We treat Enums as strings and provide an option to treat wrapped primitives like {@link Int32Value} and {@link StringValue} as messages
   * (default behavior) or as nullable versions of those primitives.
   * 2. Convert directly from a protobuf {@link Message} to a {@link GenericRecord} while properly handling enums and wrapped primitives mentioned above.
   */
  private static class AvroSupport {
    private static final Schema STRING_SCHEMA = Schema.create(Schema.Type.STRING);
    private static final Schema NULL_SCHEMA = Schema.create(Schema.Type.NULL);
    private static final String OVERFLOW_DESCRIPTOR_FIELD_NAME = "descriptor_full_name";
    private static final String OVERFLOW_BYTES_FIELD_NAME = "proto_bytes";
    private static final Schema RECURSION_OVERFLOW_SCHEMA = Schema.createRecord("recursion_overflow", null, "org.apache.hudi.proto", false,
        Arrays.asList(new Schema.Field(OVERFLOW_DESCRIPTOR_FIELD_NAME, STRING_SCHEMA, null, ""),
            new Schema.Field(OVERFLOW_BYTES_FIELD_NAME, Schema.create(Schema.Type.BYTES), null, "".getBytes())));
    private static final AvroSupport INSTANCE = new AvroSupport();
    // A cache of the proto class name paired with whether wrapped primitives should be flattened as the key and the generated avro schema as the value
    private static final Map<SchemaCacheKey, Schema> SCHEMA_CACHE = new ConcurrentHashMap<>();
    // A cache with a key as the pair target avro schema and the proto descriptor for the source and the value as an array of proto field descriptors where the order matches the avro ordering.
    // When converting from proto to avro, we want to be able to iterate over the fields in the proto in the same order as they appear in the avro schema.
    private static final Map<Pair<Schema, Descriptors.Descriptor>, Descriptors.FieldDescriptor[]> FIELD_CACHE = new ConcurrentHashMap<>();
    private static final Schema UNSIGNED_LONG_SCHEMA = LogicalTypes.decimal(21).addToSchema(Schema.createFixed("unsigned_long", null, "org.apache.hudi.protos", 9));
    private static final Set<Descriptors.Descriptor> WRAPPER_DESCRIPTORS_TO_TYPE = getWrapperDescriptorsToType();

    private static Set<Descriptors.Descriptor> getWrapperDescriptorsToType() {
      Set<Descriptors.Descriptor> wrapperDescriptorsToType = new HashSet<>();
      wrapperDescriptorsToType.add(StringValue.getDescriptor());
      wrapperDescriptorsToType.add(Int32Value.getDescriptor());
      wrapperDescriptorsToType.add(UInt32Value.getDescriptor());
      wrapperDescriptorsToType.add(Int64Value.getDescriptor());
      wrapperDescriptorsToType.add(UInt64Value.getDescriptor());
      wrapperDescriptorsToType.add(BoolValue.getDescriptor());
      wrapperDescriptorsToType.add(BytesValue.getDescriptor());
      wrapperDescriptorsToType.add(DoubleValue.getDescriptor());
      wrapperDescriptorsToType.add(FloatValue.getDescriptor());
      return wrapperDescriptorsToType;
    }

    private AvroSupport() {
    }

    public static AvroSupport get() {
      return INSTANCE;
    }

    public GenericRecord convert(Schema schema, Message message) {
      return (GenericRecord) convertObject(schema, message);
    }

    public Schema getSchema(Class c, boolean wrappedPrimitivesAsRecords, int maxRecursionDepth, boolean timestampsAsRecords) {
      return SCHEMA_CACHE.computeIfAbsent(Pair.of(c, wrappedPrimitivesAsRecords), key -> {
        try {
          Object descriptor = c.getMethod("getDescriptor").invoke(null);
          if (c.isEnum()) {
            return getEnumSchema((Descriptors.EnumDescriptor) descriptor);
          } else {
            Descriptors.Descriptor castedDescriptor = (Descriptors.Descriptor) descriptor;
            return getMessageSchema(castedDescriptor, new CopyOnWriteMap<>(), wrappedPrimitivesAsRecords, getNamespace(castedDescriptor.getFullName()), maxRecursionDepth, timestampsAsRecords);
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
    }

    private Schema getEnumSchema(Descriptors.EnumDescriptor enumDescriptor) {
      List<String> symbols = new ArrayList<>(enumDescriptor.getValues().size());
      for (Descriptors.EnumValueDescriptor valueDescriptor : enumDescriptor.getValues()) {
        symbols.add(valueDescriptor.getName());
      }
      return Schema.createEnum(enumDescriptor.getName(), null, getNamespace(enumDescriptor.getFullName()), symbols);
    }

    /**
     * Translates a Proto Message descriptor into an Avro Schema
     * @param descriptor the descriptor for the proto message
     * @param recursionDepths a map of the descriptor to the number of times it has been encountered in this depth first traversal of the schema.
     *                        This is used to cap the number of times we recurse on a schema.
     * @param wrappedPrimitivesAsRecords if false, treat wrapped primitives as nullable primitives, if true, treat them as proto messages
     * @param path a string prefixed with the namespace of the original message being translated to avro and containing the current dot separated path tracking progress through the schema.
     *             This value is used for a namespace when creating Avro records to avoid an error when reusing the same class name when unraveling a recursive schema.
     * @param maxRecursionDepth the number of times to unravel a recursive proto schema before spilling the rest to bytes
     * @param timestampsAsRecords if true, treat timestamps as records with a seconds and nanos field. If false, treat as a long with timestamp-micros logical type
     * @return an avro schema
     */
    private Schema getMessageSchema(Descriptors.Descriptor descriptor, CopyOnWriteMap<Descriptors.Descriptor, Integer> recursionDepths, boolean wrappedPrimitivesAsRecords, String path,
                                    int maxRecursionDepth, boolean timestampsAsRecords) {
      // Parquet does not handle recursive schemas so we "unravel" the proto N levels
      Integer currentRecursionCount = recursionDepths.getOrDefault(descriptor, 0);
      if (currentRecursionCount >= maxRecursionDepth) {
        return RECURSION_OVERFLOW_SCHEMA;
      }
      // The current path is used as a namespace to avoid record name collisions within recursive schemas
      Schema result = Schema.createRecord(descriptor.getName(), null, path, false);

      recursionDepths.put(descriptor, ++currentRecursionCount);

      List<Schema.Field> fields = new ArrayList<>(descriptor.getFields().size());
      for (Descriptors.FieldDescriptor f : descriptor.getFields()) {
        // each branch of the schema traversal requires its own recursion depth tracking so copy the recursionDepths map
        fields.add(new Schema.Field(f.getName(), getFieldSchema(f, new CopyOnWriteMap<>(recursionDepths), wrappedPrimitivesAsRecords, path, maxRecursionDepth, timestampsAsRecords),
            null, getDefault(f)));
      }
      result.setFields(fields);
      return result;
    }

    private Schema getFieldSchema(Descriptors.FieldDescriptor f, CopyOnWriteMap<Descriptors.Descriptor, Integer> recursionDepths, boolean wrappedPrimitivesAsRecords, String path,
                                  int maxRecursionDepth, boolean timestampsAsRecords) {
      Function<Schema, Schema> schemaFinalizer = schema -> {
        Schema updatedSchema = schema;
        // all fields in the oneof will be treated as nullable
        if (f.getContainingOneof() != null && !(schema.isUnion() && schema.getTypes().get(0).getType() == Schema.Type.NULL)) {
          updatedSchema = makeSchemaNullable(schema);
        }
        if (f.isRepeated()) {
          updatedSchema = Schema.createArray(updatedSchema);
        }
        return updatedSchema;
      };
      switch (f.getType()) {
        case BOOL:
          return schemaFinalizer.apply(Schema.create(Schema.Type.BOOLEAN));
        case FLOAT:
          return schemaFinalizer.apply(Schema.create(Schema.Type.FLOAT));
        case DOUBLE:
          return schemaFinalizer.apply(Schema.create(Schema.Type.DOUBLE));
        case ENUM:
          return schemaFinalizer.apply(getEnumSchema(f.getEnumType()));
        case STRING:
          Schema s = Schema.create(Schema.Type.STRING);
          GenericData.setStringType(s, GenericData.StringType.String);
          return schemaFinalizer.apply(s);
        case BYTES:
          return schemaFinalizer.apply(Schema.create(Schema.Type.BYTES));
        case INT32:
        case SINT32:
        case FIXED32:
        case SFIXED32:
          return schemaFinalizer.apply(Schema.create(Schema.Type.INT));
        case UINT32:
        case INT64:
        case SINT64:
        case FIXED64:
        case SFIXED64:
          return schemaFinalizer.apply(Schema.create(Schema.Type.LONG));
        case UINT64:
          return schemaFinalizer.apply(UNSIGNED_LONG_SCHEMA);
        case MESSAGE:
          String updatedPath = appendFieldNameToPath(path, f.getName());
          if (!wrappedPrimitivesAsRecords && WRAPPER_DESCRIPTORS_TO_TYPE.contains(f.getMessageType())) {
            // all wrapper types have a single field, so we can get the first field in the message's schema
            return schemaFinalizer.apply(makeSchemaNullable(getFieldSchema(f.getMessageType().getFields().get(0), recursionDepths, wrappedPrimitivesAsRecords, updatedPath, maxRecursionDepth,
                timestampsAsRecords)));
          }
          if (!timestampsAsRecords && Timestamp.getDescriptor().equals(f.getMessageType())) {
            // Handle timestamps as long with logical type
            return schemaFinalizer.apply(makeSchemaNullable(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG))));
          }
          // if message field is repeated (like a list), elements are non-null
          if (f.isRepeated()) {
            return schemaFinalizer.apply(getMessageSchema(f.getMessageType(), recursionDepths, wrappedPrimitivesAsRecords, updatedPath, maxRecursionDepth, timestampsAsRecords));
          }
          // otherwise we create a nullable field schema
          return schemaFinalizer.apply(makeSchemaNullable(getMessageSchema(f.getMessageType(), recursionDepths, wrappedPrimitivesAsRecords, updatedPath, maxRecursionDepth, timestampsAsRecords)));
        case GROUP: // groups are deprecated
        default:
          throw new RuntimeException("Unexpected type: " + f.getType());
      }
    }

    private static Schema makeSchemaNullable(Schema schema) {
      return Schema.createUnion(Arrays.asList(NULL, schema));
    }

    private Object getDefault(Descriptors.FieldDescriptor f) {
      if (f.isRepeated()) { // empty array as repeated fields' default value
        return Collections.emptyList();
      }
      if (f.getContainingOneof() != null) {
        // fields inside oneof are nullable
        return null;
      }

      switch (f.getType()) { // generate default for type
        case BOOL:
          return false;
        case FLOAT:
          return 0.0F;
        case DOUBLE:
          return 0.0D;
        case INT32:
        case UINT32:
        case SINT32:
        case FIXED32:
        case SFIXED32:
        case INT64:
        case SINT64:
        case FIXED64:
        case SFIXED64:
          return 0;
        case UINT64:
          return "\u0000"; // requires bytes for decimal type
        case STRING:
        case BYTES:
          return "";
        case ENUM:
          return f.getEnumType().getValues().get(0).getName();
        case MESSAGE:
          return Schema.Field.NULL_VALUE;
        case GROUP: // groups are deprecated
        default:
          throw new RuntimeException("Unexpected type: " + f.getType());
      }
    }

    private Descriptors.FieldDescriptor[] getOrderedFields(Schema schema, Message message) {
      Descriptors.Descriptor descriptor = message.getDescriptorForType();
      return FIELD_CACHE.computeIfAbsent(Pair.of(schema, descriptor), key -> {
        Descriptors.FieldDescriptor[] fields = new Descriptors.FieldDescriptor[key.getLeft().getFields().size()];
        for (Schema.Field f : key.getLeft().getFields()) {
          fields[f.pos()] = key.getRight().findFieldByName(f.name());
        }
        return fields;
      });
    }

    private Object convertObject(Schema schema, Object value) {
      if (value == null) {
        return null;
      }
      // if we've reached max recursion depth in the provided schema, write out message to bytes
      if (RECURSION_OVERFLOW_SCHEMA.getFullName().equals(schema.getFullName())) {
        GenericData.Record overflowRecord = new GenericData.Record(schema);
        Message messageValue = (Message) value;
        overflowRecord.put(OVERFLOW_DESCRIPTOR_FIELD_NAME, messageValue.getDescriptorForType().getFullName());
        overflowRecord.put(OVERFLOW_BYTES_FIELD_NAME, ByteBuffer.wrap(messageValue.toByteArray()));
        return overflowRecord;
      }

      switch (schema.getType()) {
        case ARRAY:
          List<Object> arrayValue = (List<Object>) value;
          List<Object> arrayCopy = new GenericData.Array<>(arrayValue.size(), schema);
          for (Object obj : arrayValue) {
            arrayCopy.add(convertObject(schema.getElementType(), obj));
          }
          return arrayCopy;
        case BYTES:
          ByteBuffer byteBufferValue;
          if (value instanceof ByteString) {
            byteBufferValue = ((ByteString) value).asReadOnlyByteBuffer();
          } else if (value instanceof Message) {
            byteBufferValue = ((ByteString) getWrappedValue(value)).asReadOnlyByteBuffer();
          } else {
            byteBufferValue = (ByteBuffer) value;
          }
          int start = byteBufferValue.position();
          int length = byteBufferValue.limit() - start;
          byte[] bytesCopy = new byte[length];
          byteBufferValue.get(bytesCopy, 0, length);
          byteBufferValue.position(start);
          return ByteBuffer.wrap(bytesCopy, 0, length);
        case ENUM:
          return GenericData.get().createEnum(value.toString(), schema);
        case FIXED:
          if (value instanceof Long) {
            // convert the long to its unsigned value
            Conversions.DecimalConversion converter = new Conversions.DecimalConversion();
            return converter.toFixed(new BigDecimal(toUnsignedBigInteger((Long) value)), schema, schema.getLogicalType());
          }
          return GenericData.get().createFixed(null, ((GenericFixed) value).bytes(), schema);
        case BOOLEAN:
        case DOUBLE:
        case FLOAT:
        case INT:
          if (value instanceof Message) {
            return getWrappedValue(value);
          }
          return value; // immutable
        case LONG:
          Object tmpValue = value;
          if (value instanceof Message) {
            // check if this is a Timestamp
            if (schema.getLogicalType().equals(LogicalTypes.timestampMicros())) {
              return Timestamps.toMicros((Timestamp) value);
            } else {
              tmpValue = getWrappedValue(value);
            }
          }
          // unsigned ints need to be casted to long
          if (tmpValue instanceof Integer) {
            tmpValue = new Long((Integer) tmpValue);
          }
          return tmpValue;
        case MAP:
          Map<Object, Object> mapValue = (Map) value;
          Map<Object, Object> mapCopy = new HashMap<>(mapValue.size());
          for (Map.Entry<Object, Object> entry : mapValue.entrySet()) {
            mapCopy.put(convertObject(STRING_SCHEMA, entry.getKey()), convertObject(schema.getValueType(), entry.getValue()));
          }
          return mapCopy;
        case NULL:
          return null;
        case RECORD:
          GenericData.Record newRecord = new GenericData.Record(schema);
          Message messageValue = (Message) value;
          for (Schema.Field f : schema.getFields()) {
            int position = f.pos();
            Descriptors.FieldDescriptor fieldDescriptor = getOrderedFields(schema, messageValue)[position];
            Object convertedValue;
            if (fieldDescriptor.getType() == Descriptors.FieldDescriptor.Type.MESSAGE && !fieldDescriptor.isRepeated() && !messageValue.hasField(fieldDescriptor)) {
              convertedValue = null;
            } else {
              convertedValue = convertObject(f.schema(), messageValue.getField(fieldDescriptor));
            }
            newRecord.put(position, convertedValue);
          }
          return newRecord;
        case STRING:
          if (value instanceof String) {
            return value;
          } else if (value instanceof StringValue) {
            return ((StringValue) value).getValue();
          } else {
            return new Utf8(value.toString());
          }
        case UNION:
          // Unions only occur for nullable fields when working with proto + avro and null is the first schema in the union
          return convertObject(schema.getTypes().get(1), value);
        default:
          throw new HoodieException("Proto to Avro conversion failed for schema \"" + schema + "\" and value \"" + value + "\"");
      }
    }

    private static BigInteger toUnsignedBigInteger(long input) {
      if (input >= 0L) {
        return BigInteger.valueOf(input);
      } else {
        int upper = (int) (input >>> 32);
        int lower = (int) input;
        // return (upper << 32) + lower
        return (BigInteger.valueOf(Integer.toUnsignedLong(upper))).shiftLeft(32).add(BigInteger.valueOf(Integer.toUnsignedLong(lower)));
      }
    }

    /**
     * Returns the wrapped field, assumes all wrapped fields have a single value
     * @param value wrapper message like {@link Int32Value} or {@link StringValue}
     * @return the wrapped object
     */
    private Object getWrappedValue(Object value) {
      Message valueAsMessage = (Message) value;
      return valueAsMessage.getField(valueAsMessage.getDescriptorForType().getFields().get(0));
    }

    private String getNamespace(String descriptorFullName) {
      int lastDotIndex = descriptorFullName.lastIndexOf('.');
      return descriptorFullName.substring(0, lastDotIndex);
    }

    private String appendFieldNameToPath(String existingPath, String fieldName) {
      return existingPath + "." + fieldName;
    }

    private static class SchemaCacheKey {
      private final String className;
      private final boolean flattenWrappedPrimitives;
      private final int maxRecursionDepth;

      SchemaCacheKey(Class clazz, boolean flattenWrappedPrimitives, int maxRecursionDepth) {
        this.className = clazz.getName();
        this.flattenWrappedPrimitives = flattenWrappedPrimitives;
        this.maxRecursionDepth = maxRecursionDepth;
      }

      @Override
      public boolean equals(Object o) {
        if (this == o) {
          return true;
        }
        if (o == null || getClass() != o.getClass()) {
          return false;
        }
        SchemaCacheKey that = (SchemaCacheKey) o;
        return flattenWrappedPrimitives == that.flattenWrappedPrimitives && maxRecursionDepth == that.maxRecursionDepth && className.equals(that.className);
      }

      @Override
      public int hashCode() {
        return Objects.hash(className, flattenWrappedPrimitives, maxRecursionDepth);
      }
    }
  }
}
