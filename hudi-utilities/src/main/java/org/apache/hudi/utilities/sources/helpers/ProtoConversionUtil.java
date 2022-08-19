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
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.FloatValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.google.protobuf.Message;
import com.google.protobuf.StringValue;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * A utility class to help translate from Proto to Avro.
 */
public class ProtoConversionUtil {

  /**
   * Creates an Avro {@link Schema} for the provided class. Assumes that the class is a protobuf {@link Message}.
   * @param clazz The protobuf class
   * @param flattenWrappedPrimitives set to true to treat wrapped primitives like nullable fields instead of nested messages.
   * @return An Avro schema
   */
  public static Schema getAvroSchemaForMessageClass(Class clazz, boolean flattenWrappedPrimitives) {
    return AvroSupport.get().getSchema(clazz, flattenWrappedPrimitives);
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
    private static final AvroSupport INSTANCE = new AvroSupport();
    private static final Map<Pair<Class, Boolean>, Schema> SCHEMA_CACHE = new ConcurrentHashMap<>();
    private static final Map<Pair<Schema, Descriptors.Descriptor>, Descriptors.FieldDescriptor[]> FIELD_CACHE = new ConcurrentHashMap<>();


    private static final Schema STRINGS = Schema.create(Schema.Type.STRING);

    private static final Schema NULL = Schema.create(Schema.Type.NULL);
    private static final Map<Descriptors.Descriptor, Schema.Type> WRAPPER_DESCRIPTORS_TO_TYPE = getWrapperDescriptorsToType();

    private static Map<Descriptors.Descriptor, Schema.Type> getWrapperDescriptorsToType() {
      Map<Descriptors.Descriptor, Schema.Type> wrapperDescriptorsToType = new HashMap<>();
      wrapperDescriptorsToType.put(StringValue.getDescriptor(), Schema.Type.STRING);
      wrapperDescriptorsToType.put(Int32Value.getDescriptor(), Schema.Type.INT);
      wrapperDescriptorsToType.put(UInt32Value.getDescriptor(), Schema.Type.INT);
      wrapperDescriptorsToType.put(Int64Value.getDescriptor(), Schema.Type.LONG);
      wrapperDescriptorsToType.put(UInt64Value.getDescriptor(), Schema.Type.LONG);
      wrapperDescriptorsToType.put(BoolValue.getDescriptor(), Schema.Type.BOOLEAN);
      wrapperDescriptorsToType.put(BytesValue.getDescriptor(), Schema.Type.BYTES);
      wrapperDescriptorsToType.put(DoubleValue.getDescriptor(), Schema.Type.DOUBLE);
      wrapperDescriptorsToType.put(FloatValue.getDescriptor(), Schema.Type.FLOAT);
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

    public Schema getSchema(Class c, boolean flattenWrappedPrimitives) {
      return SCHEMA_CACHE.computeIfAbsent(Pair.of(c, flattenWrappedPrimitives), key -> {
        try {
          Object descriptor = c.getMethod("getDescriptor").invoke(null);
          if (c.isEnum()) {
            return getEnumSchema((Descriptors.EnumDescriptor) descriptor);
          } else {
            return getMessageSchema((Descriptors.Descriptor) descriptor, new HashMap<>(), flattenWrappedPrimitives);
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
    }

    private Schema getEnumSchema(Descriptors.EnumDescriptor d) {
      List<String> symbols = new ArrayList<>(d.getValues().size());
      for (Descriptors.EnumValueDescriptor e : d.getValues()) {
        symbols.add(e.getName());
      }
      return Schema.createEnum(d.getName(), null, getNamespace(d.getFile(), d.getContainingType()), symbols);
    }

    private Schema getMessageSchema(Descriptors.Descriptor descriptor, Map<Descriptors.Descriptor, Schema> seen, boolean flattenWrappedPrimitives) {
      if (seen.containsKey(descriptor)) {
        return seen.get(descriptor);
      }
      Schema result = Schema.createRecord(descriptor.getName(), null,
          getNamespace(descriptor.getFile(), descriptor.getContainingType()), false);

      seen.put(descriptor, result);

      List<Schema.Field> fields = new ArrayList<>(descriptor.getFields().size());
      for (Descriptors.FieldDescriptor f : descriptor.getFields()) {
        fields.add(new Schema.Field(f.getName(), getFieldSchema(f, seen, flattenWrappedPrimitives), null, getDefault(f)));
      }
      result.setFields(fields);
      return result;
    }

    private Schema getFieldSchema(Descriptors.FieldDescriptor f, Map<Descriptors.Descriptor, Schema> seen, boolean flattenWrappedPrimitives) {
      Function<Schema, Schema> schemaFinalizer =  f.isRepeated() ? Schema::createArray : Function.identity();
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
        case UINT32:
        case SINT32:
        case FIXED32:
        case SFIXED32:
          return schemaFinalizer.apply(Schema.create(Schema.Type.INT));
        case INT64:
        case UINT64:
        case SINT64:
        case FIXED64:
        case SFIXED64:
          return schemaFinalizer.apply(Schema.create(Schema.Type.LONG));
        case MESSAGE:
          if (flattenWrappedPrimitives && WRAPPER_DESCRIPTORS_TO_TYPE.containsKey(f.getMessageType())) {
            // all wrapper types have a single field so we can get the first field in the message's schema
            return schemaFinalizer.apply(Schema.createUnion(Arrays.asList(NULL, getFieldSchema(f.getMessageType().getFields().get(0), seen, flattenWrappedPrimitives))));
          }
          // if message field is repeated (like a list), elements are non-null
          if (f.isRepeated()) {
            return schemaFinalizer.apply(getMessageSchema(f.getMessageType(), seen, flattenWrappedPrimitives));
          }
          // otherwise we create a nullable field schema
          return schemaFinalizer.apply(Schema.createUnion(Arrays.asList(NULL, getMessageSchema(f.getMessageType(), seen, flattenWrappedPrimitives))));
        case GROUP: // groups are deprecated
        default:
          throw new RuntimeException("Unexpected type: " + f.getType());
      }
    }

    private Object getDefault(Descriptors.FieldDescriptor f) {
      if (f.isRepeated()) { // empty array as repeated fields' default value
        return Collections.emptyList();
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
        case UINT64:
        case SINT64:
        case FIXED64:
        case SFIXED64:
          return 0;
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
          return GenericData.get().createFixed(null, ((GenericFixed) value).bytes(), schema);
        case BOOLEAN:
        case DOUBLE:
        case FLOAT:
        case INT:
        case LONG:
          if (value instanceof Message) {
            return getWrappedValue(value);
          }
          return value; // immutable
        case MAP:
          Map<Object, Object> mapValue = (Map) value;
          Map<Object, Object> mapCopy = new HashMap<>(mapValue.size());
          for (Map.Entry<Object, Object> entry : mapValue.entrySet()) {
            mapCopy.put(convertObject(STRINGS, entry.getKey()), convertObject(schema.getValueType(), entry.getValue()));
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

    /**
     * Returns the wrapped field, assumes all wrapped fields have a single value
     * @param value wrapper message like {@link Int32Value} or {@link StringValue}
     * @return the wrapped object
     */
    private Object getWrappedValue(Object value) {
      Message valueAsMessage = (Message) value;
      return valueAsMessage.getField(valueAsMessage.getDescriptorForType().getFields().get(0));
    }

    private String getNamespace(Descriptors.FileDescriptor fd, Descriptors.Descriptor containing) {
      DescriptorProtos.FileOptions filedOptions = fd.getOptions();
      String classPackage = filedOptions.hasJavaPackage() ? filedOptions.getJavaPackage() : fd.getPackage();
      String outer = "";
      if (!filedOptions.getJavaMultipleFiles()) {
        if (filedOptions.hasJavaOuterClassname()) {
          outer = filedOptions.getJavaOuterClassname();
        } else {
          outer = new File(fd.getName()).getName();
          outer = outer.substring(0, outer.lastIndexOf('.'));
          outer = toCamelCase(outer);
        }
      }
      StringBuilder inner = new StringBuilder();
      while (containing != null) {
        if (inner.length() == 0) {
          inner.insert(0, containing.getName());
        } else {
          inner.insert(0, containing.getName() + "$");
        }
        containing = containing.getContainingType();
      }
      String d1 = (!outer.isEmpty() || inner.length() != 0 ? "." : "");
      String d2 = (!outer.isEmpty() && inner.length() != 0 ? "$" : "");
      return classPackage + d1 + outer + d2 + inner;
    }

    private String toCamelCase(String s) {
      String[] parts = s.split("_");
      StringBuilder camelCaseString = new StringBuilder(s.length());
      for (String part : parts) {
        camelCaseString.append(cap(part));
      }
      return camelCaseString.toString();
    }

    private String cap(String s) {
      return s.substring(0, 1).toUpperCase() + s.substring(1).toLowerCase();
    }
  }
}