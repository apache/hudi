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
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.UnresolvedUnionException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.protobuf.ProtobufData;
import org.apache.avro.specific.SpecificData;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.protobuf.Descriptors.FieldDescriptor.Type.BYTES;

/**
 * A utility class to help translate
 */
public class ProtobufConversionUtil {

  /**
   * Creates an Avro {@link Schema} for the provided class. Assumes that the class is a protobuf {@link Message}.
   * @param clazz The protobuf class
   * @param flattenWrappedPrimitives set to true to treat wrapped primitives like nullable fields instead of nested messages.
   * @return An Avro schema
   */
  public static Schema getAvroSchemaForMessageClass(Class clazz, boolean flattenWrappedPrimitives) {
    return ConvertingProtobufData.getWithFlattening(flattenWrappedPrimitives).getSchema(clazz);
  }

  /**
   * Converts the provided {@link Message} into an avro {@link GenericRecord} with the provided schema.
   * @param schema target schema to convert into
   * @param message the source message to convert
   * @return an Avro GenericRecord
   */
  public static GenericRecord convertToAvro(Schema schema, Message message) {
    return ConvertingProtobufData.convert(schema, message);
  }

  /**
   * This class extends {@link ProtobufData} so we can:
   * 1. Customize how schemas are generated for protobufs. We treat Enums as strings and provide an option to treat wrapped primitives like {@link Int32Value} and {@link StringValue} as messages
   * (default behavior) or as nullable versions of those primitives.
   * 2. Convert directly from a protobuf {@link Message} to a {@link GenericRecord} while properly handling enums and wrapped primitives mentioned above.
   */
  private static class ConvertingProtobufData extends ProtobufData {
    private static final Schema STRINGS = Schema.create(Schema.Type.STRING);

    private static final ConvertingProtobufData NEST_WRAPPER_INSTANCE = new ConvertingProtobufData(false);
    private static final ConvertingProtobufData FLATTEN_WRAPPER_INSTANCE = new ConvertingProtobufData(true);
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

    private final boolean flattenWrappedPrimitives;

    private ConvertingProtobufData(final boolean flattenWrappers) {
      this.flattenWrappedPrimitives = flattenWrappers;
    }

    /**
     * Return one of two singletons based on flattening behavior.
     */
    static ConvertingProtobufData getWithFlattening(final boolean shouldFlattenWrappers) {
      return shouldFlattenWrappers ? FLATTEN_WRAPPER_INSTANCE : NEST_WRAPPER_INSTANCE;
    }

    /**
     * Overrides the default behavior to avoid confusion.
     * @return throws exception
     */
    public static ConvertingProtobufData get() {
      throw new UnsupportedOperationException("Do not directly call `get` on ConvertingProtobufData, use getWithFlattening");
    }

    static GenericRecord convert(Schema schema, Message message) {
      // the conversion infers the flattening based on the schema so the singleton used doesn not make a difference
      return (GenericRecord) FLATTEN_WRAPPER_INSTANCE.deepCopy(schema, (Object) message);
    }

    @Override
    public Schema getSchema(Descriptors.FieldDescriptor f) {
      Schema s = this.getFileSchema(f);
      if (f.isRepeated()) {
        s = Schema.createArray(s);
      }

      return s;
    }

    private Schema getFileSchema(Descriptors.FieldDescriptor f) {
      Schema result;
      switch (f.getType()) {
        case BOOL:
          return Schema.create(Schema.Type.BOOLEAN);
        case FLOAT:
          return Schema.create(Schema.Type.FLOAT);
        case DOUBLE:
          return Schema.create(Schema.Type.DOUBLE);
        case ENUM:
          return getSchema(f.getEnumType());
        case STRING:
          Schema s = Schema.create(Schema.Type.STRING);
          GenericData.setStringType(s, GenericData.StringType.String);
          return s;
        case BYTES:
          return Schema.create(Schema.Type.BYTES);
        case INT32:
        case UINT32:
        case SINT32:
        case FIXED32:
        case SFIXED32:
          return Schema.create(Schema.Type.INT);
        case INT64:
        case UINT64:
        case SINT64:
        case FIXED64:
        case SFIXED64:
          return Schema.create(Schema.Type.LONG);
        case MESSAGE:
          if (flattenWrappedPrimitives && WRAPPER_DESCRIPTORS_TO_TYPE.containsKey(f.getMessageType())) {
            // all wrapper types have a single field so we can get the first field in the message's schema
            return Schema.createUnion(Arrays.asList(NULL, getSchema(f.getMessageType().getFields().get(0))));
          }
          result = getSchema(f.getMessageType());
          if (f.isOptional()) {
            // wrap optional record fields in a union with null
            result = Schema.createUnion(Arrays.asList(NULL, result));
          }
          return result;
        case GROUP: // groups are deprecated
        default:
          throw new RuntimeException("Unexpected type: " + f.getType());
      }
    }

    @Override
    public Object newRecord(Object old, Schema schema) {
      // override the default ProtobufData behavior of creating a new proto instance
      // instead we create a new avro instance and rely on the field indexes matching between the proto and our new generic record's schema
      return new GenericData.Record(schema);
    }

    @Override
    protected Object getRecordState(Object r, Schema s) {
      // get record state is called on the original (proto message) and new (avro generic record) so we need to handle based on the class
      if (r instanceof Message) {
        // gets field values in order
        return super.getRecordState(r, s);
      }
      // the record state of the new object is never used for our conversion use case so we return null
      return null;
    }

    /**
     * Overrides the default behavior to handle flattening during conversion.
     * @param union Schema for the union
     * @param datum Value corresponding to one of the union types
     * @return the position of the schema for the input datum's type within the union's types
     */
    @Override
    public int resolveUnion(Schema union, Object datum) {
      try {
        return super.resolveUnion(union, datum);
      } catch (UnresolvedUnionException ex) {
        if (datum instanceof Message) {
          // in the case of flattening we need to try looking for the index of the underlying type
          Integer i = union.getIndexNamed(WRAPPER_DESCRIPTORS_TO_TYPE.get(((Message) datum).getDescriptorForType()).getName());
          if (i != null) {
            return i;
          }
        }
        // if we're not dealing with a nested proto Message or the underlying type wasn't found, allow original exception to be thrown
        throw ex;
      }
    }

    @Override
    protected void setField(Object record, String name, int position, Object value, Object state) {
      SpecificData.get().setField(record, name, position, value);
    }

    @Override
    public Object createString(Object value) {
      if (value instanceof StringValue) {
        return ((StringValue) value).getValue();
      }
      return super.createString(value);
    }

    @Override
    protected boolean isBytes(Object datum) {
      return datum instanceof ByteBuffer || datum instanceof ByteString;
    }

    /**
     * Note this is cloned from {@link GenericData#deepCopy(Schema, Object)} so we can override the copying of the raw values to flatten objects when needed and properly handle proto byte array types.
     * Makes a deep copy of a value given its schema.
     * <P>
     * Logical types are converted to raw types, copied, then converted back.
     *
     * @param schema the schema of the value to deep copy.
     * @param value  the value to deep copy.
     * @return a deep copy of the given value.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public <T> T deepCopy(Schema schema, T value) {
      if (value == null) {
        return null;
      }
      LogicalType logicalType = schema.getLogicalType();
      if (logicalType == null) { // not a logical type -- use raw copy
        return (T) deepCopyRaw(schema, value);
      }
      Conversion conversion = getConversionByClass(value.getClass(), logicalType);
      if (conversion == null) { // no conversion defined -- try raw copy
        return (T) deepCopyRaw(schema, value);
      }
      // logical type with conversion: convert to raw, copy, then convert back to
      // logical
      Object raw = Conversions.convertToRawType(value, schema, logicalType, conversion);
      Object copy = deepCopyRaw(schema, raw); // copy raw
      return (T) Conversions.convertToLogicalType(copy, schema, logicalType, conversion);
    }

    private Object deepCopyRaw(Schema schema, Object value) {
      if (value == null) {
        return null;
      }

      switch (schema.getType()) {
        case ARRAY:
          List<Object> arrayValue = (List<Object>) value;
          List<Object> arrayCopy = new GenericData.Array<>(arrayValue.size(), schema);
          for (Object obj : arrayValue) {
            arrayCopy.add(deepCopy(schema.getElementType(), obj));
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
          return createEnum(value.toString(), schema);
        case FIXED:
          return createFixed(null, ((GenericFixed) value).bytes(), schema);
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
            mapCopy.put(deepCopy(STRINGS, entry.getKey()), deepCopy(schema.getValueType(), entry.getValue()));
          }
          return mapCopy;
        case NULL:
          return null;
        case RECORD:
          Object oldState = getRecordState(value, schema);
          Object newRecord = newRecord(null, schema);
          for (Schema.Field f : schema.getFields()) {
            int pos = f.pos();
            String name = f.name();
            Object newValue = deepCopy(f.schema(), getField(value, name, pos, oldState));
            setField(newRecord, name, pos, newValue, null);
          }
          return newRecord;
        case STRING:
          return createString(value);
        case UNION:
          return deepCopy(schema.getTypes().get(resolveUnion(schema, value)), value);
        default:
          throw new AvroRuntimeException("Deep copy failed for schema \"" + schema + "\" and value \"" + value + "\"");
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
  }
}