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

package org.apache.hudi.common.util;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.exception.HoodieSerializationException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.esotericsoftware.reflectasm.ConstructorAccess;
import org.objenesis.instantiator.ObjectInstantiator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;

/**
 * {@link SerializationUtils} class internally uses {@link Kryo} serializer for serializing / deserializing objects.
 */
public class SerializationUtils {

  // Caching kryo serializer to avoid creating kryo instance for every serde operation
  private static final ThreadLocal<KryoSerializerInstance> SERIALIZER_REF =
      ThreadLocal.withInitial(KryoSerializerInstance::new);

  /**
   * <p>
   * Serializes an {@code Object} to a byte array for storage/serialization.
   * </p>
   *
   * @param obj the object to serialize to bytes
   * @return a byte[] with the converted Serializable
   * @throws IOException if the serialization fails
   */
  public static byte[] serialize(final Object obj) throws IOException {
    return SERIALIZER_REF.get().serialize(obj);
  }

  // Deserialize
  // -----------------------------------------------------------------------

  /**
   * <p>
   * Deserializes a single {@code Object} from an array of bytes.
   * </p>
   *
   * <p>
   * If the call site incorrectly types the return value, a {@link ClassCastException} is thrown from the call site.
   * Without Generics in this declaration, the call site must type cast and can cause the same ClassCastException. Note
   * that in both cases, the ClassCastException is in the call site, not in this method.
   * </p>
   *
   * @param <T> the object type to be deserialized
   * @param objectData the serialized object, must not be null
   * @return the deserialized object
   * @throws IllegalArgumentException if {@code objectData} is {@code null}
   * @throws HoodieSerializationException (runtime) if the serialization fails
   */
  public static <T> T deserialize(final byte[] objectData) {
    if (objectData == null) {
      throw new IllegalArgumentException("The byte[] must not be null");
    }
    return (T) SERIALIZER_REF.get().deserialize(objectData);
  }

  public static class HoodieKeySerializer extends Serializer<HoodieKey> implements Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public void write(Kryo kryo, Output output, HoodieKey hoodieKey) {
      output.writeString(hoodieKey.getRecordKey());
      output.writeString(hoodieKey.getPartitionPath());
    }

    @Override
    public HoodieKey read(Kryo kryo, Input input, Class<HoodieKey> type) {
      return new HoodieKey(input.readString(), input.readString());
    }
  }

  public static class OverwriteWithLatestPayloadSerializer extends Serializer<OverwriteWithLatestAvroPayload> implements Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public void write(Kryo kryo, Output output, OverwriteWithLatestAvroPayload payload) {
      output.writeInt(payload.recordBytes.length);
      output.writeBytes(payload.recordBytes);
      kryo.writeClassAndObject(output, payload.orderingVal);
    }

    @Override
    public OverwriteWithLatestAvroPayload read(Kryo kryo, Input input, Class<OverwriteWithLatestAvroPayload> type) {
      int size = input.readInt();
      byte[] recordBytes = new byte[size];
      input.read(recordBytes);
      Comparable orderingVal = (Comparable) kryo.readClassAndObject(input);
      return new OverwriteWithLatestAvroPayload(recordBytes, orderingVal);
    }
  }

  public static class HoodieRecordLocationSerializer extends Serializer<HoodieRecordLocation> implements Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public void write(Kryo kryo, Output output, HoodieRecordLocation location) {
      output.writeString(location.getInstantTime());
      output.writeString(location.getFileId());
    }

    @Override
    public HoodieRecordLocation read(Kryo kryo, Input input, Class<HoodieRecordLocation> type) {
      return new HoodieRecordLocation(input.readString(), input.readString());
    }
  }

  public static class HoodieRecordSerializer extends Serializer<HoodieRecord> implements Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public void write(Kryo kryo, Output output, HoodieRecord record) {
      kryo.writeObject(output, record.getKey());
      kryo.writeClassAndObject(output, record.getData());
      kryo.writeObject(output, record.getNewLocation().get());
      kryo.writeObject(output, record.getCurrentLocation());
    }

    @Override
    public HoodieRecord read(Kryo kryo, Input input, Class<HoodieRecord> type) {
      HoodieKey key = kryo.readObject(input, HoodieKey.class);
      HoodieRecordPayload payload = (HoodieRecordPayload) kryo.readClassAndObject(input);
      HoodieRecordLocation newLocation = kryo.readObject(input, HoodieRecordLocation.class);
      HoodieRecordLocation currentLocation = kryo.readObject(input, HoodieRecordLocation.class);

      HoodieRecord record = new HoodieRecord(key, payload);
      record.setNewLocation(newLocation);
      record.setCurrentLocation(currentLocation);
      return record;
    }
  }

  public static class GenericDataRecordSerializer extends Serializer<GenericRecord> implements Serializable {
    private static final long serialVersionUID = 1L;

    private void serializeDatum(Output output, GenericRecord object) throws IOException {

      BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(output, null);
      Schema schema = object.getSchema();

      byte[] bytes = schema.toString().getBytes(StandardCharsets.UTF_8);
      output.writeInt(bytes.length);
      output.write(bytes);

      DatumWriter<GenericRecord> datumWriter = GenericData.get().createDatumWriter(schema);
      datumWriter.write(object, binaryEncoder);

      binaryEncoder.flush();

    }

    private GenericRecord deserializeDatum(Input input) throws IOException {
      int length = input.readInt();
      Schema schema = new Schema.Parser().parse(new String(input.readBytes(length)));

      BinaryDecoder binaryDecoder = DecoderFactory.get().directBinaryDecoder(input, null);
      DatumReader<GenericRecord> datumReader = GenericData.get().createDatumReader(schema);
      return datumReader.read(null, binaryDecoder);

    }

    @Override
    public void write(Kryo kryo, Output output, GenericRecord object) {
      try {
        serializeDatum(output, object);
      } catch (IOException e) {
        throw new RuntimeException();
      }
    }

    @Override
    public GenericRecord read(Kryo kryo, Input input, Class<GenericRecord> type) {
      try {
        return deserializeDatum(input);
      } catch (IOException e) {
        throw new RuntimeException();
      }
    }
  }

  private static class KryoSerializerInstance implements Serializable {
    public static final int KRYO_SERIALIZER_INITIAL_BUFFER_SIZE = 1048576;
    private final Kryo kryo;
    // Caching ByteArrayOutputStream to avoid recreating it for every operation
    private final ByteArrayOutputStream baos;

    KryoSerializerInstance() {
      KryoInstantiator kryoInstantiator = new KryoInstantiator();
      // TODO: this kryo instantiation is much slower than a simple way below
      //kryo = kryoInstantiator.newKryo();
      kryo = new Kryo();
      kryo.register(HoodieKey.class, new HoodieKeySerializer());
      kryo.register(GenericData.Record.class, new GenericDataRecordSerializer());
      kryo.register(HoodieRecord.class, new HoodieRecordSerializer());
      kryo.register(HoodieRecordLocationSerializer.class, new HoodieRecordLocationSerializer());
      kryo.register(OverwriteWithLatestAvroPayload.class, new OverwriteWithLatestPayloadSerializer());

      baos = new ByteArrayOutputStream(KRYO_SERIALIZER_INITIAL_BUFFER_SIZE);
      kryo.setRegistrationRequired(false);
    }

    byte[] serialize(Object obj) {
      kryo.reset();
      baos.reset();
      Output output = new Output(baos);
      this.kryo.writeClassAndObject(output, obj);
      output.close();
      return baos.toByteArray();
    }

    Object deserialize(byte[] objectData) {
      return this.kryo.readClassAndObject(new Input(objectData));
    }
  }

  /**
   * This class has a no-arg constructor, suitable for use with reflection instantiation. For Details checkout
   * com.twitter.chill.KryoBase.
   */
  private static class KryoInstantiator implements Serializable {

    public Kryo newKryo() {

      Kryo kryo = new KryoBase();
      // ensure that kryo doesn't fail if classes are not registered with kryo.
      kryo.setRegistrationRequired(true);
      // This would be used for object initialization if nothing else works out.
      kryo.setInstantiatorStrategy(new org.objenesis.strategy.StdInstantiatorStrategy());
      // Handle cases where we may have an odd classloader setup like with libjars
      // for hadoop
      kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
      return kryo;
    }

    private static class KryoBase extends Kryo {
      @Override
      protected Serializer newDefaultSerializer(Class type) {
        final Serializer serializer = super.newDefaultSerializer(type);
        if (serializer instanceof FieldSerializer) {
          final FieldSerializer fieldSerializer = (FieldSerializer) serializer;
          fieldSerializer.setIgnoreSyntheticFields(true);
        }
        return serializer;
      }

      @Override
      protected ObjectInstantiator newInstantiator(Class type) {
        return () -> {
          // First try reflectasm - it is fastest way to instantiate an object.
          try {
            final ConstructorAccess access = ConstructorAccess.get(type);
            return access.newInstance();
          } catch (Throwable t) {
            // ignore this exception. We may want to try other way.
          }
          // fall back to java based instantiation.
          try {
            final Constructor constructor = type.getConstructor();
            constructor.setAccessible(true);
            return constructor.newInstance();
          } catch (NoSuchMethodException | IllegalAccessException | InstantiationException
              | InvocationTargetException e) {
            // ignore this exception. we will fall back to default instantiation strategy.
          }
          return super.getInstantiatorStrategy().newInstantiatorOf(type).newInstance();
        };
      }
    }
  }
}
