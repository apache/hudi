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

import org.apache.hudi.avro.GenericAvroSerializer;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.TreeSet;

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
   */
  public static <T> T deserialize(final byte[] objectData) {
    if (objectData == null) {
      throw new IllegalArgumentException("The byte[] must not be null");
    }
    return (T) SERIALIZER_REF.get().deserialize(objectData);
  }

  private static class KryoSerializerInstance implements Serializable {
    public static final int KRYO_SERIALIZER_INITIAL_BUFFER_SIZE = 1048576;
    private final Kryo kryo;
    // Caching ByteArrayOutputStream to avoid recreating it for every operation
    private final ByteArrayOutputStream baos;

    KryoSerializerInstance() {
      KryoInstantiator kryoInstantiator = new KryoInstantiator();
      kryo = kryoInstantiator.newKryo();
      baos = new ByteArrayOutputStream(KRYO_SERIALIZER_INITIAL_BUFFER_SIZE);
      kryo.setRegistrationRequired(false);
    }

    byte[] serialize(Object obj) {
      kryo.reset();
      baos.reset();
      try (Output output = new Output(baos)) {
        this.kryo.writeClassAndObject(output, obj);
      }
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
  public static class KryoInstantiator implements Serializable {

    public Kryo newKryo() {
      Kryo kryo = new Kryo();

      // This instance of Kryo should not require prior registration of classes
      kryo.setRegistrationRequired(false);
      kryo.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
      // Handle cases where we may have an odd classloader setup like with libjars
      // for hadoop
      kryo.setClassLoader(Thread.currentThread().getContextClassLoader());

      // Register Hudi's classes
      new HoodieCommonKryoRegistrar().registerClasses(kryo);

      // Register serializers
      kryo.register(Utf8.class, new AvroUtf8Serializer());
      kryo.register(GenericData.Fixed.class, new GenericAvroSerializer<>());
      kryo.register(IndexedRecord.class, new GenericAvroSerializer<>());
      kryo.register(GenericData.Record.class, new GenericAvroSerializer<>());
      kryo.register(HoodieFileGroupId.class);
      kryo.register(FileSlice.class);
      kryo.register(HoodieBaseFile.class);
      kryo.register(HoodieLogFile.class);
      kryo.register(TreeSet.class);

      return kryo;
    }

  }

  /**
   * NOTE: This {@link Serializer} could deserialize instance of {@link Utf8} serialized
   *       by implicitly generated Kryo serializer (based on {@link com.esotericsoftware.kryo.serializers.FieldSerializer}
   */
  public static class AvroUtf8Serializer extends Serializer<Utf8> {

    @SuppressWarnings("unchecked")
    @Override
    public void write(Kryo kryo, Output output, Utf8 utf8String) {
      Serializer<byte[]> bytesSerializer = kryo.getDefaultSerializer(byte[].class);
      bytesSerializer.write(kryo, output, utf8String.getBytes());
    }

    @SuppressWarnings("unchecked")
    @Override
    public Utf8 read(Kryo kryo, Input input, Class<Utf8> type) {
      Serializer<byte[]> bytesSerializer = kryo.getDefaultSerializer(byte[].class);
      byte[] bytes = bytesSerializer.read(kryo, input, byte[].class);
      return new Utf8(bytes);
    }
  }
}
