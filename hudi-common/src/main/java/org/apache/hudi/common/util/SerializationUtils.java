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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;

/**
 * {@link SerializationUtils} class internally uses {@link Kryo} serializer for serializing / deserializing objects.
 */
public class SerializationUtils {

  // Caching kryo serializer to avoid creating kryo instance for every serde operation
  private static final ThreadLocal<KryoSerializerInstance> SERIALIZER_REF =
      ThreadLocal.withInitial(KryoSerializerInstance::new);

  private static Pair<String, Serializer<?>> SERIALIZER_REGISTER = null;

  public static void setOverallRegister(String className, Serializer<?> serializer) {
    if (SERIALIZER_REGISTER == null) {
      SERIALIZER_REGISTER = Pair.of(className, serializer);
    }
  }

  // Serialize
  // -----------------------------------------------------------------------

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

      Kryo kryo = new Kryo();
      // ensure that kryo doesn't fail if classes are not registered with kryo.
      kryo.setRegistrationRequired(false);
      // This would be used for object initialization if nothing else works out.
      kryo.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
      // Handle cases where we may have an odd classloader setup like with libjars
      // for hadoop
      kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
      if (SERIALIZER_REGISTER != null) {
        try {
          kryo.register(Class.forName(SERIALIZER_REGISTER.getLeft()), SERIALIZER_REGISTER.getRight());
        } catch (ClassNotFoundException e) {
          throw new HoodieException(e);
        }
      }
      return kryo;
    }

  }
}
