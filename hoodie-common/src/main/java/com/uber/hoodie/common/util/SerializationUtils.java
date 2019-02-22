/*
 *  Copyright (c) 2018 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.uber.hoodie.common.util;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.twitter.chill.EmptyScalaKryoInstantiator;
import com.uber.hoodie.exception.HoodieSerializationException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicReference;


/**
 * {@link SerializationUtils} class internally uses {@link Kryo} serializer for serializing
 * / deserializing objects.
 */
public class SerializationUtils {

  // Caching kryo serializer to avoid creating kryo instance for every serde operation
  private static final AtomicReference<KryoSerializerInstance> serializerRef =
      new AtomicReference<KryoSerializerInstance>();

  // Serialize
  //-----------------------------------------------------------------------

  /**
   * <p>Serializes an {@code Object} to a byte array for storage/serialization.</p>
   *
   * @param obj the object to serialize to bytes
   * @return a byte[] with the converted Serializable
   * @throws IOException if the serialization fails
   */
  public static byte[] serialize(final Object obj) throws IOException {
    final KryoSerializerInstance serializerInstance = borrowKryoSerializerInstance();
    try {
      return serializerInstance.serialize(obj);
    } finally {
      releaseSerializerInstance(serializerInstance);
    }
  }

  // Deserialize
  //-----------------------------------------------------------------------

  /**
   * <p> Deserializes a single {@code Object} from an array of bytes. </p>
   *
   * <p> If the call site incorrectly types the return value, a {@link ClassCastException} is thrown
   * from the call site. Without Generics in this declaration, the call site must type cast and can
   * cause the same ClassCastException. Note that in both cases, the ClassCastException is in the
   * call site, not in this method. </p>
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
    final KryoSerializerInstance serializerInstance = borrowKryoSerializerInstance();
    try {
      return (T) serializerInstance.deserialize(objectData);
    } finally {
      releaseSerializerInstance(serializerInstance);
    }
  }

  // Returns cached kryo serializer instance if available otherwise creates a new copy of it.
  private static KryoSerializerInstance borrowKryoSerializerInstance() {
    final KryoSerializerInstance kryoSerializerInstance = SerializationUtils.serializerRef
        .getAndSet(null);
    if (kryoSerializerInstance == null) {
      return new KryoSerializerInstance();
    }
    return kryoSerializerInstance;
  }

  // Adds kryo serializer into local cache
  private static void releaseSerializerInstance(KryoSerializerInstance kryoSerializerInstance) {
    SerializationUtils.serializerRef.compareAndSet(null, kryoSerializerInstance);
  }

  private static class KryoSerializerInstance implements Serializable {

    private final Kryo kryo;
    // Caching ByteArrayOutputStream to avoid recreating it for every operation
    private final ByteArrayOutputStream baos;

    KryoSerializerInstance() {
      EmptyScalaKryoInstantiator kryoInstantiator = new EmptyScalaKryoInstantiator();
      kryo = kryoInstantiator.newKryo();
      baos = new ByteArrayOutputStream(1024);
      kryo.setRegistrationRequired(false);
      kryo.register(ObjectWrapper.class);
    }

    byte[] serialize(Object obj) throws IOException {
      kryo.reset();
      baos.reset();
      Output output = new Output(baos);
      ObjectWrapper wrapper = new ObjectWrapper(obj);
      this.kryo.writeObject(output, wrapper);
      output.close();
      return baos.toByteArray();
    }

    Object deserialize(byte[] objectData) {
      return this.kryo.readObject(new Input(objectData), ObjectWrapper.class).value;
    }

    // Helper class for wrapping object to be serialized.
    static final class ObjectWrapper {

      Object value;

      ObjectWrapper(Object value) {
        this.value = value;
      }
    }
  }
}
