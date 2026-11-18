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

package org.apache.hudi.utilities.util;

import org.apache.hudi.exception.HoodieException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.Serializable;

/**
 * A broadcast payload that survives ANY {@code spark.serializer}, by carrying its value as
 * Java-serialized bytes.
 *
 * <p>A Spark broadcast is serialized with the configured serializer, and some launchers set
 * Kryo unconditionally WITHOUT {@code HoodieSparkKryoRegistrar} (their {@code SparkConf} comes
 * from {@code registerKryoClasses}, which sets the serializer and nothing else). Bare Kryo never
 * calls {@code writeObject}/{@code readObject} and skips transient fields, so an object graph
 * whose null-safety lives in those methods -- {@code HadoopStorageConfiguration}, and through it
 * the meta client inside a shipped active timeline -- deserializes on the executor with a null
 * Hadoop conf and NPEs on first use. With the registrar configured the raw graph does survive
 * (it registers {@code HadoopStorageConfiguration} under Kryo's {@code JavaSerializer}; that is
 * what {@code TestPhaseTwoPayloadSerialization} exercises), but the validator cannot assume every
 * launcher sets it, so it ships a payload that is safe under any serializer configuration.
 * Reproducible only on a cluster: local-mode broadcasts hand back the driver's object without a
 * round trip, which is why a green functional suite proves nothing about this. Pinned by
 * {@code TestShippedTimelineSurvivesSerialization}'s registrar-less Kryo round trips.
 *
 * <p>To every serializer this object is one {@code byte[]}, which nothing can mangle; the real
 * graph round-trips through Java serialization inside it, the contract {@code writeObject}-based
 * classes are written against.
 *
 * <p>{@link #get()} deserializes once per deserialized instance and caches -- a broadcast value
 * is deserialized once per executor and shared by its tasks, so the cost is one deserialization
 * per executor, same as the raw broadcast had. The constructor retains only the bytes, so driver
 * broadcast sizing does not walk the value's transient object graph. {@code get()} is synchronized
 * because those tasks share the instance; it is called once per partition, so the lock is
 * uncontended in practice.
 */
public final class JavaSerializedPayload<T extends Serializable> implements Serializable {

  private static final long serialVersionUID = 1L;

  private final byte[] bytes;
  private transient T value;

  public JavaSerializedPayload(T value) {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(buffer)) {
      out.writeObject(value);
    } catch (IOException e) {
      throw new HoodieException("Could not serialize the broadcast payload", e);
    }
    this.bytes = buffer.toByteArray();
  }

  @SuppressWarnings("unchecked")
  public synchronized T get() {
    if (value == null) {
      try (ObjectInputStream in = new ContextClassLoaderObjectInputStream(
          new ByteArrayInputStream(bytes))) {
        value = (T) in.readObject();
      } catch (IOException | ClassNotFoundException e) {
        throw new HoodieException("Could not deserialize the broadcast payload", e);
      }
    }
    return value;
  }

  /**
   * Resolves classes against the task's context class loader first: on an executor the payload's
   * classes live in the application class loader, which the default stream cannot always see.
   */
  private static final class ContextClassLoaderObjectInputStream extends ObjectInputStream {
    ContextClassLoaderObjectInputStream(InputStream in) throws IOException {
      super(in);
    }

    @Override
    protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
      try {
        return Class.forName(desc.getName(), false, Thread.currentThread().getContextClassLoader());
      } catch (ClassNotFoundException e) {
        return super.resolveClass(desc);
      }
    }
  }
}
