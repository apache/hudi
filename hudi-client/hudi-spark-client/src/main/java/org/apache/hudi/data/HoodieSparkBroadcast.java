/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.data;

import org.apache.hudi.common.data.HoodieBroadcast;
import org.apache.hudi.common.util.VisibleForTesting;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.serializer.JavaSerializer;
import org.apache.spark.serializer.SerializerInstance;

import java.io.Serializable;
import java.nio.ByteBuffer;

import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

/**
 * A {@link HoodieBroadcast} backed by a Spark {@link Broadcast}: tasks fetch the value once per executor
 * and share it.
 *
 * <p>The broadcast carries only the value serialized with Spark's closure serializer, so the value
 * round-trips exactly like a value captured in a task closure whichever serializer Spark uses for
 * broadcasts, and the driver's block manager holds only bytes. Each executor, including the driver JVM in
 * local mode, deserializes one copy for its tasks; the handle created on the driver returns the driver's
 * own instance.
 *
 * @param <T> type of the value
 */
public class HoodieSparkBroadcast<T> implements HoodieBroadcast<T> {

  private static final long serialVersionUID = 1L;

  private final Broadcast<SerializedValue<T>> broadcast;
  private transient T driverValue;

  private HoodieSparkBroadcast(Broadcast<SerializedValue<T>> broadcast, T driverValue) {
    this.broadcast = broadcast;
    this.driverValue = driverValue;
  }

  public static <T> HoodieSparkBroadcast<T> create(JavaSparkContext jsc, T value) {
    return new HoodieSparkBroadcast<>(jsc.broadcast(new SerializedValue<>(value)), value);
  }

  @Override
  public T value() {
    T value = driverValue;
    return value != null ? value : broadcast.value().get();
  }

  @Override
  public void destroy() {
    driverValue = null;
    broadcast.destroy();
  }

  /**
   * Holds the serialized bytes of a value and, once read, the value deserialized from them.
   */
  static class SerializedValue<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final ClassTag<Object> OBJECT_TAG = ClassTag$.MODULE$.AnyRef();

    private final byte[] bytes;
    private transient volatile T value;

    SerializedValue(T value) {
      ByteBuffer buffer = closureSerializer().serialize(value, OBJECT_TAG);
      this.bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
    }

    @SuppressWarnings("unchecked")
    T get() {
      T result = value;
      if (result == null) {
        synchronized (this) {
          result = value;
          if (result == null) {
            result = (T) closureSerializer().deserialize(ByteBuffer.wrap(bytes), OBJECT_TAG);
            value = result;
          }
        }
      }
      return result;
    }

    @VisibleForTesting
    T getCachedValue() {
      return value;
    }

    /**
     * Returns the serializer Spark uses for task closures, which resolves classes with the executor's
     * class loader whichever thread calls it.
     */
    private static SerializerInstance closureSerializer() {
      SparkEnv env = SparkEnv.get();
      return env != null ? env.closureSerializer().newInstance() : new JavaSerializer(new SparkConf(false)).newInstance();
    }
  }
}
