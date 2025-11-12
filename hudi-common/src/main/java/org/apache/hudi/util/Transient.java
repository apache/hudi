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

package org.apache.hudi.util;

import org.apache.hudi.common.function.SerializableSupplier;
import org.apache.hudi.common.function.ThrowingConsumer;

import javax.annotation.concurrent.ThreadSafe;

import java.io.Serializable;

import static org.apache.hudi.common.util.ValidationUtils.checkArgument;

/**
 * {@link Serializable} counterpart of {@link Lazy}
 *
 * @param <T> type of the object being held by {@link Transient}
 */
@ThreadSafe
public class Transient<T> implements Serializable {

  private SerializableSupplier<T> initializer;

  private transient boolean initialized;
  private transient T ref;

  private Transient(SerializableSupplier<T> initializer) {
    checkArgument(initializer != null);

    this.initializer = initializer;
    this.ref = null;
    this.initialized = false;
  }

  private Transient(T value, SerializableSupplier<T> initializer) {
    checkArgument(value != null);
    checkArgument(initializer != null);

    this.initializer = initializer;
    this.ref = value;
    this.initialized = true;
  }

  public T get() {
    if (!initialized) {
      synchronized (this) {
        if (!initialized) {
          this.ref = initializer.get();
          initialized = true;
        }
      }
    }

    return ref;
  }

  public void reset() {
    synchronized (this) {
      this.ref = null;
      this.initialized = false;
    }
  }

  public void destroy(ThrowingConsumer<T> cleaner) throws Exception {
    synchronized (this) {
      if (initialized) {
        cleaner.accept(ref);
      }

      this.ref = null;
      this.initialized = false;
      this.initializer = null;
    }
  }

  /**
   * Creates instance of {@link Transient} by lazily executing provided {@code initializer},
   * to instantiate value of type {@link T}. Same initializer will be used to re-instantiate
   * the value after original one being dropped during serialization/deserialization cycle
   */
  public static <T> Transient<T> lazy(SerializableSupplier<T> initializer) {
    return new Transient<>(initializer);
  }

  /**
   * Creates instance of {@link Transient} by eagerly setting it to provided {@code value},
   * while given {@code initializer} will be used to re-instantiate the value after original
   * one being dropped during serialization/deserialization cycle
   */
  public static <T> Transient<T> eager(T value, SerializableSupplier<T> initializer) {
    return new Transient<>(value, initializer);
  }
}