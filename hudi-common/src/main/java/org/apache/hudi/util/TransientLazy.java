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

import javax.annotation.concurrent.ThreadSafe;
import java.io.Serializable;

/**
 * {@link Serializable} counterpart of {@link Lazy}
 *
 * @param <T> type of the object being held by {@link TransientLazy}
 */
@ThreadSafe
public class TransientLazy<T> implements Serializable {

  private transient volatile boolean initialized;

  private SerializableSupplier<T> initializer;
  private transient T ref;

  private TransientLazy(SerializableSupplier<T> initializer) {
    this.initializer = initializer;
    this.ref = null;
    this.initialized = false;
  }

  private TransientLazy(T ref) {
    this.initializer = null;
    this.ref = ref;
    this.initialized = true;
  }

  public T get() {
    if (!initialized) {
      synchronized (this) {
        if (!initialized) {
          this.ref = initializer.get();
          this.initializer = null;
          initialized = true;
        }
      }
    }

    return ref;
  }

  /**
   * Executes provided {@code initializer} lazily, while providing for "exactly once" semantic,
   * to instantiate value of type {@link T} being subsequently held by the returned instance of
   * {@link TransientLazy}
   */
  public static <T> TransientLazy<T> lazily(SerializableSupplier<T> initializer) {
    return new TransientLazy<>(initializer);
  }

  /**
   * Instantiates {@link TransientLazy} in an "eagerly" fashion setting it w/ the provided value of
   * type {@link T} directly, bypassing lazy initialization sequence
   */
  public static <T> TransientLazy<T> eagerly(T ref) {
    return new TransientLazy<>(ref);
  }
}
