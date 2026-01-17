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

import javax.annotation.concurrent.ThreadSafe;

import java.util.function.Supplier;

/**
 * Utility implementing lazy semantics in Java
 *
 * @param <T> type of the object being held by {@link Lazy}
 */
@ThreadSafe
public class Lazy<T> {

  private volatile boolean initialized;

  private Supplier<T> initializer;
  private T ref;

  private Lazy(Supplier<T> initializer) {
    this.initializer = initializer;
    this.ref = null;
    this.initialized = false;
  }

  private Lazy(T ref) {
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
   * to instantiate value of type T being subsequently held by the returned instance of
   * {@link Lazy}
   */
  public static <T> Lazy<T> lazily(Supplier<T> initializer) {
    return new Lazy<>(initializer);
  }

  /**
   * Instantiates {@link Lazy} in an "eagerly" fashion setting it w/ the provided value of
   * type T directly, bypassing lazy initialization sequence
   */
  public static <T> Lazy<T> eagerly(T ref) {
    return new Lazy<>(ref);
  }
}
