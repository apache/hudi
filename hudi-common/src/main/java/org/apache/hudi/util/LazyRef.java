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

import java.util.function.Supplier;

// TODO java-doc
public class LazyRef<T> {

  private volatile boolean initialized;

  private Supplier<T> initializer;
  private T ref;

  private LazyRef(Supplier<T> initializer) {
    this.initializer = initializer;
    this.ref = null;
    this.initialized = false;
  }

  private LazyRef(T ref) {
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

  public static <T> LazyRef<T> lazy(Supplier<T> initializer) {
    return new LazyRef<>(initializer);
  }

  public static <T> LazyRef<T> eager(T ref) {
    return new LazyRef<>(ref);
  }
}
