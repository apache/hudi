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

package org.apache.hudi.common.util.collection;

import java.io.Serializable;

/**
 * A tuple with 3 fields. Tuples are strongly typed; each field may be of a separate type. The
 * fields of the tuple can be accessed directly as public fields (f0, f1, ...). The tuple field
 * positions start at zero.
 *
 * @param <T0> The type of field 0
 * @param <T1> The type of field 1
 * @param <T2> The type of field 2
 */
public class Tuple3<T0, T1, T2> implements Serializable {

  private static final long serialVersionUID = 1L;

  /**
   * Field 0 of the tuple.
   */
  public final T0 f0;
  /**
   * Field 1 of the tuple.
   */
  public final T1 f1;
  /**
   * Field 2 of the tuple.
   */
  public final T2 f2;

  /**
   * Creates a new tuple and assigns the given values to the tuple's fields.
   *
   * @param f0 The value for field 0
   * @param f1 The value for field 1
   * @param f2 The value for field 2
   */
  private Tuple3(T0 f0, T1 f1, T2 f2) {
    this.f0 = f0;
    this.f1 = f1;
    this.f2 = f2;
  }

  /**
   * Creates a new tuple and assigns the given values to the tuple's fields. This is more
   * convenient than using the constructor, because the compiler can infer the generic type
   * arguments implicitly. For example: {@code Tuple3.of(n, x, s)} instead of {@code new
   * Tuple3<Integer, Double, String>(n, x, s)}
   */
  public static <T0, T1, T2> Tuple3<T0, T1, T2> of(T0 f0, T1 f1, T2 f2) {
    return new Tuple3<>(f0, f1, f2);
  }
}
