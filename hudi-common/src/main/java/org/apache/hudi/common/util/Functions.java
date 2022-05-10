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

import java.io.Serializable;

/**
 * An interface contains a set of functions.
 */
public interface Functions {

  static Runnable noop() {
    return () -> {
    };
  }

  /**
   * A function which has not any parameter.
   */
  interface Function0<R> extends Serializable {
    R apply();
  }

  /**
   * A function which contains only one parameter.
   */
  interface Function1<T1, R> extends Serializable {
    R apply(T1 val1);
  }

  /**
   * A function which contains two parameters.
   */
  interface Function2<T1, T2, R> extends Serializable {
    R apply(T1 val1, T2 val2);
  }

  /**
   * A function which contains three parameters.
   */
  interface Function3<T1, T2, T3, R> extends Serializable {
    R apply(T1 val1, T2 val2, T3 val3);
  }

  /**
   * A function which contains 4 parameters.
   */
  interface Function4<T1, T2, T3, T4, R> extends Serializable {
    R apply(T1 val1, T2 val2, T3 val3, T4 val4);
  }
}
