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

/**
 * <p>Operations on arrays, primitive arrays (like {@code int[]}) and
 * primitive wrapper arrays (like {@code Integer[]}).</p>
 *
 * <p>This class tries to handle {@code null} input gracefully.
 * An exception will not be thrown for a {@code null}
 * array input. However, an Object array that contains a {@code null}
 * element may throw an exception. Each method documents its behaviour.</p>
 *
 * NOTE : Adapted from org.apache.commons.lang3.ArrayUtils
 */
public class ArrayUtils {

  /**
   * An empty immutable {@code long} array.
   */
  public static final long[] EMPTY_LONG_ARRAY = new long[0];

  // Long array converters
  // ----------------------------------------------------------------------
  /**
   * <p>Converts an array of object Longs to primitives.</p>
   *
   * <p>This method returns {@code null} for a {@code null} input array.</p>
   *
   * @param array  a {@code Long} array, may be {@code null}
   * @return a {@code long} array, {@code null} if null array input
   * @throws NullPointerException if array content is {@code null}
   */
  public static long[] toPrimitive(Long[] array) {
    if (array == null) {
      return null;
    } else if (array.length == 0) {
      return EMPTY_LONG_ARRAY;
    }
    final long[] result = new long[array.length];
    for (int i = 0; i < array.length; i++) {
      result[i] = array[i].longValue();
    }
    return result;
  }
}
