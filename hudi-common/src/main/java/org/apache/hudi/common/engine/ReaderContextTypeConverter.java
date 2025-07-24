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

package org.apache.hudi.common.engine;

/**
 * An interface that implements type conversion related functions,
 * which is expected to extended by different engines to
 * handle their type conversion accordingly.
 */
public interface ReaderContextTypeConverter {
  /**
   * Cast to Java boolean value.
   * If the object is not compatible with boolean type, throws.
   */
  default boolean castToBoolean(Object value) {
    if (value instanceof Boolean) {
      return (boolean) value;
    } else {
      throw new IllegalArgumentException(
          "Input value type " + value.getClass() + ", cannot be cast to boolean");
    }
  }

  /**
   * Cast to Java string value.
   */
  default String castToString(Object value) {
    if (null != value) {
      return value.toString();
    }
    return null;
  }
}
