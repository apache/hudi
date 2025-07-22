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

package org.apache.hudi.common.function;

import java.io.Serializable;

/**
 * A serializable functional interface that represents a function taking a key-value pair
 * and returns a boolean. Designed to be used in filter() calls.
 *
 * @param <K> the type of the key parameter
 * @param <V> the type of the value parameter
 *
 */
@FunctionalInterface
public interface SerializablePairPredicate<K, V> extends Serializable {
  
  /**
   * Applies this function to the given key-value pair.
   * 
   * <p>This method processes the input key-value pair and returns a result of type boolean.
   * The function may throw exceptions to indicate processing errors or exceptional conditions.</p>
   * 
   * @param key the key parameter of the function
   * @param value the value parameter of the function
   * @return boolean
   * @throws Exception if an error occurs during function execution
   */
  boolean call(K key, V value) throws Exception;
}
