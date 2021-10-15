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

package org.apache.hudi.common.data;

import org.apache.hudi.common.function.SerializableFunction;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * An abstraction for a data collection of objects in type T to store the reference
 * and do transformation.
 *
 * @param <T> type of object.
 */
public abstract class HoodieData<T> implements Serializable {
  /**
   * @return the collection of objects.
   */
  public abstract Object get();

  /**
   * @return whether the collection is empty.
   */
  public abstract boolean isEmpty();

  /**
   * Caches the data.
   *
   * @param properties config in properties.
   */
  public abstract void persist(Properties properties);

  /**
   * @param func serializable map function.
   * @param <O>  output object type.
   * @return {@link HoodieData<O>} containing the result. Actual execution may be deferred.
   */
  public abstract <O> HoodieData<O> map(SerializableFunction<T, O> func);

  /**
   * @param func serializable flatmap function.
   * @param <O>  output object type.
   * @return {@link HoodieData<O>} containing the result. Actual execution may be deferred.
   */
  public abstract <O> HoodieData<O> flatMap(SerializableFunction<T, Iterator<O>> func);

  /**
   * @return collected results in {@link List<T>}.
   */
  public abstract List<T> collectAsList();
}
