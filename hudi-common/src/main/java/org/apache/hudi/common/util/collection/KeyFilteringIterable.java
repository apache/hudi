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

import java.util.Iterator;
import java.util.function.Predicate;

/**
 * An iterable that allows filtering on the element keys.
 *
 * @param <K> the type of element keys
 * @param <V> the type of elements returned by the iterator
 */
public interface KeyFilteringIterable<K, V> extends Iterable<V> {
  /**
   * Returns an iterator over elements of type {@code V}.
   *
   * @param filter The filter on the key of type {@code K}.
   *
   * @return an Iterator.
   */
  Iterator<V> iterator(Predicate<K> filter);
}
