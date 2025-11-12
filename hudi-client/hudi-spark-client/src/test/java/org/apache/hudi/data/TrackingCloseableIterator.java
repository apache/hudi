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

package org.apache.hudi.data;

import org.apache.hudi.common.util.collection.ClosableIterator;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Closeable iterator to use in Spark related testing to ensure that the close method is properly called after transformations.
 * @param <T> type of record within the iterator
 */
class TrackingCloseableIterator<T> implements ClosableIterator<T>, Serializable {
  private static final Map<String, Boolean> IS_CLOSED_BY_ID = new HashMap<>();
  private final String id;
  private final Iterator<T> inner;

  public TrackingCloseableIterator(String id, Iterator<T> inner) {
    this.id = id;
    this.inner = inner;
    IS_CLOSED_BY_ID.put(id, false);
  }

  public static boolean isClosed(String id) {
    return IS_CLOSED_BY_ID.get(id);
  }

  @Override
  public void close() {
    IS_CLOSED_BY_ID.put(id, true);
  }

  @Override
  public boolean hasNext() {
    return inner.hasNext();
  }

  @Override
  public T next() {
    return inner.next();
  }
}
