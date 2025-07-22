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
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * An iterator wrapper that deduplicate the outputs. The nested inner iterator must outputs sorted items.
 * @param <T> Item type.
 */
public class ClosableSortedDedupingIterator<T> implements Iterator<T>, AutoCloseable {
  private final Iterator<T> inner;
  private T nextUnique;
  private boolean hasNext;

  public ClosableSortedDedupingIterator(Iterator<T> inner) {
    this.inner = inner;
  }

  @Override
  public boolean hasNext() {
    if (hasNext) {
      return true;
    }

    while (inner.hasNext()) {
      T candidate = inner.next();
      if (!Objects.equals(candidate, nextUnique)) {
        nextUnique = candidate;
        hasNext = true;
        return true;
      }
    }

    return false;
  }

  @Override
  public T next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    hasNext = false;
    return nextUnique;
  }

  @Override
  public void close() throws Exception {
    if (inner instanceof AutoCloseable) {
      ((AutoCloseable) inner).close();
    }
  }
}
