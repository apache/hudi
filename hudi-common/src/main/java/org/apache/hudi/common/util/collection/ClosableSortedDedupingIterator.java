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

public class ClosableSortedDedupingIterator<T> implements Iterator<T>, AutoCloseable {
  private final Iterator<T> inner;
  private final boolean closable;
  private T nextUnique = null;
  private boolean hasNextCached = false;
  private T lastReturned = null;

  public ClosableSortedDedupingIterator(Iterator<T> inner) {
    this.inner = inner;
    this.closable = inner instanceof AutoCloseable;
  }

  @Override
  public boolean hasNext() {
    if (hasNextCached) {
      return true;
    }

    while (inner.hasNext()) {
      T candidate = inner.next();
      if (lastReturned == null || (!candidate.equals(lastReturned))) {
        nextUnique = candidate;
        hasNextCached = true;
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
    hasNextCached = false;
    lastReturned = nextUnique;
    return nextUnique;
  }

  @Override
  public void close() throws Exception {
    if (closable) {
      ((AutoCloseable) inner).close();
    }
  }
}
