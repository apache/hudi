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

import org.apache.hudi.common.util.ValidationUtils;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.function.Supplier;

/**
 * Provides iterator interface over List of iterators. Consumes all records from first iterator element
 * before moving to next iterator in the list. That is concatenating elements across multiple iterators.
 *
 * <p>Different with {@code ConcatenatingIterator}, the internal iterators are instantiated lazily.
 */
public class LazyConcatenatingIterator<T> implements ClosableIterator<T> {

  private final Queue<Supplier<ClosableIterator<T>>> iteratorSuppliers;

  private ClosableIterator<T> itr;

  private boolean closed = false;

  public LazyConcatenatingIterator(List<Supplier<ClosableIterator<T>>> iteratorSuppliers) {
    this.iteratorSuppliers = new LinkedList<>(iteratorSuppliers);
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      iteratorSuppliers.clear();
      closeCurrent();
    }
  }

  @Override
  public boolean hasNext() {
    while (true) {
      if (itr != null) {
        if (itr.hasNext()) {
          return true;
        }
        closeCurrent();
      }
      if (iteratorSuppliers.isEmpty()) {
        return false;
      }
      itr = iteratorSuppliers.poll().get();
    }
  }

  @Override
  public T next() {
    ValidationUtils.checkState(hasNext(), "No more elements left");
    return itr.next();
  }

  /**
   * Releases the reference before closing, so an iterator whose close fails is never closed again.
   */
  private void closeCurrent() {
    ClosableIterator<T> current = itr;
    itr = null;
    if (current != null) {
      current.close();
    }
  }
}
