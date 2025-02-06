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

package org.apache.hudi.client.utils;

import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.function.Supplier;

/**
 * Provides iterator interface over List of iterators. Consumes all records from first iterator element
 * before moving to next iterator in the list. That is concatenating elements across multiple iterators.
 *
 * <p>Different with {@link ConcatenatingIterator}, the internal iterators are instantiated lazily.
 */
public class LazyConcatenatingIterator<T> implements ClosableIterator<T> {

  private final Queue<Supplier<ClosableIterator<T>>> iteratorSuppliers;

  private ClosableIterator<T> itr;

  private boolean initialed = false;

  private boolean closed = false;

  public LazyConcatenatingIterator(List<Supplier<ClosableIterator<T>>> iteratorSuppliers) {
    this.iteratorSuppliers = new LinkedList<>(iteratorSuppliers);
  }

  @Override
  public void close() {
    if (!closed) {
      if (itr != null) {
        itr.close();
        itr = null;
      }
      iteratorSuppliers.clear();
      closed = true;
    }
  }

  @Override
  public boolean hasNext() {
    init();
    while (itr != null) {
      if (itr.hasNext()) {
        return true;
      }
      // close current iterator
      this.itr.close();
      if (!iteratorSuppliers.isEmpty()) {
        // move to the next
        itr = iteratorSuppliers.poll().get();
      } else {
        itr = null;
      }
    }
    return false;
  }

  @Override
  public T next() {
    ValidationUtils.checkState(hasNext(), "No more elements left");
    return itr.next();
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private void init() {
    if (!initialed) {
      if (!this.iteratorSuppliers.isEmpty()) {
        this.itr = iteratorSuppliers.poll().get();
      }
      initialed = true;
    }
  }
}
