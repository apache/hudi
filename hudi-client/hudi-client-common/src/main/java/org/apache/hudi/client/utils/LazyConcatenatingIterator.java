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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.function.Supplier;

public class LazyConcatenatingIterator<T> implements ClosableIterator<T> {

  private final Queue<Supplier<ClosableIterator<T>>> allLaziedIterators;

  private Option<ClosableIterator<T>> currentIterator = Option.empty();

  private boolean initialed = false;

  private boolean closed = false;

  public LazyConcatenatingIterator(List<Supplier<ClosableIterator<T>>> allLaziedIterators) {
    this.allLaziedIterators = new LinkedList<>(allLaziedIterators);
  }

  @Override
  public void close() {
    if (!closed) {
      if (currentIterator.isPresent()) {
        currentIterator.get().close();
        currentIterator = Option.empty();
      }
      allLaziedIterators.clear();
      closed = true;
    }
  }

  private void init() {
    if (!initialed) {
      if (!allLaziedIterators.isEmpty()) {
        currentIterator = Option.of(allLaziedIterators.poll().get());
      }
      initialed = true;
    }
  }

  @Override
  public boolean hasNext() {
    init();
    while (currentIterator.isPresent()) {
      if (currentIterator.get().hasNext()) {
        return true;
      }
      // close current iterator
      currentIterator.get().close();
      if (!allLaziedIterators.isEmpty()) {
        // move to next
        currentIterator = Option.of(allLaziedIterators.poll().get());
      } else {
        currentIterator = Option.empty();
      }
    }
    return false;
  }

  @Override
  public T next() {
    ValidationUtils.checkState(hasNext(), "No more elements left");
    return currentIterator.get().next();
  }
}
