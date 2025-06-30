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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * Provides iterator interface over List of iterators. Consumes all records from first iterator element
 * before moving to next iterator in the list. That is concatenating elements across multiple iterators.
 *
 * @param <T>
 */
public class ConcatenatingIterator<T> implements Iterator<T> {

  private final Queue<Iterator<T>> allIterators;

  public ConcatenatingIterator(List<? extends Iterator<T>> iterators) {
    allIterators = new LinkedList<>(iterators);
  }

  @Override
  public boolean hasNext() {
    while (!allIterators.isEmpty()) {
      if (allIterators.peek().hasNext()) {
        return true;
      }
      // iterator at current head is done. move ahead
      advanceIterator();
    }

    return false;
  }

  protected Iterator<T> advanceIterator() {
    return allIterators.poll();
  }

  @Override
  public T next() {
    ValidationUtils.checkArgument(hasNext(), "No more elements left");
    return allIterators.peek().next();
  }
}
