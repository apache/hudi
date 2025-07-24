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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Iterator that provides sorted elements from a source iterator.
 * Elements are sorted using natural ordering if they implement Comparable.
 * If sorting fails or elements are not Comparable, the original order is preserved.
 * This iterator is closable and will close the underlying iterator if it implements AutoCloseable.
 *
 * @param <T> Type of elements
 */
public class ClosableSortingIterator<T> implements Iterator<T>, AutoCloseable {
  private final Iterator<T> source;
  private final boolean closable;
  private Iterator<T> sortedIterator;
  private boolean initialized = false;

  public ClosableSortingIterator(Iterator<T> source) {
    this.source = source;
    this.closable = source instanceof AutoCloseable;
  }

  @Override
  public boolean hasNext() {
    initializeIfNeeded();
    return sortedIterator.hasNext();
  }

  @Override
  public T next() {
    initializeIfNeeded();
    return sortedIterator.next();
  }

  @Override
  public void close() throws Exception {
    if (closable) {
      ((AutoCloseable) source).close();
    }
  }

  private void initializeIfNeeded() {
    if (!initialized) {
      // Collect all elements from the source iterator
      List<T> list = new ArrayList<>();
      source.forEachRemaining(list::add);
      
      // If the list is empty or has only one element, no sorting needed
      if (list.size() <= 1) {
        sortedIterator = list.iterator();
      } else {
        // Check if the first element is Comparable
        T firstElement = list.get(0);
        if (!(firstElement instanceof Comparable)) {
          throw new IllegalArgumentException("Elements must implement Comparable interface for sorting. Found: " 
              + firstElement.getClass().getName());
        }
        
        try {
          // Sort the list using natural ordering
          Collections.sort((List) list);
        } catch (ClassCastException e) {
          throw new IllegalArgumentException("Elements cannot be compared with each other for sorting", e);
        }
        sortedIterator = list.iterator();
      }
      initialized = true;
    }
  }
} 