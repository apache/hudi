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

package org.apache.hudi.common.util.collection;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Predicate;

/**
 * An Iterator that filters another Iterator.
 *
 * @param <T> the type of objects produced by this Iterator
 */
public class FilterIterator<T> implements Iterator<T> {
  public static <T> FilterIterator<T> filter(Iterator<T> items, Predicate<T> shouldKeep) {
    return new FilterIterator(items, shouldKeep);
  }

  private final Iterator<T> items;
  private final Predicate<T> shouldKeep;
  private boolean nextReady;
  private T next;

  private FilterIterator(Iterator<T> items, Predicate<T> shouldKeep) {
    this.items = items;
    this.shouldKeep = shouldKeep;
    this.next = null;
    this.nextReady = false;
  }

  @Override
  public boolean hasNext() {
    return nextReady || advance();
  }

  @Override
  public T next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    this.nextReady = false;
    return next;
  }

  private boolean advance() {
    while (items.hasNext()) {
      this.next = items.next();
      if (shouldKeep.test(next)) {
        this.nextReady = true;
        return true;
      }
    }

    this.nextReady = false;
    return false;
  }
}
