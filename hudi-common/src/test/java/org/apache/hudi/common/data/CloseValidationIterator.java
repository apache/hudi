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

package org.apache.hudi.common.data;

import org.apache.hudi.common.util.collection.ClosableIterator;

import lombok.Getter;

import java.util.Iterator;

/**
 * Implementation of a {@link ClosableIterator} to help validate that the close method is properly called.
 * @param <T> type of record within the iterator
 */
class CloseValidationIterator<T> implements ClosableIterator<T> {
  private final Iterator<T> inner;
  @Getter
  private boolean isClosed = false;

  public CloseValidationIterator(Iterator<T> inner) {
    this.inner = inner;
  }

  @Override
  public void close() {
    isClosed = true;
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
