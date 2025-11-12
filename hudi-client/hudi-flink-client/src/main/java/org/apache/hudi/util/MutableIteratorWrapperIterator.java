/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.util;

import org.apache.hudi.exception.HoodieException;

import org.apache.flink.util.MutableObjectIterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.function.Supplier;

/**
 * An implement of {@link Iterator} with an inner {@link MutableObjectIterator} from Flink.
 * `hasNext()` here is idempotent, so it's safe to be called multiple times before `next()`
 * is called, for example, to check whether the iterator is empty before iterating.
 */
public class MutableIteratorWrapperIterator<T> implements Iterator<T> {
  private final MutableObjectIterator<T> innerItr;
  // todo BinaryRowData wrapper can be used if there is no delete records in iterator, HUDI-9195
  private final Supplier<T> rowSupplier;
  // if there is no caching operation for the iterator, it's safe to use a singleton reused row.
  private T curRow;

  public MutableIteratorWrapperIterator(
      MutableObjectIterator<T> innerItr,
      Supplier<T> rowSupplier) {
    this.innerItr = innerItr;
    this.rowSupplier = rowSupplier;
  }

  @Override
  public boolean hasNext() {
    if (curRow != null) {
      return true;
    }
    try {
      curRow = innerItr.next(rowSupplier.get());
      return curRow != null;
    } catch (IOException e) {
      throw new HoodieException("Failed to get next record from inner iterator.", e);
    }
  }

  @Override
  public T next() {
    T result = curRow;
    curRow = null;
    return result;
  }
}
