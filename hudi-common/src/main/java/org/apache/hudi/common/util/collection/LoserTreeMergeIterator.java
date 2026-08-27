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

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Iterator adapter over a {@link LoserTree} for lazily supplied sorted input streams.
 */
public class LoserTreeMergeIterator<T> implements ClosableIterator<T> {
  private final LoserTree<T> loserTree;

  public LoserTreeMergeIterator(
      List<Supplier<ClosableIterator<T>>> iteratorSuppliers, Function<T, String> recordKeyExtractor) {
    List<LoserTree.SortedRunReader<T>> readers = new ArrayList<>(iteratorSuppliers.size());
    try {
      for (int mergeOrder = 0; mergeOrder < iteratorSuppliers.size(); mergeOrder++) {
        LoserTree.SortedRunReader<T> reader =
            new LoserTree.SortedRunReader<>(mergeOrder, iteratorSuppliers.get(mergeOrder).get());
        readers.add(reader);
        if (!reader.advance()) {
          reader.close();
        }
      }
      this.loserTree = new LoserTree<>(readers, recordKeyExtractor);
    } catch (RuntimeException e) {
      readers.forEach(LoserTree.SortedRunReader::close);
      throw e;
    }
  }

  @Override
  public boolean hasNext() {
    return !loserTree.isEmpty();
  }

  @Override
  public T next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return loserTree.popWinner();
  }

  @Override
  public void close() {
    loserTree.close();
  }
}
