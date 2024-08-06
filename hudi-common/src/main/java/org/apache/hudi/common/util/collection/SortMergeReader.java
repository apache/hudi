/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.util.collection;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieNotSupportedException;

import java.util.Comparator;
import java.util.List;

public interface SortMergeReader<K, V> extends ClosableIterator<V> {
  static <K, V> SortMergeReader<K, V> create(SortEngine engine, List<SortedEntryReader<Pair<K, V>>> readers, Comparator<V> comparator, Option<CombineFunc<K, V, V>> combineFunc) {
    if (readers == null || readers.isEmpty()) {
      return new SortMergeReader<K, V>() {
        @Override
        public void close() {

        }

        @Override
        public boolean hasNext() {
          return false;
        }

        @Override
        public V next() {
          return null;
        }

        @Override
        public long getCombinedRecordsNum() {
          return 0;
        }
      };
    }
    switch (engine) {
      case HEAP:
        return new HeapSortMergeReader<K, V>(readers, comparator, combineFunc.orElse(CombineFunc.defaultCombineFunc()));
      case LOSER_TREE:
        throw new HoodieNotSupportedException("Loser tree is not supported yet");
      default:
        throw new IllegalArgumentException("Unsupported sort engine: " + engine);
    }
  }

  long getCombinedRecordsNum();
}

