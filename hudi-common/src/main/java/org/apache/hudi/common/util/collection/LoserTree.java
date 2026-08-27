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

import org.apache.hudi.common.util.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

/**
 * Loser-tree state machine for merging sorted runs.
 *
 * <p>Each leaf keeps one active element from one sorted input stream; {@code tree[0]} stores the
 * current champion and internal nodes store the loser from the corresponding tournament match.
 */
public class LoserTree<T> implements AutoCloseable {
  private final List<SortedRunReader<T>> leaves;
  private final Function<T, String> recordKeyExtractor;
  private final int leafBase;
  private final int[] tree;
  private final int[] winners;

  public LoserTree(List<SortedRunReader<T>> leaves, Function<T, String> recordKeyExtractor) {
    this.leaves = leaves;
    this.recordKeyExtractor = recordKeyExtractor;
    this.leafBase = nextPowerOfTwo(Math.max(1, leaves.size()));
    this.tree = new int[leafBase];
    this.winners = new int[leafBase << 1];
    Arrays.fill(tree, -1);
    Arrays.fill(winners, -1);
    build();
  }

  private void build() {
    for (int i = 0; i < leaves.size(); i++) {
      winners[leafBase + i] = leaves.get(i).current == null ? -1 : i;
    }
    if (leafBase == 1) {
      tree[0] = winners[leafBase];
    } else {
      for (int node = leafBase - 1; node > 0; node--) {
        replay(node);
      }
    }
  }

  public boolean isEmpty() {
    return tree[0] < 0;
  }

  public T peekWinner() {
    int winnerIndex = tree[0];
    return winnerIndex < 0 ? null : leaves.get(winnerIndex).current;
  }

  public int peekWinnerMergeOrder() {
    int winnerIndex = tree[0];
    return winnerIndex < 0 ? -1 : leaves.get(winnerIndex).mergeOrder;
  }

  public T popWinner() {
    int winnerIndex = tree[0];
    SortedRunReader<T> winner = leaves.get(winnerIndex);
    T record = winner.current;
    if (!winner.advance()) {
      winner.close();
    }
    update(winnerIndex);
    return record;
  }

  private void update(int leafIndex) {
    winners[leafBase + leafIndex] = leaves.get(leafIndex).current == null ? -1 : leafIndex;
    if (leafBase == 1) {
      tree[0] = winners[leafBase];
      return;
    }
    int node = (leafBase + leafIndex) >> 1;
    while (node > 0) {
      replay(node);
      node >>= 1;
    }
  }

  private void replay(int node) {
    int left = winners[node << 1];
    int right = winners[(node << 1) + 1];
    if (left < 0 && right < 0) {
      winners[node] = -1;
      tree[node] = -1;
    } else if (left < 0) {
      winners[node] = right;
      tree[node] = -1;
    } else if (right < 0) {
      winners[node] = left;
      tree[node] = -1;
    } else if (compare(left, right) <= 0) {
      winners[node] = left;
      tree[node] = right;
    } else {
      winners[node] = right;
      tree[node] = left;
    }
    if (node == 1) {
      tree[0] = winners[node];
    }
  }

  private int compare(int leftIndex, int rightIndex) {
    SortedRunReader<T> left = leaves.get(leftIndex);
    SortedRunReader<T> right = leaves.get(rightIndex);
    int recordKeyComparison = StringUtils.compareUtf8Bytes(
        recordKeyExtractor.apply(left.current), recordKeyExtractor.apply(right.current));
    return recordKeyComparison != 0
        ? recordKeyComparison
        : Integer.compare(left.mergeOrder, right.mergeOrder);
  }

  @Override
  public void close() {
    leaves.forEach(SortedRunReader::close);
  }

  private static int nextPowerOfTwo(int value) {
    int result = 1;
    while (result < value) {
      result <<= 1;
    }
    return result;
  }

  /**
   * Reader state for one sorted input run.
   */
  public static class SortedRunReader<T> {
    private final int mergeOrder;
    private final ClosableIterator<T> iterator;
    private T current;
    private boolean closed;

    public SortedRunReader(int mergeOrder, ClosableIterator<T> iterator) {
      this.mergeOrder = mergeOrder;
      this.iterator = iterator;
    }

    public boolean advance() {
      if (iterator.hasNext()) {
        current = iterator.next();
        return true;
      }
      current = null;
      return false;
    }

    public void close() {
      if (!closed) {
        iterator.close();
        closed = true;
      }
    }
  }
}
