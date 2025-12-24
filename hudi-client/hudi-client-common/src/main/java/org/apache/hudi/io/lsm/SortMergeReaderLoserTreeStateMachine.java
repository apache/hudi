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

package org.apache.hudi.io.lsm;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

public class SortMergeReaderLoserTreeStateMachine<T> implements SortMergeReader<T> {

  private final Comparator<HoodieRecord> comparator;
  private final RecordMergeWrapper<T> mergeFunctionWrapper;
  private final List<RecordReader<HoodieRecord>> readers;
  private final List<Iterator<HoodieRecord>> iterables;

  public SortMergeReaderLoserTreeStateMachine(List<RecordReader<HoodieRecord>> readers,
                                              Comparator<HoodieRecord> userKeyComparator,
                                              RecordMergeWrapper<T> mergeFunctionWrapper) {
    this.comparator = userKeyComparator;
    this.readers = readers;
    this.mergeFunctionWrapper = mergeFunctionWrapper;

    this.iterables = readers.stream().map(reader -> {
      try {
        return reader.read();
      } catch (IOException e) {
        throw new HoodieIOException(e.getMessage(), e);
      }
    }).collect(Collectors.toList());
  }

  @Override
  public Iterator<T> read() {
    return new SortMergeReaderLoserTreeIterator(iterables, comparator);
  }

  @Override
  public void close() throws IOException {
    for (RecordReader<HoodieRecord> reader : readers) {
      reader.close();
    }
  }

  // 败者树节点状态
  protected enum State {
    WINNER_WITH_NEW_KEY,
    WINNER_WITH_SAME_KEY,
    WINNER_POPPED,
    LOSER_WITH_NEW_KEY,
    LOSER_WITH_SAME_KEY,
    LOSER_POPPED
  }

  // 败者树节点结构
  protected static class LoserTreeLeafNode {
    HoodieRecord record;
    int sourceIndex; // 输入流Index
    State state;
    int firstSameKeyIndex = -1;

    public LoserTreeLeafNode(HoodieRecord record, int sourceIndex, State state, int firstSameKeyIndex) {
      this.record = record;
      this.sourceIndex = sourceIndex;
      this.state = state;
      this.firstSameKeyIndex = firstSameKeyIndex;
    }

    public void setFirstSameKeyIndex(int index) {
      if (firstSameKeyIndex == -1) {
        firstSameKeyIndex = index;
      }
    }
  }

  // 败者树
  protected static class LoserTree {
    protected final int[] tree;          // tree[0]存储冠军下标，tree[1...k-1]存储败者
    protected final int k;         // 叶子节点（即输入序列）的数量
    protected final Comparator<HoodieRecord> comparator;
    protected final LoserTreeLeafNode[] leaves;

    public LoserTree(List<Iterator<HoodieRecord>> iterators, Comparator<HoodieRecord> comparator) {
      this.k = iterators.size();
      this.leaves = new LoserTreeLeafNode[k];     // 叶子当前元素
      this.tree = new int[k];
      this.comparator = comparator;

      // 初始化叶子，取出第一个元素，包装成 `LoserTreeNode`（元素值和序列编号）
      // 叶子节点状态初始化为 `WINNER_WITH_NEW_KEY`
      for (int i = 0; i < k; i++) {
        Iterator<HoodieRecord> iter = iterators.get(i);
        if (iter != null && iter.hasNext()) {
          leaves[i] = new LoserTreeLeafNode(iter.next(), i, State.WINNER_WITH_NEW_KEY, -1);
        } else {
          leaves[i] = null;
        }
      }

      // 初始化节点
      Arrays.fill(tree, -1);
      for (int i = 0; i < k; i++) {
        adjust(i);
      }
    }

    // 调整第index个叶子节点
    // 叶子节点和父节点比较, 败者留在父节点位置, 胜者继续和父节点的父节点比较,直到整棵树的根节点
    protected void adjust(int index) {
      int parent = (index + k) >> 1;     // 父节点下标
      int winner = index;      // winner 需要向上调整的叶子结点索引
      LoserTreeLeafNode parentNode;
      while (parent > 0) {
        if (tree[parent] == -1) {    // 父节点还未初始化
          tree[parent] = winner;
          if (leaves[winner] != null) {
            leaves[winner].state = State.LOSER_WITH_NEW_KEY;    // 更改对应叶子结点的state
          }
          winner = -1;
          break;     // 父节点为空
        } else {
          parentNode = leaves[tree[parent]];
          // null值判断
          if (parentNode == null && leaves[winner] == null) {
            parent >>= 1;
            continue;
          } else if (leaves[winner] == null && parentNode != null) {   // parent胜出
            if (parentNode.state == State.LOSER_POPPED) {    // 相当于照搬adjustWinnerWithNewKey逻辑，提前设置state
              parentNode.state = State.WINNER_POPPED;
              parentNode.firstSameKeyIndex = -1;
            } else {
              if (parentNode.state != State.WINNER_WITH_NEW_KEY && parentNode.state != State.WINNER_WITH_SAME_KEY && parentNode.state != State.WINNER_POPPED) {
                parentNode.state = State.WINNER_WITH_NEW_KEY;
              }
            }
          } else if (leaves[winner] != null && parentNode == null) {    // winner胜出
            if (leaves[winner].state != State.WINNER_WITH_NEW_KEY && leaves[winner].state != State.WINNER_WITH_SAME_KEY && leaves[winner].state != State.WINNER_POPPED) {
              leaves[winner].state = State.WINNER_WITH_NEW_KEY;
            }
          } else {
            switch (leaves[winner].state) {
              case WINNER_WITH_NEW_KEY:
                adjustWinnerWithNewKey(parent, winner, parentNode, leaves[winner]);
                break;
              case WINNER_WITH_SAME_KEY:
                adjustWinnerWithSameKey(parent, winner, parentNode, leaves[winner]);
                break;
              case WINNER_POPPED:     // 是否有相同的未处理 UserKey 节点
                if (leaves[winner].firstSameKeyIndex < 0) {
                  // 当前 winnerNode 没有另一个相同 key、尚未处理完的叶子节点
                  parent = -1;    // 结束循环
                } else {
                  // 为“不同叶子”的同 key 做快速跳转
                  parent = leaves[winner].firstSameKeyIndex;
                  parentNode = leaves[tree[parent]];
                  leaves[winner].state = State.LOSER_POPPED;
                  parentNode.state = State.WINNER_WITH_SAME_KEY;
                }
                break;
              default:
                throw new UnsupportedOperationException(
                    "unknown state for " + leaves[winner].state.name());
            }
          }
        }

        if (parent > 0) {
          int loser = tree[parent];
          // if the winner loses, exchange nodes.
          if (leaves[winner] == null || leaves[winner].state == State.LOSER_WITH_SAME_KEY || leaves[winner].state == State.LOSER_WITH_NEW_KEY || leaves[winner].state == State.LOSER_POPPED) {
            int tmp = winner;
            winner = loser;
            tree[parent] = tmp;
          }
          parent >>= 1;
        }
      }
      tree[0] = winner;
    }

    protected void adjustWinnerWithNewKey(int parent, int winner, LoserTreeLeafNode parentNode, LoserTreeLeafNode winnerNode) {
      switch (parentNode.state) {
        case LOSER_WITH_NEW_KEY:
          int loser = tree[parent];
          int compareResult = compare(winner, loser);
          if (compareResult < 0) {   // 子节点胜
            parentNode.state = State.LOSER_WITH_NEW_KEY;
          } else if (compareResult > 0) {   // 父节点胜
            winnerNode.state = State.LOSER_WITH_NEW_KEY;
            parentNode.state = State.WINNER_WITH_NEW_KEY;
          } else {   // key相等，比较叶子索引，小的优先
            if (winner > loser) {    // 索引大、判为loser
              winnerNode.state = State.LOSER_WITH_SAME_KEY;
              parentNode.state = State.WINNER_WITH_NEW_KEY;
              parentNode.setFirstSameKeyIndex(parent);
            } else {
              parentNode.state = State.LOSER_WITH_SAME_KEY;
              winnerNode.setFirstSameKeyIndex(parent);
            }
          }
          return;
        case LOSER_WITH_SAME_KEY:
          throw new RuntimeException(
              "This is a bug. Please file an issue. A node in the WINNER_WITH_NEW_KEY "
                  + "state cannot encounter a node in the LOSER_WITH_SAME_KEY state.");
        case LOSER_POPPED:
          parentNode.state = State.WINNER_POPPED;
          parentNode.firstSameKeyIndex = -1;
          winnerNode.state = State.LOSER_WITH_NEW_KEY;
          return;
        default:
          throw new UnsupportedOperationException(
              "unknown state for " + parentNode.state.name());
      }
    }

    protected void adjustWinnerWithSameKey(int parent, int winner, LoserTreeLeafNode parentNode, LoserTreeLeafNode winnerNode) {
      switch (parentNode.state) {
        case LOSER_WITH_NEW_KEY:
          return;
        case LOSER_POPPED:
          return;
        case LOSER_WITH_SAME_KEY:    // Key相同，只需要比较两个节点的索引
          int loser = tree[parent];
          if (winner > loser) {    // 索引大、判为loser
            winnerNode.state = State.LOSER_WITH_SAME_KEY;
            parentNode.state = State.WINNER_WITH_SAME_KEY;
            parentNode.setFirstSameKeyIndex(parent);
          } else {
            parentNode.state = State.LOSER_WITH_SAME_KEY;     // 可删除，因状态没变
            winnerNode.setFirstSameKeyIndex(parent);
          }
          return;
        default:
          throw new UnsupportedOperationException(
              "unknown state for " + parentNode.state.name());
      }
    }

    // 返回结果 正数为index2胜出 负数为index1胜出 0为相等
    private int compare(int index1, int index2) {
      if (index1 == -1) {
        return 1;
      }
      if (index2 == -1) {
        return -1;
      }
      if (leaves[index1] == null) {
        return 1;    // 1为空，2胜出
      }
      if (leaves[index2] == null) {
        return -1;
      }
      return comparator.compare(leaves[index1].record, leaves[index2].record);
    }

    // pop获胜节点，补充对应元素
    protected void popAdvance(List<Iterator<HoodieRecord>> iterators) {
      LoserTreeLeafNode winner = leaves[tree[0]];   // 获胜节点
      while (winner != null && winner.state == State.WINNER_POPPED) {
        if (tree[0] == -1) {
          return;
        }
        if (iterators.get(tree[0]).hasNext()) {    // 输入序列有值
          leaves[tree[0]] = new LoserTreeLeafNode(iterators.get(tree[0]).next(), tree[0], State.WINNER_WITH_NEW_KEY, -1);
        } else {
          leaves[tree[0]] = null;
        }
        adjust(tree[0]);    // 调整
        winner = leaves[tree[0]];
      }
    }

    protected HoodieRecord popWinner() {
      LoserTreeLeafNode winner = leaves[tree[0]];    // 获胜节点
      if (winner == null) {
        return null;
      }
      if (winner.state == State.WINNER_POPPED) {
        // 如果winner的状态已经是WINNER_POPPED，说明本组key已经处理过了，直接返回null
        return null;
      }
      winner.state = State.WINNER_POPPED;
      HoodieRecord winnerRecord = winner.record;

      // 重新调整获胜的叶子节点，检查是否还有相同key未处理
      adjust(tree[0]);
      return winnerRecord;
    }

    // 查看但不弹出当前败者树的胜者
    protected HoodieRecord peekWinner() {
      if (tree[0] == -1) {
        return null;
      }
      LoserTreeLeafNode winner = leaves[tree[0]];
      if (winner == null || winner.state == State.WINNER_POPPED) {
        return null;
      }
      return winner.record;
    }

    public boolean peekNull() {
      LoserTreeLeafNode winner = leaves[tree[0]];
      return winner != null;
    }
  }

  private class SortMergeReaderLoserTreeIterator implements Iterator<T> {
    private final List<Iterator<HoodieRecord>> iterators;
    private final LoserTree loserTree;
    private T nextRecord;

    // Merge迭代器
    public SortMergeReaderLoserTreeIterator(List<Iterator<HoodieRecord>> iterables, Comparator<HoodieRecord> comparator) {
      this.iterators = iterables;
      this.loserTree = new LoserTree(iterators, comparator);
    }

    @Override
    public boolean hasNext() {
      nextRecord = getNextRecord();
      return nextRecord != null;
    }

    @Override
    public T next() {
      // 如果 nextRecord 没值，说明没有调用过hasNext方法，需要提前抛出异常
      if (nextRecord == null) {
        throw new NoSuchElementException();
      }
      T resultRecord = nextRecord;
      nextRecord = null;
      return resultRecord;
    }

    public T getNextRecord() {
      if (!hasNextWinner()) {
        return null;
      }

      Option<T> result = null;
      while (hasNextWinner()) {
        // 判断是否是WINNER_POPPED，是的话，补充弹出后的叶子值
        loserTree.popAdvance(iterators);
        // 判断是否处理相同key，如WINNER_POPPED，则已经处理过了
        HoodieRecord winnerRecord = loserTree.popWinner();
        if (winnerRecord == null) {
          break;
        }

        mergeFunctionWrapper.merge(winnerRecord);
        while (loserTree.peekWinner() != null) {    // 当前胜者没有相同key待处理，不需要增加sameKeyRecords
          mergeFunctionWrapper.merge(loserTree.popWinner());
        }
        result = mergeFunctionWrapper.getMergedResult();
        mergeFunctionWrapper.reset();

        if (result.isPresent()) {
          break;
        }
      }

      if (!result.isPresent()) {
        // 如果最后真的没有这条数据 说明来自delete数据
        return null;
      }

      return result.get();

    }

    private boolean hasNextWinner() {
      loserTree.popAdvance(iterators);
      return loserTree.peekNull();
    }
  }
}

