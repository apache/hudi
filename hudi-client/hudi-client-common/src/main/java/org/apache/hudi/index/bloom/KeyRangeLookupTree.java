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

package org.apache.hudi.index.bloom;

import org.apache.hudi.common.util.rbtree.RedBlackTree;

import java.util.HashSet;
import java.util.Set;

/**
 * Look up tree implemented as red-black trees to search for any given key in (N logN) time complexity.
 */
class KeyRangeLookupTree extends RedBlackTree<KeyRangeNode, RecordKeyRange> {

  /**
   * Flag for whether sub-tree min-max metrics need to be recalculated. When inserting or deleting nodes,
   * we need to recalculated.
   */
  private volatile boolean needReloadMetrics = false;

  @Override
  public void insert(KeyRangeNode newNode) {
    needReloadMetrics = true;
    super.insert(newNode);
  }

  @Override
  public void remove(RecordKeyRange key) {
    needReloadMetrics = true;
    super.remove(key);
  }

  /**
   * If current root and newNode matches with min record key and max record key, merge two nodes. In other words, add
   * files from {@code newNode}.
   *
   * @param oldNode previously inserted node
   * @param newNode newly inserted same node
   */
  @Override
  protected void processWhenInsertSame(KeyRangeNode oldNode, KeyRangeNode newNode) {
    oldNode.addFiles(newNode.getFileNameList());
  }

  /**
   * Traverse the tree to calculate sub-tree min-max metrics.
   */
  private void calculateSubTreeMinMax(KeyRangeNode node) {
    if (node == null) {
      return;
    }
    if (node.getLeft() != null) {
      calculateSubTreeMinMax(node.getLeft());
      node.setLeftSubTreeMin(minRecord(node.getLeft()));
      node.setLeftSubTreeMax(maxRecord(node.getLeft()));
    }
    if (node.getRight() != null) {
      calculateSubTreeMinMax(node.getRight());
      node.setRightSubTreeMin(minRecord(node.getRight()));
      node.setRightSubTreeMax(maxRecord(node.getRight()));
    }
  }

  /**
   * Get the minimum value among the node and its child nodes.
   */
  private String minRecord(KeyRangeNode node) {
    String min = node.getKey().getMinRecordKey();
    if (node.getLeft() != null && node.getLeftSubTreeMin().compareTo(min) < 0) {
      min = node.getLeftSubTreeMin();
    }
    if (node.getRight() != null && node.getRightSubTreeMin().compareTo(min) < 0) {
      min = node.getRightSubTreeMin();
    }
    return min;
  }

  /**
   * Get the maximum value among the node and its child nodes.
   */
  private String maxRecord(KeyRangeNode node) {
    String max = node.getKey().getMaxRecordKey();
    if (node.getLeft() != null && node.getLeftSubTreeMax().compareTo(max) > 0) {
      max = node.getLeftSubTreeMax();
    }
    if (node.getRight() != null && node.getRightSubTreeMax().compareTo(max) > 0) {
      max = node.getRightSubTreeMax();
    }
    return max;
  }

  /**
   * Fetches all the matching index files where the key could possibly be present.
   *
   * @param lookupKey the key to be searched for
   * @return the {@link Set} of matching index file names
   */
  Set<String> getMatchingIndexFiles(String lookupKey) {
    if (needReloadMetrics) {
      calculateSubTreeMinMax(getRoot());
    }
    Set<String> matchingFileNameSet = new HashSet<>();
    getMatchingIndexFiles(getRoot(), lookupKey, matchingFileNameSet);
    return matchingFileNameSet;
  }

  /**
   * Fetches all the matching index files where the key could possibly be present.
   *
   * @param root      refers to the current root of the look up tree
   * @param lookupKey the key to be searched for
   */
  private void getMatchingIndexFiles(KeyRangeNode root, String lookupKey, Set<String> matchingFileNameSet) {
    if (root == null) {
      return;
    }

    if (root.getMinRecordKey().compareTo(lookupKey) <= 0 && lookupKey.compareTo(root.getMaxRecordKey()) <= 0) {
      matchingFileNameSet.addAll(root.getFileNameList());
    }

    if (root.getLeftSubTreeMax() != null && root.getLeftSubTreeMin().compareTo(lookupKey) <= 0
        && lookupKey.compareTo(root.getLeftSubTreeMax()) <= 0) {
      getMatchingIndexFiles(root.getLeft(), lookupKey, matchingFileNameSet);
    }

    if (root.getRightSubTreeMax() != null && root.getRightSubTreeMin().compareTo(lookupKey) <= 0
        && lookupKey.compareTo(root.getRightSubTreeMax()) <= 0) {
      getMatchingIndexFiles(root.getRight(), lookupKey, matchingFileNameSet);
    }
  }
}
