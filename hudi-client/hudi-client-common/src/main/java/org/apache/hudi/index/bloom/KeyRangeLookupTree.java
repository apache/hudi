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

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * Look up tree implemented as interval trees to search for any given key in (N logN) time complexity.
 */
class KeyRangeLookupTree implements Serializable {

  private KeyRangeNode root;

  /**
   * @return the root of the tree. Could be {@code null}
   */
  public KeyRangeNode getRoot() {
    return root;
  }

  /**
   * Inserts a new {@link KeyRangeNode} to this look up tree.
   *
   * @param newNode the new {@link KeyRangeNode} to be inserted
   */
  void insert(KeyRangeNode newNode) {
    root = insert(getRoot(), newNode);
  }

  /**
   * Inserts a new {@link KeyRangeNode} to this look up tree.
   *
   * If no root exists, make {@code newNode} as the root and return the new root.
   *
   * If current root and newNode matches with min record key and max record key, merge two nodes. In other words, add
   * files from {@code newNode} to current root. Return current root.
   *
   * If current root is < newNode if current root has no right sub tree update current root's right sub tree max and min
   * set newNode as right sub tree else update root's right sub tree min and max with newNode's min and max record key
   * as applicable recursively call insert() with root's right subtree as new root
   *
   * else // current root is >= newNode if current root has no left sub tree update current root's left sub tree max and
   * min set newNode as left sub tree else update root's left sub tree min and max with newNode's min and max record key
   * as applicable recursively call insert() with root's left subtree as new root
   *
   * @param root refers to the current root of the look up tree
   * @param newNode newNode the new {@link KeyRangeNode} to be inserted
   */
  private KeyRangeNode insert(KeyRangeNode root, KeyRangeNode newNode) {
    if (root == null) {
      root = newNode;
      return root;
    }

    if (root.compareTo(newNode) == 0) {
      root.addFiles(newNode.getFileNameList());
      return root;
    }

    if (root.compareTo(newNode) < 0) {
      if (root.getRight() == null) {
        root.setRightSubTreeMax(newNode.getMaxRecordKey());
        root.setRightSubTreeMin(newNode.getMinRecordKey());
        root.setRight(newNode);
      } else {
        if (root.getRightSubTreeMax().compareTo(newNode.getMaxRecordKey()) < 0) {
          root.setRightSubTreeMax(newNode.getMaxRecordKey());
        }
        if (root.getRightSubTreeMin().compareTo(newNode.getMinRecordKey()) > 0) {
          root.setRightSubTreeMin(newNode.getMinRecordKey());
        }
        insert(root.getRight(), newNode);
      }
    } else {
      if (root.getLeft() == null) {
        root.setLeftSubTreeMax(newNode.getMaxRecordKey());
        root.setLeftSubTreeMin(newNode.getMinRecordKey());
        root.setLeft(newNode);
      } else {
        if (root.getLeftSubTreeMax().compareTo(newNode.getMaxRecordKey()) < 0) {
          root.setLeftSubTreeMax(newNode.getMaxRecordKey());
        }
        if (root.getLeftSubTreeMin().compareTo(newNode.getMinRecordKey()) > 0) {
          root.setLeftSubTreeMin(newNode.getMinRecordKey());
        }
        insert(root.getLeft(), newNode);
      }
    }
    return root;
  }

  /**
   * Fetches all the matching index files where the key could possibly be present.
   *
   * @param lookupKey the key to be searched for
   * @return the {@link Set} of matching index file names
   */
  Set<String> getMatchingIndexFiles(String lookupKey) {
    Set<String> matchingFileNameSet = new HashSet<>();
    getMatchingIndexFiles(getRoot(), lookupKey, matchingFileNameSet);
    return matchingFileNameSet;
  }

  /**
   * Fetches all the matching index files where the key could possibly be present.
   *
   * @param root refers to the current root of the look up tree
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
