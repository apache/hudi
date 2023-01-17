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

package org.apache.hudi.common.testutils;

import org.apache.hudi.common.util.rbtree.RedBlackTree;
import org.apache.hudi.common.util.rbtree.RedBlackTreeNode;

@SuppressWarnings("rawtypes")
public class RedBlackTreeTestUtil {

  /**
   * Method to check if the tree conforms to a red-black tree. The following must be satisfied:
   *  1. Root node is black.
   *  2. Red node does not have a red child.
   *  2. Every path from a given node to any of its descendant NIL nodes goes through the same number of black nodes.
   */
  public static <T extends RedBlackTree> boolean isRedBlackTree(T tree) {
    return checkNodesColor(tree.getRoot()) && checkBlackNodeNum(tree.getRoot()) != -1;
  }

  /**
   * Method to check the treeNode color. The following must be satisfied:
   *  1. Root node is black.
   *  2. Red node does not have a red child.
   */
  private static <N extends RedBlackTreeNode> boolean checkNodesColor(N node) {
    if (node == null) {
      return true;
    }
    // Root Node
    if (node.getParent() == null && node.isRed()) {
      return false;
    }
    if (node.isRed()) {
      if (node.getLeft() != null && node.getLeft().isRed()) {
        return false;
      }
      if (node.getRight() != null && node.getRight().isRed()) {
        return false;
      }
    }
    if (!checkNodesColor(node.getLeft()) || !checkNodesColor(node.getRight())) {
      return false;
    }
    return true;
  }

  /**
   * Method to check if every path from a given node to any of its descendant NIL nodes goes through
   * the same number of black nodes.
   */
  private static <N extends RedBlackTreeNode> int checkBlackNodeNum(N node) {
    if (node == null) {
      return 0;
    }
    int leftNum = checkBlackNodeNum(node.getLeft());
    int rightNum = checkBlackNodeNum(node.getRight());
    if (leftNum == -1 || rightNum == -1 || leftNum != rightNum) {
      return -1;
    }
    return leftNum + (node.isRed() ? 0 : 1);
  }
}
