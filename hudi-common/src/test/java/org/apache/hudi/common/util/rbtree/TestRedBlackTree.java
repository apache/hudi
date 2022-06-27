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

package org.apache.hudi.common.util.rbtree;

import org.apache.hudi.common.testutils.RedBlackTreeTestUtil;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedList;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestRedBlackTree {

  private static final Logger LOG = LogManager.getLogger(TestRedBlackTree.class);

  /**
   * Insert and remove nodes in order.
   */
  @Test
  public void testOrdered() {
    RedBlackTree<RedBlackTreeNode<Integer>, Integer> tree = new RedBlackTree<>();
    addNodesInRange(tree, 1, 100, false);
    removeNodesInRange(tree, 1, 10, false);
    addNodesInRange(tree, 101, 150, false);
    removeNodesInRange(tree, 30, 50, false);
  }

  /**
   * Insert and remove nodes randomly.
   */
  @Test
  public void testRandom() {
    RedBlackTree<RedBlackTreeNode<Integer>, Integer> tree = new RedBlackTree<>();
    addNodesInRange(tree, 1, 50, true);
    removeNodesInRange(tree, 10, 30, true);
    addNodesInRange(tree, 51, 75, true);
    removeNodesInRange(tree, 65, 70, true);
    addNodesInRange(tree, 76, 150, true);
    removeNodesInRange(tree, 130, 150, true);
  }

  private void addNodesInRange(RedBlackTree<RedBlackTreeNode<Integer>, Integer> tree, int start, int end, boolean random) {
    LinkedList<Integer> nodes = new LinkedList<>();
    for (int i = start; i <= end; i++) {
      nodes.add(i);
    }
    if (random) {
      Collections.shuffle(nodes);
      LOG.info("RedBlackTree insert nodes:" + nodes);
    }
    nodes.forEach(node -> tree.insert(new RedBlackTreeNode<>(node)));
    assertTrue(RedBlackTreeTestUtil.isRedBlackTree(tree));
  }

  private void removeNodesInRange(RedBlackTree<RedBlackTreeNode<Integer>, Integer> tree, int start, int end, boolean random) {
    LinkedList<Integer> nodes = new LinkedList<>();
    for (int i = start; i <= end; i++) {
      nodes.add(i);
    }
    if (random) {
      Collections.shuffle(nodes);
      LOG.info("RedBlackTree remove nodes:" + nodes);
    }
    nodes.forEach(tree::remove);
    assertTrue(RedBlackTreeTestUtil.isRedBlackTree(tree));
  }
}
