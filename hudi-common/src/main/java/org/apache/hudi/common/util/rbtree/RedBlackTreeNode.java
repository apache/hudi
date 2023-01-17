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

import org.jetbrains.annotations.NotNull;

import java.io.Serializable;

/**
 * Represents a red black tree node in the {@link RedBlackTree}.
 */
@SuppressWarnings("rawtypes")
public class RedBlackTreeNode<K extends Comparable<K>> implements Comparable<RedBlackTreeNode<K>>, Serializable {

  private K key;
  private RedBlackTreeNode parent;
  private RedBlackTreeNode left;
  private RedBlackTreeNode right;
  private boolean red;

  public RedBlackTreeNode(K key) {
    this.key = key;
  }

  public K getKey() {
    return key;
  }

  public void setKey(K key) {
    this.key = key;
  }

  public RedBlackTreeNode getParent() {
    return parent;
  }

  public void setParent(RedBlackTreeNode parent) {
    this.parent = parent;
  }

  public RedBlackTreeNode getLeft() {
    return left;
  }

  public void setLeft(RedBlackTreeNode left) {
    this.left = left;
  }

  public RedBlackTreeNode getRight() {
    return right;
  }

  public void setRight(RedBlackTreeNode right) {
    this.right = right;
  }

  public boolean isRed() {
    return red;
  }

  public void setRed(boolean red) {
    this.red = red;
  }

  @Override
  public int compareTo(@NotNull RedBlackTreeNode<K> that) {
    return this.getKey().compareTo(that.getKey());
  }

  @Override
  public String toString() {
    return "RedBlackTreeNode{color=" + (red ? "red" : "black") + ", key=" + key + '}';
  }
}
