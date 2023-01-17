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

import java.io.Serializable;

/**
 * Red–black tree is a kind of self-balancing binary search tree. Each node has its "color" ("red" or "black"),
 * used to ensure that the tree remains balanced during insertions and deletions. The re-balancing is not perfect,
 * but guarantees searching in O(log N) time, where n is the number of entries.
 * In addition to the requirements imposed on a binary search tree, the following must be satisfied by a red–black tree:
 * 1. Every node has a color either red or black.
 * 2. The root of the tree is always black.
 * 3. There are no two adjacent red nodes (A red node cannot have a red parent or red child).
 * 4. Every path from a node (including root) to any of its descendants NULL nodes has the same number of black nodes.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class RedBlackTree<T extends RedBlackTreeNode<K>, K extends Comparable<K>> implements Serializable {

  private RedBlackTreeNode root;

  /**
   * @return the root of the tree. Could be {@code null}
   */
  public T getRoot() {
    return (T) root;
  }

  /**
   * Binary search for key.
   */
  public T search(K key) {
    return (T) searchInternal(getRoot(), key);
  }

  /**
   * Insert a new node. After every insertion operation, we need to check all the properties of red-black Tree.
   * If all the properties are satisfied then we go to next operation otherwise we perform the following operation
   * to make it Red Black Tree.
   * 1. Recolor
   * 2. Rotation
   * 3. Rotation followed by Recolor
   */
  public void insert(T newNode) {
    insertInternal(newNode);
  }

  /**
   * Delete a node by key. The operation is similar to insertion.
   */
  public void remove(K key) {
    RedBlackTreeNode node;
    if ((node = searchInternal(root, key)) != null) {
      removeInternal(node);
    }
  }

  /**
   * Handle the situation where duplicate nodes are inserted.
   *
   * @param oldNode previously inserted node
   * @param newNode newly inserted duplicate node
   */
  protected void processWhenInsertSame(T oldNode, T newNode) {
  }

  private RedBlackTreeNode searchInternal(RedBlackTreeNode<K> node, K key) {
    if (node == null) {
      return null;
    }
    int cmp = key.compareTo(node.getKey());
    if (cmp < 0) {
      return searchInternal(node.getLeft(), key);
    } else if (cmp > 0) {
      return searchInternal(node.getRight(), key);
    } else {
      return node;
    }
  }

  private void insertInternal(RedBlackTreeNode newNode) {
    if (newNode == null) {
      return;
    }
    if (root == null) {
      root = newNode;
      setBlack(root);
      return;
    }
    RedBlackTreeNode parent = getRoot();
    RedBlackTreeNode current = parent;
    while (current != null) {
      parent = current;
      int compare = compare(newNode, current);
      if (compare == 0) {
        processWhenInsertSame((T) current, (T) newNode);
        return;
      }
      if (compare < 0) {
        current = current.getLeft();
      } else {
        current = current.getRight();
      }
    }
    newNode.setParent(parent);
    setRed(newNode);
    if (compare(newNode, parent) < 0) {
      parent.setLeft(newNode);
    } else {
      parent.setRight(newNode);
    }
    balanceInsert(newNode);
  }

  private void balanceInsert(RedBlackTreeNode node) {
    RedBlackTreeNode parent;
    RedBlackTreeNode gparent;
    while ((parent = node.getParent()) != null && isRed(parent)) {
      gparent = parent.getParent();
      if (isLeftChild(gparent, parent)) {
        RedBlackTreeNode uncle = gparent.getRight();
        if (uncle != null && isRed(uncle)) {
          setBlack(uncle);
          setBlack(parent);
          setRed(gparent);
          node = gparent;
          continue;
        }

        if (isRightChild(parent, node)) {
          leftRotate(parent);
          RedBlackTreeNode tmp = parent;
          parent = node;
          node = tmp;
        }
        setBlack(parent);
        setRed(gparent);
        rightRotate(gparent);
      } else {
        RedBlackTreeNode uncle = gparent.getLeft();
        if (uncle != null && isRed(uncle)) {
          setBlack(uncle);
          setBlack(parent);
          setRed(gparent);
          node = gparent;
          continue;
        }
        if (isLeftChild(parent, node)) {
          rightRotate(parent);
          RedBlackTreeNode tmp = parent;
          parent = node;
          node = tmp;
        }
        setBlack(parent);
        setRed(gparent);
        leftRotate(gparent);
      }
    }
    setBlack(this.root);
  }

  private void removeInternal(RedBlackTreeNode node) {
    RedBlackTreeNode child;
    RedBlackTreeNode parent;
    boolean isReadNode;
    if ((node.getLeft() != null) && (node.getRight() != null)) {
      RedBlackTreeNode replace = node;
      replace = replace.getRight();
      while (replace.getLeft() != null) {
        replace = replace.getLeft();
      }
      RedBlackTreeNode nodeParent = node.getParent();
      if (nodeParent != null) {
        if (nodeParent.getLeft() == node) {
          nodeParent.setLeft(replace);
        } else {
          nodeParent.setRight(replace);
        }
      } else {
        this.root = replace;
      }
      child = replace.getRight();
      parent = replace.getParent();
      isReadNode = isRed(replace);
      if (parent == node) {
        parent = replace;
      } else {
        if (child != null) {
          child.setParent(parent);
        }
        parent.setLeft(child);
        replace.setRight(node.getRight());
        node.getRight().setParent(replace);
      }

      replace.setParent(node.getParent());
      replace.setRed(isRed(node));
      replace.setLeft(node.getLeft());
      node.getLeft().setParent(replace);
      if (!isReadNode) {
        balanceRemove(child, parent);
      }
      return;
    }
    if (node.getLeft() != null) {
      child = node.getLeft();
    } else {
      child = node.getRight();
    }

    parent = node.getParent();
    isReadNode = isRed(node);
    if (child != null) {
      child.setParent(parent);
    }
    if (parent != null) {
      if (parent.getLeft() == node) {
        parent.setLeft(child);
      } else {
        parent.setRight(child);
      }
    } else {
      this.root = child;
    }

    if (!isReadNode) {
      balanceRemove(child, parent);
    }
  }

  private void balanceRemove(RedBlackTreeNode node, RedBlackTreeNode parent) {
    RedBlackTreeNode brother;
    while ((node == null || isBlack(node)) && (node != this.root)) {
      if (isLeftChild(parent, node)) {
        brother = parent.getRight();
        if (isRed(brother)) {
          setBlack(brother);
          setRed(parent);
          leftRotate(parent);
          brother = parent.getRight();
        }

        if ((brother.getLeft() == null || isBlack(brother.getLeft()))
            && (brother.getRight() == null || isBlack(brother.getRight()))) {
          setRed(brother);
          node = parent;
          parent = node.getParent();
        } else {
          if (brother.getRight() == null || isBlack(brother.getRight())) {
            setBlack(brother.getLeft());
            setRed(brother);
            rightRotate(brother);
            brother = parent.getRight();
          }
          brother.setRed(parent.isRed());
          setBlack(parent);
          setBlack(brother.getRight());
          leftRotate(parent);
          node = this.root;
          break;
        }
      } else {
        brother = parent.getLeft();
        if (isRed(brother)) {
          setBlack(brother);
          setRed(parent);
          rightRotate(parent);
          brother = parent.getLeft();
        }
        if ((brother.getLeft() == null || isBlack(brother.getLeft()))
            && (brother.getRight() == null || isBlack(brother.getRight()))) {
          setRed(brother);
          node = parent;
          parent = node.getParent();
        } else {

          if (brother.getLeft() == null || isBlack(brother.getLeft())) {
            setBlack(brother.getRight());
            setRed(brother);
            leftRotate(brother);
            brother = parent.getLeft();
          }
          brother.setRed(isRed(parent));
          setBlack(parent);
          setBlack(brother.getLeft());
          rightRotate(parent);
          node = this.root;
          break;
        }
      }
    }
    if (node != null) {
      setBlack(node);
    }
  }

  /**
   * Left rotate the nodes to fit the red-black tree definition.
   *      px                              px
   *     /                               /
   *    x                               y
   *   /  \      -- left rotate --     / \
   *  lx   y                          x  ry
   *     /   \                       /  \
   *    ly   ry                     lx  ly
   */
  private void leftRotate(RedBlackTreeNode x) {

    RedBlackTreeNode y = x.getRight();
    x.setRight(y.getLeft());
    if (y.getLeft() != null) {
      y.getLeft().setParent(x);
    }
    RedBlackTreeNode xParent = x.getParent();
    y.setParent(xParent);
    if (xParent == null) {
      this.root = y;
    } else {
      if (isLeftChild(xParent, x)) {
        xParent.setLeft(y);
      } else {
        xParent.setRight(y);
      }
    }
    x.setParent(y);
    y.setLeft(x);
  }

  /**
   * Right rotate the nodes to fit the red-black tree definition.
   *            py                               py
   *           /                                /
   *          y                                x
   *         /  \      -- right rotate --     /  \
   *        x   ry                           lx   y
   *       / \                                   / \
   *      lx  rx                                rx  ry
   */
  private void rightRotate(RedBlackTreeNode y) {
    RedBlackTreeNode x = y.getLeft();
    y.setLeft(x.getRight());
    if (x.getRight() != null) {
      x.getRight().setParent(y);
    }
    RedBlackTreeNode yParent = y.getParent();
    x.setParent(yParent);
    if (yParent == null) {
      this.root = x;
    } else {
      if (isLeftChild(yParent, y)) {
        yParent.setLeft(x);
      } else {
        yParent.setRight(x);
      }
    }
    x.setRight(y);
    y.setParent(x);
  }

  private void setBlack(RedBlackTreeNode node) {
    if (node != null) {
      node.setRed(false);
    }
  }

  private void setRed(RedBlackTreeNode node) {
    if (node != null) {
      node.setRed(true);
    }
  }

  /**
   * Whether the child node is the left node of the parent.
   */
  private boolean isLeftChild(RedBlackTreeNode parent, RedBlackTreeNode child) {
    if (parent == null) {
      return false;
    }
    return child == parent.getLeft();
  }

  /**
   * Whether the child node is the right node of the parent.
   */
  private boolean isRightChild(RedBlackTreeNode parent, RedBlackTreeNode child) {
    if (parent == null) {
      return false;
    }
    return child == parent.getRight();
  }

  private int compare(RedBlackTreeNode compare, RedBlackTreeNode toCompare) {
    return compare.compareTo(toCompare);
  }

  private boolean isBlack(RedBlackTreeNode node) {
    return !isRed(node);
  }

  private boolean isRed(RedBlackTreeNode node) {
    return node != null && node.isRed();
  }
}
