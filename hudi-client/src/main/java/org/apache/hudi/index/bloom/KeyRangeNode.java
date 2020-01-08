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
import java.util.ArrayList;
import java.util.List;

/**
 * Represents a node in the {@link KeyRangeLookupTree}. Holds information pertaining to a single index file, viz file
 * name, min record key and max record key.
 */
class KeyRangeNode implements Comparable<KeyRangeNode>, Serializable {

  private final List<String> fileNameList = new ArrayList<>();
  private final String minRecordKey;
  private final String maxRecordKey;
  private String rightSubTreeMax = null;
  private String leftSubTreeMax = null;
  private String rightSubTreeMin = null;
  private String leftSubTreeMin = null;
  private KeyRangeNode left = null;
  private KeyRangeNode right = null;

  /**
   * Instantiates a new {@link KeyRangeNode}.
   *
   * @param minRecordKey min record key of the index file
   * @param maxRecordKey max record key of the index file
   * @param fileName file name of the index file
   */
  KeyRangeNode(String minRecordKey, String maxRecordKey, String fileName) {
    this.fileNameList.add(fileName);
    this.minRecordKey = minRecordKey;
    this.maxRecordKey = maxRecordKey;
  }

  /**
   * Adds a new file name list to existing list of file names.
   *
   * @param newFiles {@link List} of file names to be added
   */
  void addFiles(List<String> newFiles) {
    this.fileNameList.addAll(newFiles);
  }

  @Override
  public String toString() {
    return "KeyRangeNode{minRecordKey='" + minRecordKey + '\'' + ", maxRecordKey='" + maxRecordKey + '\''
        + ", fileNameList=" + fileNameList + ", rightSubTreeMax='" + rightSubTreeMax + '\'' + ", leftSubTreeMax='"
        + leftSubTreeMax + '\'' + ", rightSubTreeMin='" + rightSubTreeMin + '\'' + ", leftSubTreeMin='" + leftSubTreeMin
        + '\'' + '}';
  }

  /**
   * Compares the min record key of two nodes, followed by max record key.
   *
   * @param that the {@link KeyRangeNode} to be compared with
   * @return the result of comparison. 0 if both min and max are equal in both. 1 if this {@link KeyRangeNode} is
   * greater than the {@code that} keyRangeNode. -1 if {@code that} keyRangeNode is greater than this {@link
   * KeyRangeNode}
   */
  @Override
  public int compareTo(KeyRangeNode that) {
    int compareValue = minRecordKey.compareTo(that.minRecordKey);
    if (compareValue == 0) {
      return maxRecordKey.compareTo(that.maxRecordKey);
    } else {
      return compareValue;
    }
  }

  public List<String> getFileNameList() {
    return fileNameList;
  }

  public String getMinRecordKey() {
    return minRecordKey;
  }

  public String getMaxRecordKey() {
    return maxRecordKey;
  }

  public String getRightSubTreeMin() {
    return rightSubTreeMin;
  }

  public void setRightSubTreeMin(String rightSubTreeMin) {
    this.rightSubTreeMin = rightSubTreeMin;
  }

  public String getLeftSubTreeMin() {
    return leftSubTreeMin;
  }

  public void setLeftSubTreeMin(String leftSubTreeMin) {
    this.leftSubTreeMin = leftSubTreeMin;
  }

  public String getRightSubTreeMax() {
    return rightSubTreeMax;
  }

  public void setRightSubTreeMax(String rightSubTreeMax) {
    this.rightSubTreeMax = rightSubTreeMax;
  }

  public String getLeftSubTreeMax() {
    return leftSubTreeMax;
  }

  public void setLeftSubTreeMax(String leftSubTreeMax) {
    this.leftSubTreeMax = leftSubTreeMax;
  }

  public KeyRangeNode getLeft() {
    return left;
  }

  public void setLeft(KeyRangeNode left) {
    this.left = left;
  }

  public KeyRangeNode getRight() {
    return right;
  }

  public void setRight(KeyRangeNode right) {
    this.right = right;
  }
}
