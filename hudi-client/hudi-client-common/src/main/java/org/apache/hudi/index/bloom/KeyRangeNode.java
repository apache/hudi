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

import org.apache.hudi.common.util.rbtree.RedBlackTreeNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a red-black tree node in the {@link KeyRangeLookupTree}. Holds information pertaining to a single index file, viz file
 * name, min record key and max record key.
 */
class KeyRangeNode extends RedBlackTreeNode<RecordKeyRange> {

  private final List<String> fileNameList = new ArrayList<>();
  private String rightSubTreeMax = null;
  private String leftSubTreeMax = null;
  private String rightSubTreeMin = null;
  private String leftSubTreeMin = null;

  /**
   * Instantiates a new {@link KeyRangeNode}.
   *
   * @param minRecordKey min record key of the index file
   * @param maxRecordKey max record key of the index file
   * @param fileName     file name of the index file
   */
  KeyRangeNode(String minRecordKey, String maxRecordKey, String fileName) {
    super(new RecordKeyRange(minRecordKey, maxRecordKey));
    this.fileNameList.add(fileName);
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
    final RecordKeyRange key = getKey();
    String range = key != null ? "minRecordKey='" + key.getMinRecordKey() + '\'' + ", maxRecordKey='"
        + key.getMaxRecordKey() + "', " : "";
    return "KeyRangeNode{" + range + "fileNameList=" + fileNameList
        + ", rightSubTreeMax='" + rightSubTreeMax + '\'' + ", leftSubTreeMax='" + leftSubTreeMax + '\''
        + ", rightSubTreeMin='" + rightSubTreeMin + '\'' + ", leftSubTreeMin='" + leftSubTreeMin + '\'' + '}';
  }

  public KeyRangeNode getLeft() {
    return (KeyRangeNode) super.getLeft();
  }

  public KeyRangeNode getRight() {
    return (KeyRangeNode) super.getRight();
  }

  public List<String> getFileNameList() {
    return fileNameList;
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

  public String getMinRecordKey() {
    return getKey().getMinRecordKey();
  }

  public String getMaxRecordKey() {
    return getKey().getMaxRecordKey();
  }
}
