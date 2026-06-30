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

package org.apache.hudi.metadata;

import org.apache.hudi.common.util.Option;

import java.io.Serializable;

/**
 * Parsed representation of an MDT fileId, produced by
 * {@link HoodieMetadataTableLayout#parseFileId}.
 *
 * <p>Examples:
 * <ul>
 *   <li>Non-partitioned RLI fileId {@code "record-index-1500-0"} →
 *       {@code FileIdInfo(fileGroupIndex=1500, dataPartitionName=Option.empty())}.</li>
 *   <li>Files-partition fileId {@code "files-0000-0"} →
 *       {@code FileIdInfo(fileGroupIndex=0, dataPartitionName=Option.empty())}.</li>
 *   <li>Expression-index fileId {@code "expr-index-idx-datestr-0000-0"} →
 *       {@code FileIdInfo(fileGroupIndex=0, dataPartitionName=Option.empty())}.</li>
 *   <li>Partitioned RLI fileId {@code "record-index-default-0003-0"} (where {@code default} is the
 *       data-table partition name) → {@code FileIdInfo(fileGroupIndex=3, dataPartitionName=Option.of("default"))}.
 *       Note: built-in layouts in this patch return {@code Option.empty()} for partitioned RLI
 *       since they fall back to the flat layout; custom layouts may choose to populate it.</li>
 * </ul>
 */
public final class FileIdInfo implements Serializable {

  private final int fileGroupIndex;
  private final Option<String> dataPartitionName;

  public FileIdInfo(int fileGroupIndex, Option<String> dataPartitionName) {
    this.fileGroupIndex = fileGroupIndex;
    this.dataPartitionName = dataPartitionName;
  }

  public int getFileGroupIndex() {
    return fileGroupIndex;
  }

  public Option<String> getDataPartitionName() {
    return dataPartitionName;
  }
}
