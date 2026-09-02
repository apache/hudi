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

package org.apache.hudi.metadata.index.vector;

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.util.Option;

import java.io.Serializable;

/** Previous/current file-slice pair for one touched data file group. */
public final class VectorIndexFileGroupUpdate implements Serializable {

  private final String partitionPath;
  private final Option<FileSlice> previousSlice;
  private final FileSlice currentSlice;

  public VectorIndexFileGroupUpdate(
      String partitionPath, Option<FileSlice> previousSlice, FileSlice currentSlice) {
    this.partitionPath = partitionPath;
    this.previousSlice = previousSlice;
    this.currentSlice = currentSlice;
  }

  public String getPartitionPath() {
    return partitionPath;
  }

  public Option<FileSlice> getPreviousSlice() {
    return previousSlice;
  }

  public FileSlice getCurrentSlice() {
    return currentSlice;
  }
}
