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

package org.apache.hudi.utilities.util;

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;

import java.io.Serializable;
import java.util.Comparator;

/**
 * File-slice comparison helpers for the metadata table validator v2.
 *
 * <p>{@link #assertBaseFilesEquality} and {@link FileSliceComparator} are the same rules
 * {@link org.apache.hudi.utilities.HoodieMetadataTableValidator} applies to latest file slices, so
 * the two validators agree on what makes two slices' base files equal and how slices are ordered.
 */
public class HoodieMetadataValidationUtils {

  private HoodieMetadataValidationUtils() {
  }

  /**
   * Whether two file slices carry the same base file (name, file id and size), or both carry none.
   */
  static boolean assertBaseFilesEquality(FileSlice fileSlice1, FileSlice fileSlice2) {
    if (fileSlice1.getBaseFile().isPresent() && fileSlice2.getBaseFile().isPresent()) {
      HoodieBaseFile baseFile1 = fileSlice1.getBaseFile().get();
      HoodieBaseFile baseFile2 = fileSlice2.getBaseFile().get();
      return baseFile1.getFileName().equals(baseFile2.getFileName()) && baseFile1.getFileId().equals(baseFile2.getFileId())
          && baseFile1.getFileSize() == baseFile2.getFileSize();
    } else {
      return fileSlice1.getBaseFile().isPresent() == fileSlice2.getBaseFile().isPresent();
    }
  }

  /** Orders file slices by partition path, file id and base instant. */
  public static class FileSliceComparator implements Comparator<FileSlice>, Serializable {

    @Override
    public int compare(FileSlice o1, FileSlice o2) {
      return (o1.getPartitionPath() + o1.getFileId() + o1.getBaseInstantTime())
          .compareTo(o2.getPartitionPath() + o2.getFileId() + o2.getBaseInstantTime());
    }
  }
}
