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

package org.apache.hudi.table.action.cluster;

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieClusteringException;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Builds a {@link Comparator} for {@link FileSlice} based on the
 * {@link org.apache.hudi.config.HoodieClusteringConfig#PLAN_STRATEGY_FILE_SLICES_SORT_BY} config.
 * The comparators for each field are combined in the order specified, so earlier fields take priority.
 */
public class ClusteringFileSliceComparator {

  public static Comparator<FileSlice> buildComparator(HoodieWriteConfig config) {
    String sortByFields = config.getFileSlicesSortBy();

    List<ClusteringFileSliceSortByField> fields = Arrays.stream(sortByFields.split(","))
        .map(String::trim)
        .map(s -> ClusteringFileSliceSortByField.valueOf(s.toUpperCase()))
        .collect(Collectors.toList());

    if (fields.isEmpty()) {
      throw new HoodieClusteringException("At least one sort field must be specified in: " + sortByFields);
    }

    Comparator<FileSlice> comparator = comparatorForField(fields.get(0), config);
    for (int i = 1; i < fields.size(); i++) {
      comparator = comparator.thenComparing(comparatorForField(fields.get(i), config));
    }
    return comparator;
  }

  private static Comparator<FileSlice> comparatorForField(ClusteringFileSliceSortByField field, HoodieWriteConfig config) {
    switch (field) {
      case INSTANT_TIME:
        return Comparator.comparing(fileSlice ->
            fileSlice.getBaseFile().map(baseFile -> baseFile.getCommitTime()).orElse(""));
      case SIZE:
        return Comparator.comparing(
            (FileSlice fileSlice) -> fileSlice.getBaseFile().map(baseFile -> baseFile.getFileSize()).orElse(config.getParquetMaxFileSize()),
            Comparator.reverseOrder());
      default:
        throw new HoodieClusteringException("Unknown file slice sort field: " + field);
    }
  }
}
