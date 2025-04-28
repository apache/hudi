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

package org.apache.hudi.metadata.index;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.util.Lazy;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface Indexer {
  List<InitialIndexPartitionData> build(
      List<HoodieTableMetadataUtil.DirectoryInfo> partitionInfoList,
      Map<String, Map<String, Long>> partitionToFilesMap,
      String createInstantTime,
      Lazy<HoodieTableFileSystemView> fsView,
      HoodieBackedTableMetadata metadata,
      String instantTimeForPartition) throws IOException;

  default void updateTableConfig() {
    // No index-specific table config update by default
  }

  class IndexPartitionData {
    private final String partitionName;
    private final HoodieData<HoodieRecord> records;

    private IndexPartitionData(String partitionName, HoodieData<HoodieRecord> records) {
      this.partitionName = partitionName;
      this.records = records;
    }

    public static IndexPartitionData of(String partitionName, HoodieData<HoodieRecord> records) {
      return new IndexPartitionData(partitionName, records);
    }

    public String partitionName() {
      return partitionName;
    }

    public HoodieData<HoodieRecord> records() {
      return records;
    }
  }

  class InitialIndexPartitionData {
    private final int numFileGroup;
    private final IndexPartitionData partitionedRecords;

    private InitialIndexPartitionData(int numFileGroup,
                                      String partitionName,
                                      HoodieData<HoodieRecord> records) {
      this.numFileGroup = numFileGroup;
      this.partitionedRecords = IndexPartitionData.of(partitionName, records);
    }

    public static InitialIndexPartitionData of(int numFileGroup,
                                               String partitionName,
                                               HoodieData<HoodieRecord> records) {
      ValidationUtils.checkArgument(numFileGroup > 0,
          "The number of file groups of the index data should be positive");
      return new InitialIndexPartitionData(numFileGroup, partitionName, records);
    }

    public int numFileGroup() {
      return numFileGroup;
    }

    public String partitionName() {
      return partitionedRecords.partitionName();
    }

    public HoodieData<HoodieRecord> records() {
      return partitionedRecords.records();
    }
  }
}
