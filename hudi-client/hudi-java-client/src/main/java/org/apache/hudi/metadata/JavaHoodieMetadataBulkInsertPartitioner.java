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

package org.apache.hudi.metadata;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.table.BulkInsertPartitioner;

import java.util.Comparator;
import java.util.List;

/**
 * A {@code BulkInsertPartitioner} implementation for Metadata Table to improve performance of initialization of metadata
 * table partition when a very large number of records are inserted.
 *
 * @param <T> HoodieRecordPayload type
 */
public class JavaHoodieMetadataBulkInsertPartitioner<T>
    implements BulkInsertPartitioner<List<HoodieRecord<T>>> {
  private String fileId = null;

  @Override
  public List<HoodieRecord<T>> repartitionRecords(List<HoodieRecord<T>> records, int outputPartitions) {
    if (records.isEmpty()) {
      return records;
    }
    records.sort(Comparator.comparing(record -> record.getKey().getRecordKey()));
    fileId = HoodieTableMetadataUtil.getFileGroupPrefix(records.get(0).getCurrentLocation().getFileId());
    return records;
  }

  @Override
  public boolean arePartitionRecordsSorted() {
    return true;
  }

  @Override
  public String getFileIdPfx(int partitionId) {
    return fileId == null ? BulkInsertPartitioner.super.getFileIdPfx(partitionId) : fileId;
  }
}
