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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.io.CreateHandleFactory;
import org.apache.hudi.io.WriteHandleFactory;
import org.apache.hudi.table.BulkInsertPartitioner;

import java.util.Comparator;
import java.util.List;

/**
 * A {@code BulkInsertPartitioner} implementation for Metadata Table to improve performance of initialization of metadata
 * table partition when a very large number of records are inserted.
 *
 * <p>The records of a metadata table partition are tagged with the file group they hash to. The partitioner sorts them
 * by file group and then by record key, so each file group's records form one sorted run that is written with that
 * file group's file id prefix.
 *
 * @param <T> HoodieRecordPayload type
 */
public class JavaHoodieMetadataBulkInsertPartitioner<T>
    implements BulkInsertPartitioner<List<HoodieRecord<T>>> {

  @Override
  public List<HoodieRecord<T>> repartitionRecords(List<HoodieRecord<T>> records, int outputPartitions) {
    records.sort(Comparator.comparing((HoodieRecord<T> record) -> getFileIdPrefix(record))
        .thenComparing(record -> record.getKey().getRecordKey(), StringUtils.UTF8_LEXICOGRAPHIC_COMPARATOR));
    return records;
  }

  @Override
  public boolean arePartitionRecordsSorted() {
    return true;
  }

  /**
   * Each file group's run of records is written with its own handle factory, because a factory numbers the file ids it
   * creates and the first file of every file group must keep the index the metadata table initialized it with.
   */
  @Override
  public Option<WriteHandleFactory> getWriteHandleFactory(int partitionId) {
    return Option.of(new CreateHandleFactory<>(false));
  }

  /**
   * Returns the file id prefix of the metadata table file group the record is tagged with.
   */
  public static String getFileIdPrefix(HoodieRecord<?> record) {
    return HoodieTableMetadataUtil.getFileGroupPrefix(record.getCurrentLocation().getFileId());
  }
}
