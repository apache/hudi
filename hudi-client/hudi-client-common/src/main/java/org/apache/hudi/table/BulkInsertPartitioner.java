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

package org.apache.hudi.table;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.execution.bulkinsert.BulkInsertSortMode;
import org.apache.hudi.io.WriteHandleFactory;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import java.io.Serializable;

/**
 * Partitions the input records for bulk insert operation.
 * <p>
 * The actual implementation of {@link BulkInsertPartitioner} is determined by the bulk insert
 * sort mode, {@link BulkInsertSortMode}, specified by
 * {@code HoodieWriteConfig.BULK_INSERT_SORT_MODE} (`hoodie.bulkinsert.sort.mode`).
 */
public interface BulkInsertPartitioner<I> extends Serializable {

  /**
   * Partitions the input records based on the number of output partitions as a hint.
   * <p>
   * Note that, the number of output partitions may or may not be enforced, depending on the
   * specific implementation.
   *
   * @param records          Input Hoodie records.
   * @param outputPartitions Expected number of output partitions as a hint.
   * @return Repartitioned records.
   */
  I repartitionRecords(I records, int outputPartitions);

  /**
   * @return {@code true} if the records are sorted by partition-path; {@code false} otherwise.
   */
  boolean arePartitionRecordsSorted();

  /**
   * Return file group id prefix for the given data partition.
   * By default, return a new file group id prefix, so that incoming records will route to a fresh new file group
   *
   * @param partitionId data partition
   * @return File group id prefix.
   */
  default String getFileIdPfx(int partitionId) {
    return FSUtils.createNewFileIdPfx();
  }

  /**
   * Return write handle factory for the given partition.
   *
   * @param partitionId data partition
   * @return {@link WriteHandleFactory}
   */
  default Option<WriteHandleFactory> getWriteHandleFactory(int partitionId) {
    return Option.empty();
  }

  /**
   * Whether the records being written carry a partition path, derived from the write config alone.
   * <p>
   * A partitioner named through
   * {@code HoodieWriteConfig.BULKINSERT_USER_DEFINED_PARTITIONER_CLASS_NAME} is instantiated by
   * reflection with only the write config, so an implementation that otherwise takes the flag from
   * the {@link HoodieTable} has nothing else to derive it from.
   * <p>
   * The factory path takes the flag from {@link HoodieTable#isPartitioned()}, which reads
   * {@link HoodieTableConfig#PARTITION_FIELDS}. This prefers that same property, evaluated by
   * {@link HoodieTableConfig#getPartitionFields}, so a user defined use of these partitioners
   * repartitions the way the built in sort mode does whenever the table properties reached the
   * write config. Only when that property is absent does it fall back to the write side partition
   * path field, which is the best available signal for whether records will carry a non-empty
   * partition path.
   *
   * @param config Write config.
   * @return {@code true} if the table is partitioned; {@code false} otherwise.
   */
  static boolean isTablePartitioned(HoodieWriteConfig config) {
    Option<String[]> partitionFields = HoodieTableConfig.getPartitionFields(config);
    if (partitionFields.isPresent()) {
      return partitionFields.get().length > 0;
    }
    return !StringUtils.isNullOrEmpty(
        config.getProps().getProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key()));
  }
}
