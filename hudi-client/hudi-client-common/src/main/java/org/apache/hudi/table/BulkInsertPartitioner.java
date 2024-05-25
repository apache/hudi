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
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.execution.bulkinsert.BulkInsertSortMode;
import org.apache.hudi.io.WriteHandleFactory;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.apache.hudi.common.util.StringUtils.isNullOrEmpty;

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
   * @return
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
   * @return
   */
  default String getFileIdPfx(int partitionId) {
    return FSUtils.createNewFileIdPfx();
  }

  /**
   * Return write handle factory for the given partition.
   *
   * @param partitionId data partition
   * @return
   */
  default Option<WriteHandleFactory> getWriteHandleFactory(int partitionId) {
    return Option.empty();
  }

  /*
   * If possible, we want to sort the data by partition path. Doing so will reduce the number of files written.
   * This will not change the desired sort order, it is just a performance improvement.
   **/
  static String[] tryPrependPartitionPathColumns(String[] columnNames, HoodieWriteConfig config) {
    String partitionPath;
    if (config.populateMetaFields()) {
      partitionPath = HoodieRecord.HoodieMetadataField.PARTITION_PATH_METADATA_FIELD.getFieldName();
    } else {
      partitionPath = config.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key());
    }
    if (isNullOrEmpty(partitionPath)) {
      return columnNames;
    }
    Set<String> sortCols = new LinkedHashSet<>(StringUtils.split(partitionPath, ","));
    sortCols.addAll(Arrays.asList(columnNames));
    return sortCols.toArray(new String[0]);
  }

  static Object[] prependPartitionPath(String partitionPath, Object[] columnValues) {
    Object[] prependColumnValues = new Object[columnValues.length + 1];
    System.arraycopy(columnValues, 0, prependColumnValues, 1, columnValues.length);
    prependColumnValues[0] = partitionPath;
    return prependColumnValues;
  }

}
