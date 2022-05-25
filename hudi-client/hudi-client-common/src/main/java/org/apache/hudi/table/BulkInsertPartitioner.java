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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.io.WriteHandleFactory;

import java.io.Serializable;

/**
 * Repartition input records into at least expected number of output partitions. It should give below guarantees -
 * Output partition will have records from only one hoodie partition. - Average records per output
 * partitions should be almost equal to (#inputRecords / #outputPartitions) to avoid possible skews.
 */
public interface BulkInsertPartitioner<I> extends Serializable {

  /**
   * Repartitions the input records into at least expected number of output partitions.
   *
   * @param records          Input Hoodie records
   * @param outputPartitions Expected number of output partitions
   * @return
   */
  I repartitionRecords(I records, int outputPartitions);

  /**
   * @return {@code true} if the records within a partition are sorted; {@code false} otherwise.
   */
  boolean arePartitionRecordsSorted();

  /**
   * Return file group id prefix for the given data partition.
   * By defauult, return a new file group id prefix, so that incoming records will route to a fresh new file group
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

}
