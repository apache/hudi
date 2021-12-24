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
import org.apache.hudi.io.WriteHandleFactory;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Repartition input records into at least expected number of output spark partitions. It should give below guarantees -
 * Output spark partition will have records from only one hoodie partition. - Average records per output spark
 * partitions should be almost equal to (#inputRecords / #outputSparkPartitions) to avoid possible skews.
 */
public abstract class BulkInsertPartitioner<I> implements Serializable {

  private WriteHandleFactory defaultWriteHandleFactory;
  private List<String> fileIdPfx;

  /**
   * Repartitions the input records into at least expected number of output spark partitions,
   * and generates fileIdPfx for each partition.
   *
   * @param records               Input Hoodie records
   * @param outputSparkPartitions Expected number of output partitions
   * @return
   */
  public abstract I repartitionRecords(I records, int outputSparkPartitions);

  /**
   * @return {@code true} if the records within a partition are sorted; {@code false} otherwise.
   */
  public abstract boolean arePartitionRecordsSorted();

  public List<String> getFileIdPfx() {
    return fileIdPfx;
  }

  public void setDefaultWriteHandleFactory(WriteHandleFactory defaultWriteHandleFactory) {
    this.defaultWriteHandleFactory = defaultWriteHandleFactory;
  }

  /**
   * Return write handle factory for the given partition.
   * By default, return the pre-assigned write handle factory for all partitions
   * @param partition data partition
   * @return
   */
  public WriteHandleFactory getWriteHandleFactory(int partition) {
    return defaultWriteHandleFactory;
  }

  /**
   * Initialize a list of file id prefix randomly.
   * In most cases, bulk_insert put all incoming records to randomly generated file groups (i.e., the current default implementation).
   * @param parallelism the number of output file id
   * @return lists of file groups, the Nth element corresponds to partition N
   */
  protected void generateFileIdPfx(int parallelism) {
    fileIdPfx = IntStream.range(0, parallelism).mapToObj(i -> FSUtils.createNewFileIdPfx()).collect(Collectors.toList());
  }
}
