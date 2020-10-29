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

/**
 * Repartition input records into at least expected number of output spark partitions. It should give below guarantees -
 * Output spark partition will have records from only one hoodie partition. - Average records per output spark
 * partitions should be almost equal to (#inputRecords / #outputSparkPartitions) to avoid possible skews.
 */
public interface BulkInsertPartitioner<I> {

  /**
   * Repartitions the input records into at least expected number of output spark partitions.
   *
   * @param records               Input Hoodie records
   * @param outputSparkPartitions Expected number of output partitions
   * @return
   */
  I repartitionRecords(I records, int outputSparkPartitions);

  /**
   * @return {@code true} if the records within a partition are sorted; {@code false} otherwise.
   */
  boolean arePartitionRecordsSorted();
}
