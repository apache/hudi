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

package org.apache.hudi.execution.bulkinsert;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.execution.SparkLazyInsertIterable;
import org.apache.hudi.io.WriteHandleFactory;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieTable;

import org.apache.spark.api.java.function.Function2;

import java.util.Iterator;
import java.util.List;

/**
 * Map function that handles a stream of HoodieRecords.
 */
public class BulkInsertMapFunction<T>
    implements Function2<Integer, Iterator<HoodieRecord<T>>, Iterator<List<WriteStatus>>> {

  private final String instantTime;
  private final boolean areRecordsSorted;
  private final HoodieWriteConfig config;
  private final HoodieTable hoodieTable;
  private final boolean useWriterSchema;
  private final BulkInsertPartitioner partitioner;
  private final WriteHandleFactory writeHandleFactory;

  public BulkInsertMapFunction(String instantTime, boolean areRecordsSorted,
                               HoodieWriteConfig config, HoodieTable hoodieTable,
                               boolean useWriterSchema, BulkInsertPartitioner partitioner,
                               WriteHandleFactory writeHandleFactory) {
    this.instantTime = instantTime;
    this.areRecordsSorted = areRecordsSorted;
    this.config = config;
    this.hoodieTable = hoodieTable;
    this.useWriterSchema = useWriterSchema;
    this.writeHandleFactory = writeHandleFactory;
    this.partitioner = partitioner;
  }

  @Override
  public Iterator<List<WriteStatus>> call(Integer partition, Iterator<HoodieRecord<T>> recordItr) {
    return new SparkLazyInsertIterable<>(recordItr, areRecordsSorted, config, instantTime, hoodieTable,
        partitioner.getFileIdPfx(partition), hoodieTable.getTaskContextSupplier(), useWriterSchema,
        (WriteHandleFactory) partitioner.getWriteHandleFactory(partition).orElse(this.writeHandleFactory));
  }
}
