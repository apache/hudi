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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieTable;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

import java.util.Iterator;
import java.util.List;

public class SparkBulkInsertRowWriter {

  /**
   * Perform bulk insert for {@link Dataset<Row>}, will not change timeline/index, return
   * information about write files.
   */
  public static HoodieData<WriteStatus> bulkInsert(Dataset<Row> dataset,
                                            String instantTime,
                                            HoodieTable table,
                                            HoodieWriteConfig writeConfig,
                                            BulkInsertPartitioner<Dataset<Row>> partitioner,
                                            int parallelism,
                                            boolean preserveHoodieMetadata) {
    Dataset<Row> repartitionedDataset = partitioner.repartitionRecords(dataset, parallelism);

    boolean arePartitionRecordsSorted = partitioner.arePartitionRecordsSorted();
    StructType schema = dataset.schema();
    List<WriteStatus> writeStatuses = repartitionedDataset.queryExecution().toRdd().toJavaRDD().mapPartitions(
        (FlatMapFunction<Iterator<InternalRow>, WriteStatus>) rowIterator -> {
          TaskContextSupplier taskContextSupplier = table.getTaskContextSupplier();
          int taskPartitionId = taskContextSupplier.getPartitionIdSupplier().get();
          long taskId = taskContextSupplier.getStageIdSupplier().get();
          long taskEpochId = taskContextSupplier.getAttemptIdSupplier().get();

          final BulkInsertDataInternalWriterHelper writer =
              new BulkInsertDataInternalWriterHelper(table, writeConfig, instantTime, taskPartitionId, taskId, taskEpochId,
                  schema, writeConfig.populateMetaFields(), arePartitionRecordsSorted, preserveHoodieMetadata);
          while (rowIterator.hasNext()) {
            writer.write(rowIterator.next());
          }
          return writer.getWriteStatuses()
              .stream()
              .map(internalWriteStatus -> {
                WriteStatus status = new WriteStatus(
                    internalWriteStatus.isTrackSuccessRecords(), internalWriteStatus.getFailureFraction());
                status.setFileId(internalWriteStatus.getFileId());
                status.setTotalRecords(internalWriteStatus.getTotalRecords());
                status.setPartitionPath(internalWriteStatus.getPartitionPath());
                status.setStat(internalWriteStatus.getStat());
                return status;
              }).iterator();
        }).collect();
    return table.getContext().parallelize(writeStatuses);
  }
}
