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

package org.apache.hudi.client;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.StreamingMetadataWriteHandler;
import org.apache.hudi.table.HoodieTable;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * Class to assist with streaming writes to metadata table.
 */
public class SparkStreamingMetadataWriteHandler extends StreamingMetadataWriteHandler {

  @Override
  public HoodieData<WriteStatus> streamWriteToMetadataTable(HoodieTable table, HoodieData<WriteStatus> dataTableWriteStatuses, String instantTime,
                                                           int coalesceDivisorForDataTableWrites) {
    Option<HoodieTableMetadataWriter> metadataWriterOpt = getMetadataWriter(instantTime, table);
    ValidationUtils.checkState(metadataWriterOpt.isPresent(),
        "Cannot instantiate metadata writer for the table of interest " + table.getMetaClient().getBasePath());
    // Spark task retries and speculation are reconciled while the data-table commit is finalized.
    // Defer metadata generation until completeStreamingCommit can consume the resulting committed
    // HoodieCommitMetadata. This deliberately reuses the HUDI-8564 committed-output path instead
    // of deriving RLI/SI locations from pre-commit WriteStatus task attempts.
    return coalesceDataTableWriteStatuses(dataTableWriteStatuses, coalesceDivisorForDataTableWrites);
  }

  private HoodieData<WriteStatus> coalesceDataTableWriteStatuses(HoodieData<WriteStatus> dataTableWriteStatuses,
                                                                 int coalesceDivisorForDataTableWrites) {
    int coalesceParallelism = Math.max(1, dataTableWriteStatuses.getNumPartitions() / coalesceDivisorForDataTableWrites);
    // Coalesce to fewer Spark tasks before collecting successful task-attempt outputs.
    // The partitionBy shuffle also materializes the data writes before commit finalization.
    return HoodieJavaRDD.of(HoodieJavaRDD.getJavaRDD(dataTableWriteStatuses)
        .mapToPair((PairFunction<WriteStatus, String, WriteStatus>) writeStatus -> new Tuple2(writeStatus.getStat().getPath(), writeStatus))
        .partitionBy(new CoalescingPartitioner(coalesceParallelism))
        .map((Function<Tuple2<String, WriteStatus>, WriteStatus>) entry -> entry._2));
  }
}
