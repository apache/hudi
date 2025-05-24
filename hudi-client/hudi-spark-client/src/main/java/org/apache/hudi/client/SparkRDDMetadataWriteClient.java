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

import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.spark.api.java.JavaRDD;

import java.util.List;

import static org.apache.hudi.metadata.MetadataPartitionType.FILES;

public class SparkRDDMetadataWriteClient<T> extends SparkRDDWriteClient<T> {

  public SparkRDDMetadataWriteClient(HoodieEngineContext context, HoodieWriteConfig clientConfig) {
    super(context, clientConfig);
  }

  public SparkRDDMetadataWriteClient(HoodieEngineContext context, HoodieWriteConfig writeConfig,
                                     Option<EmbeddedTimelineService> timelineService) {
    super(context, writeConfig, timelineService);
  }

  /**
   * Used for streaming writes to metadata table.
   * With streaming writes, writes to metadata happens in the same RDD stage boundary as data table writes and hence this takes a special
   * route where dag will not be dereferenced at all. If we take regular router, workload profile building might dereference the dag.
   */
  @Override
  public JavaRDD<WriteStatus> upsertPreppedRecords(JavaRDD<HoodieRecord<T>> preppedRecords, String instantTime, Option<List<Pair<String, String>>> partitionFileIdPairsOpt) {
    ValidationUtils.checkArgument(isMetadataTable, "This version of upsert prepped can only be invoked for metadata table");
    HoodieTable<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> table =
        initTable(WriteOperationType.UPSERT_PREPPED, Option.ofNullable(instantTime));
    table.validateUpsertSchema();
    if (!partitionFileIdPairsOpt.map(partitionFileIdPairs -> partitionFileIdPairs.get(0).getKey().equals(FILES.getPartitionPath())).orElse(false)) {
      // we do not want to call prewrite more than once for the same instant writing to metadata table twice.
      preWrite(instantTime, WriteOperationType.UPSERT_PREPPED, table.getMetaClient());
    }
    HoodieWriteMetadata<HoodieData<WriteStatus>> result = table.upsertPrepped(context, instantTime, HoodieJavaRDD.of(preppedRecords), partitionFileIdPairsOpt);
    HoodieWriteMetadata<JavaRDD<WriteStatus>> resultRDD = result.clone(HoodieJavaRDD.getJavaRDD(result.getWriteStatuses()));
    return postWrite(resultRDD, instantTime, table);
  }
}
