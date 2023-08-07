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

import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieNotSupportedException;

import org.apache.hadoop.conf.Configuration;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy.EAGER;

public class JavaHoodieBackedTableMetadataWriter extends HoodieBackedTableMetadataWriter<List<HoodieRecord>> {

  /**
   * Hudi backed table metadata writer.
   *
   * @param hadoopConf                 Hadoop configuration to use for the metadata writer
   * @param writeConfig                Writer config
   * @param failedWritesCleaningPolicy Cleaning policy on failed writes
   * @param engineContext              Engine context
   * @param inflightInstantTimestamp   Timestamp of any instant in progress
   */
  protected JavaHoodieBackedTableMetadataWriter(Configuration hadoopConf, HoodieWriteConfig writeConfig, HoodieFailedWritesCleaningPolicy failedWritesCleaningPolicy, HoodieEngineContext engineContext,
                                                Option<String> inflightInstantTimestamp) {
    super(hadoopConf, writeConfig, failedWritesCleaningPolicy, engineContext, inflightInstantTimestamp);
  }

  public static HoodieTableMetadataWriter create(Configuration conf,
                                                 HoodieWriteConfig writeConfig,
                                                 HoodieEngineContext context,
                                                 Option<String> inflightInstantTimestamp) {
    return new JavaHoodieBackedTableMetadataWriter(
        conf, writeConfig, EAGER, context, inflightInstantTimestamp);
  }

  public static HoodieTableMetadataWriter create(Configuration conf,
                                                 HoodieWriteConfig writeConfig,
                                                 HoodieFailedWritesCleaningPolicy failedWritesCleaningPolicy,
                                                 HoodieEngineContext context,
                                                 Option<String> inflightInstantTimestamp) {
    return new JavaHoodieBackedTableMetadataWriter(
        conf, writeConfig, failedWritesCleaningPolicy, context, inflightInstantTimestamp);
  }

  @Override
  protected void initRegistry() {
    if (metadataWriteConfig.isMetricsOn()) {
      Registry registry = Registry.getRegistry("HoodieMetadata");
      this.metrics = Option.of(new HoodieMetadataMetrics(registry));
    } else {
      this.metrics = Option.empty();
    }
  }

  @Override
  protected void commit(String instantTime, Map<MetadataPartitionType, HoodieData<HoodieRecord>> partitionRecordsMap) {
    commitInternal(instantTime, partitionRecordsMap, false, Option.empty());
  }

  @Override
  protected List<HoodieRecord> convertHoodieDataToEngineSpecificData(HoodieData<HoodieRecord> records) {
    return records.collectAsList();
  }

  @Override
  protected void bulkCommit(String instantTime, MetadataPartitionType partitionType, HoodieData<HoodieRecord> records, int fileGroupCount) {
    commitInternal(instantTime, Collections.singletonMap(partitionType, records), true, Option.of(new JavaHoodieMetadataBulkInsertPartitioner()));
  }

  @Override
  protected BaseHoodieWriteClient<?, List<HoodieRecord>, ?, ?> initializeWriteClient() {
    return new HoodieJavaWriteClient(engineContext, metadataWriteConfig);
  }

  @Override
  public void deletePartitions(String instantTime, List<MetadataPartitionType> partitions) {
    throw new HoodieNotSupportedException("Dropping metadata index not supported for Java metadata table yet.");
  }

  @Override
  protected void preWrite(String instantTime) {
    metadataMetaClient.getActiveTimeline().transitionRequestedToInflight(HoodieActiveTimeline.DELTA_COMMIT_ACTION, instantTime);
  }
}
