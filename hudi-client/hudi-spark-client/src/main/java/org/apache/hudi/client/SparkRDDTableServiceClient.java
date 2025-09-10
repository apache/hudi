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
import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.client.utils.SparkReleaseResources;
import org.apache.hudi.client.utils.SparkValidatorUtils;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.spark.api.java.JavaRDD;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class SparkRDDTableServiceClient<T> extends BaseHoodieTableServiceClient<HoodieData<HoodieRecord<T>>, HoodieData<WriteStatus>, JavaRDD<WriteStatus>> {

  private final StreamingMetadataWriteHandler streamingMetadataWriteHandler = new StreamingMetadataWriteHandler();
  protected SparkRDDTableServiceClient(HoodieEngineContext context,
                                       HoodieWriteConfig clientConfig,
                                       Option<EmbeddedTimelineService> timelineService,
                                       TransactionManager transactionManager) {
    super(context, clientConfig, timelineService, transactionManager);
  }

  @Override
  protected TableWriteStats triggerWritesAndFetchWriteStats(HoodieWriteMetadata<JavaRDD<WriteStatus>> tableServiceWriteMetadata) {
    // Triggering the dag for writes.
    // If streaming writes are enabled, writes to both data table and metadata table get triggered at this juncture.
    // If not, writes to data table gets triggered here.
    // When streaming writes are enabled, data table's WriteStatus is expected to contain all stats required to generate metadata table records and so each object will be larger.
    // Here we are dropping all additional stats and error records to retain only the required information and prevent collecting large objects on the driver.
    List<SparkRDDWriteClient.SlimWriteStats> writeStatusMetadataTrackerList = SparkRDDWriteClient.SlimWriteStats.from(tableServiceWriteMetadata.getWriteStatuses());

    List<HoodieWriteStat> dataTableWriteStats = writeStatusMetadataTrackerList.stream()
        .filter(entry -> !entry.isMetadataTable())
        .map(SparkRDDWriteClient.SlimWriteStats::getWriteStat)
        .collect(Collectors.toList());
    List<HoodieWriteStat> mdtWriteStats = writeStatusMetadataTrackerList.stream()
        .filter(SparkRDDWriteClient.SlimWriteStats::isMetadataTable)
        .map(SparkRDDWriteClient.SlimWriteStats::getWriteStat)
        .collect(Collectors.toList());

    if (HoodieTableMetadata.isMetadataTable(config.getBasePath())) {
      ValidationUtils.checkArgument(dataTableWriteStats.isEmpty(), "Metadata table should not expect any data table write status.");
      return new TableWriteStats(mdtWriteStats, Collections.emptyList());
    }
    return new TableWriteStats(dataTableWriteStats, mdtWriteStats);
  }

  @Override
  protected HoodieWriteMetadata<HoodieData<WriteStatus>> partialUpdateTableMetadata(
      HoodieTable table,
      HoodieWriteMetadata<HoodieData<WriteStatus>> writeMetadata,
      String instantTime) {
    if (isStreamingWriteToMetadataEnabled(table)) {
      writeMetadata.setWriteStatuses(streamingMetadataWriteHandler.streamWriteToMetadataTable(table, writeMetadata.getWriteStatuses(), instantTime));
    }
    return writeMetadata;
  }

  @Override
  protected void writeToMetadataTable(HoodieTable table, String instantTime, HoodieCommitMetadata metadata, List<HoodieWriteStat> partialMetadataWriteStats) {
    if (isStreamingWriteToMetadataEnabled(table)) {
      streamingMetadataWriteHandler.commitToMetadataTable(table, instantTime, metadata, partialMetadataWriteStats);
    } else {
      writeTableMetadata(table, instantTime, metadata);
    }
  }

  @Override
  protected HoodieWriteMetadata<JavaRDD<WriteStatus>> convertToOutputMetadata(HoodieWriteMetadata<HoodieData<WriteStatus>> writeMetadata) {
    return writeMetadata.clone(HoodieJavaRDD.getJavaRDD(writeMetadata.getWriteStatuses()));
  }

  @Override
  protected void runPrecommitValidationForClustering(HoodieWriteMetadata<JavaRDD<WriteStatus>> writeMetadata, HoodieTable table, String instantTime) {
    HoodieWriteMetadata<HoodieData<WriteStatus>> recreatedWriteMetadata = new HoodieWriteMetadata<>();
    if (writeMetadata.getWriteStats().isPresent()) {
      recreatedWriteMetadata.setWriteStats(writeMetadata.getWriteStats().get());
    }
    recreatedWriteMetadata.setPartitionToReplaceFileIds(writeMetadata.getPartitionToReplaceFileIds());
    SparkValidatorUtils.runValidators(config, recreatedWriteMetadata, context, table, instantTime);
  }

  @Override
  protected HoodieTable<?, HoodieData<HoodieRecord<T>>, ?, HoodieData<WriteStatus>> createTable(HoodieWriteConfig config, StorageConfiguration<?> storageConf, boolean skipValidation) {
    return createTableAndValidate(config, (c, ctx, txn) -> HoodieSparkTable.create(c, ctx, txn), skipValidation);
  }

  @Override
  protected void releaseResources(String instantTime) {
    SparkReleaseResources.releaseCachedData(context, config, basePath, instantTime);
  }
}
