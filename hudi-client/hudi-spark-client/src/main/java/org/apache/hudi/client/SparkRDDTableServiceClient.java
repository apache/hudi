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

import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.client.utils.SparkReleaseResources;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Functions;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.spark.api.java.JavaRDD;

import java.util.List;
import java.util.stream.Collectors;

public class SparkRDDTableServiceClient<T> extends BaseHoodieTableServiceClient<HoodieData<HoodieRecord<T>>, HoodieData<WriteStatus>, JavaRDD<WriteStatus>> {

  protected SparkRDDTableServiceClient(HoodieEngineContext context,
                                       HoodieWriteConfig clientConfig,
                                       Option<EmbeddedTimelineService> timelineService,
                                       Functions.Function2<String, HoodieTableMetaClient, Option<HoodieTableMetadataWriter>> getMetadataWriterFunc,
                                       Functions.Function1<String, Void> cleanUpMetadataWriterInstance) {
    super(context, clientConfig, timelineService, getMetadataWriterFunc, cleanUpMetadataWriterInstance);
  }

  @Override
  protected void validateClusteringCommit(HoodieWriteMetadata<JavaRDD<WriteStatus>> clusteringMetadata, String clusteringCommitTime, HoodieTable table) {
    if (clusteringMetadata.getDataTableWriteStatuses().isEmpty()) {
      HoodieClusteringPlan clusteringPlan = ClusteringUtils.getClusteringPlan(
              table.getMetaClient(), ClusteringUtils.getInflightClusteringInstant(clusteringCommitTime, table.getActiveTimeline()).get())
          .map(Pair::getRight).orElseThrow(() -> new HoodieClusteringException(
              "Unable to read clustering plan for instant: " + clusteringCommitTime));
      throw new HoodieClusteringException("Clustering plan produced 0 WriteStatus for " + clusteringCommitTime
          + " #groups: " + clusteringPlan.getInputGroups().size() + " expected at least "
          + clusteringPlan.getInputGroups().stream().mapToInt(HoodieClusteringGroup::getNumOutputFileGroups).sum()
          + " write statuses");
    }
  }

  @Override
  protected HoodieWriteMetadata<JavaRDD<WriteStatus>> convertToOutputMetadata(HoodieWriteMetadata<HoodieData<WriteStatus>> writeMetadata) {
    return writeMetadata.clone(HoodieJavaRDD.getJavaRDD(writeMetadata.getAllWriteStatuses()));
  }

  @Override
  protected HoodieWriteMetadata<HoodieData<WriteStatus>> writeToMetadata(HoodieWriteMetadata<HoodieData<WriteStatus>> writeMetadata, String compactionInstantTime,
                                                                         Option<HoodieTableMetadataWriter> metadataWriterOpt) {
    if (metadataWriterOpt.isPresent()) { // write to metadata table if enabled
      HoodieData<WriteStatus> mdtWriteStatuses = metadataWriterOpt.get().prepareAndWriteToMDT((HoodieData<WriteStatus>) writeMetadata.getDataTableWriteStatuses(), compactionInstantTime);
      writeMetadata.setAllWriteStatuses(((HoodieData<WriteStatus>) writeMetadata.getDataTableWriteStatuses()).union(mdtWriteStatuses));
    } else {
      writeMetadata.setAllWriteStatuses(writeMetadata.getDataTableWriteStatuses());
    }
    return writeMetadata;
  }

  @Override
  protected Pair<List<HoodieWriteStat>, List<HoodieWriteStat>> processAndFetchHoodieWriteStats(HoodieWriteMetadata<JavaRDD<WriteStatus>> compactionWriteMetadata) {
      List<Pair<Boolean, HoodieWriteStat>> writeStats = compactionWriteMetadata.getAllWriteStatuses().map(writeStatus ->
          Pair.of(writeStatus.isMetadataTable(), writeStatus.getStat())).collect();
      List<HoodieWriteStat> dataTableWriteStats = writeStats.stream().filter(entry -> !entry.getKey()).map(Pair::getValue).collect(Collectors.toList());
      List<HoodieWriteStat> mdtWriteStats = writeStats.stream().filter(Pair::getKey).map(Pair::getValue).collect(Collectors.toList());
      if (HoodieTableMetadata.isMetadataTable(config.getBasePath())) {
        dataTableWriteStats.clear();
        dataTableWriteStats.addAll(mdtWriteStats);
        mdtWriteStats.clear();
      }
      return Pair.of(dataTableWriteStats, mdtWriteStats);
  }

  @Override
  protected HoodieData<WriteStatus> convertToWriteStatus(HoodieWriteMetadata<HoodieData<WriteStatus>> writeMetadata) {
    return writeMetadata.getDataTableWriteStatuses();
  }

  @Override
  protected HoodieTable<?, HoodieData<HoodieRecord<T>>, ?, HoodieData<WriteStatus>> createTable(HoodieWriteConfig config, StorageConfiguration<?> storageConf, boolean skipValidation) {
    return createTableAndValidate(config, HoodieSparkTable::create, skipValidation);
  }

  @Override
  protected void releaseResources(String instantTime) {
    SparkReleaseResources.releaseCachedData(context, config, basePath, instantTime);
  }

  @Override
  Option<HoodieTableMetadataWriter> getMetadataWriter(String triggeringInstantTimestamp, HoodieTableMetaClient metaClient) {
    return getMetadataWriterFunc.apply(triggeringInstantTimestamp, metaClient);
  }
}
