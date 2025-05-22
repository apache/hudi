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
import org.apache.hudi.client.utils.SparkReleaseResources;
import org.apache.hudi.client.utils.SparkValidatorUtils;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.spark.api.java.JavaRDD;

import java.util.List;

public class SparkRDDTableServiceClient<T> extends BaseHoodieTableServiceClient<HoodieData<HoodieRecord<T>>, HoodieData<WriteStatus>, JavaRDD<WriteStatus>> {
  protected SparkRDDTableServiceClient(HoodieEngineContext context,
                                       HoodieWriteConfig clientConfig,
                                       Option<EmbeddedTimelineService> timelineService) {
    super(context, clientConfig, timelineService);
  }

  @Override
  protected List<HoodieWriteStat> triggerWritesAndFetchWriteStats(HoodieWriteMetadata<JavaRDD<WriteStatus>> writeMetadata) {
    return writeMetadata.getWriteStatuses().map(writeStatus -> writeStatus.getStat()).collect();
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
    return createTableAndValidate(config, HoodieSparkTable::create, skipValidation);
  }

  @Override
  protected void releaseResources(String instantTime) {
    SparkReleaseResources.releaseCachedData(context, config, basePath, instantTime);
  }
}
