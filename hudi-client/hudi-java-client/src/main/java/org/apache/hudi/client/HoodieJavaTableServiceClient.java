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
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.util.Functions;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.table.HoodieJavaTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import java.util.List;
import java.util.stream.Collectors;

public class HoodieJavaTableServiceClient<T> extends BaseHoodieTableServiceClient<List<HoodieRecord<T>>, List<WriteStatus>, List<WriteStatus>> {

  protected HoodieJavaTableServiceClient(HoodieEngineContext context,
                                         HoodieWriteConfig clientConfig,
                                         Option<EmbeddedTimelineService> timelineService,
                                         Functions.Function2<String, HoodieTableMetaClient, Option<HoodieTableMetadataWriter>> getMetadataWriterFunc,
                                         Functions.Function1<String, Void> cleanUpMetadataWriterInstance) {
    super(context, clientConfig, timelineService, getMetadataWriterFunc, cleanUpMetadataWriterInstance);
  }

  @Override
  protected Pair<List<HoodieWriteStat>, List<HoodieWriteStat>> processAndFetchHoodieWriteStats(HoodieWriteMetadata<List<WriteStatus>> writeMetadata) {
    List<Pair<Boolean, HoodieWriteStat>> writeStats = writeMetadata.getDataTableWriteStatuses().stream().map(writeStatus ->
        Pair.of(writeStatus.isMetadataTable(), writeStatus.getStat())).collect(Collectors.toList());
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
  protected void updateColumnsToIndexWithColStats(HoodieTableMetaClient metaClient, List<String> columnsToIndex) {
    // no op
  }

  @Override
  protected HoodieWriteMetadata<List<WriteStatus>> convertToOutputMetadata(HoodieWriteMetadata<List<WriteStatus>> writeMetadata) {
    return writeMetadata;
  }

  @Override
  protected HoodieData<WriteStatus> convertToWriteStatus(HoodieWriteMetadata<List<WriteStatus>> writeMetadata) {
    return HoodieListData.eager(writeMetadata.getDataTableWriteStatuses());
  }

  @Override
  protected HoodieTable<?, List<HoodieRecord<T>>, ?, List<WriteStatus>> createTable(HoodieWriteConfig config, StorageConfiguration<?> storageConf, boolean skipValidation) {
    return createTableAndValidate(config, HoodieJavaTable::create, skipValidation);
  }

  @Override
  Option<HoodieTableMetadataWriter> getMetadataWriter(String triggeringInstantTimestamp, HoodieTableMetaClient metaClient) {
    return Option.empty();
  }
}
