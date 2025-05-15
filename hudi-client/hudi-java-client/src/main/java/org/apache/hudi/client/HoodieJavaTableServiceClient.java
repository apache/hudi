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
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.table.HoodieJavaTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import java.util.List;
import java.util.stream.Collectors;

public class HoodieJavaTableServiceClient<T> extends BaseHoodieTableServiceClient<List<HoodieRecord<T>>, List<WriteStatus>, List<WriteStatus>> {

  protected HoodieJavaTableServiceClient(HoodieEngineContext context,
                                         HoodieWriteConfig clientConfig,
                                         Option<EmbeddedTimelineService> timelineService) {
    super(context, clientConfig, timelineService);
  }

  @Override
  protected void updateColumnsToIndexWithColStats(HoodieTableMetaClient metaClient, List<String> columnsToIndex) {
    // no op
  }

  @Override
  protected List<HoodieWriteStat> triggerWritesAndFetchWriteStats(HoodieWriteMetadata<List<WriteStatus>> writeMetadata) {
    return writeMetadata.getWriteStatuses().stream().map(writeStatus -> writeStatus.getStat()).collect(Collectors.toList());
  }

  @Override
  protected HoodieWriteMetadata<List<WriteStatus>> convertToOutputMetadata(HoodieWriteMetadata<List<WriteStatus>> writeMetadata) {
    return writeMetadata;
  }

  @Override
  protected HoodieTable<?, List<HoodieRecord<T>>, ?, List<WriteStatus>> createTable(HoodieWriteConfig config, StorageConfiguration<?> storageConf, boolean skipValidation) {
    return createTableAndValidate(config, HoodieJavaTable::create, skipValidation);
  }
}
