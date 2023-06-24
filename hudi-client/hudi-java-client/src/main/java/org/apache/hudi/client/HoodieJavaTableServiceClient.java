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
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.table.HoodieJavaTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.hadoop.conf.Configuration;

import java.util.List;
import java.util.Map;

public class HoodieJavaTableServiceClient extends BaseHoodieTableServiceClient<List<WriteStatus>> {

  protected HoodieJavaTableServiceClient(HoodieEngineContext context,
                                         HoodieWriteConfig clientConfig,
                                         Option<EmbeddedTimelineService> timelineService) {
    super(context, clientConfig, timelineService);
  }

  @Override
  protected HoodieWriteMetadata<List<WriteStatus>> compact(String compactionInstantTime, boolean shouldComplete) {
    throw new HoodieNotSupportedException("Compact is not supported in HoodieJavaTableServiceClient");
  }

  @Override
  public void commitCompaction(String compactionInstantTime, HoodieCommitMetadata metadata, Option<Map<String, String>> extraMetadata) {
    throw new HoodieNotSupportedException("CommitCompaction is not supported in HoodieJavaTableServiceClient");
  }

  @Override
  protected void completeCompaction(HoodieCommitMetadata metadata, HoodieTable table, String compactionCommitTime) {
    throw new HoodieNotSupportedException("CompleteCompaction is not supported in HoodieJavaTableServiceClient");
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> cluster(String clusteringInstant, boolean shouldComplete) {
    throw new HoodieNotSupportedException("Cluster is not supported in HoodieJavaTableServiceClient");
  }

  @Override
  protected HoodieTable<?, ?, ?, ?> createTable(HoodieWriteConfig config, Configuration hadoopConf) {
    return HoodieJavaTable.create(config, context);
  }
}
