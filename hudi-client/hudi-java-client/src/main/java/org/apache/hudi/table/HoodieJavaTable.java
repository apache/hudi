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

package org.apache.hudi.table;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.JavaHoodieIndexFactory;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.JavaHoodieBackedTableMetadataWriter;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;

public abstract class HoodieJavaTable<T>
    extends HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> {
  private volatile boolean isMetadataTableExists = false;

  protected HoodieJavaTable(HoodieWriteConfig config, HoodieEngineContext context, HoodieTableMetaClient metaClient) {
    super(config, context, metaClient);
  }

  public static <T> HoodieJavaTable<T> create(HoodieWriteConfig config, HoodieEngineContext context) {
    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder().setConf(context.getHadoopConf().get()).setBasePath(config.getBasePath())
            .setLoadActiveTimelineOnLoad(true).setConsistencyGuardConfig(config.getConsistencyGuardConfig())
            .setLayoutVersion(Option.of(new TimelineLayoutVersion(config.getTimelineLayoutVersion()))).build();
    return HoodieJavaTable.create(config, context, metaClient);
  }

  public static <T> HoodieJavaTable<T> create(HoodieWriteConfig config,
                                              HoodieEngineContext context,
                                              HoodieTableMetaClient metaClient) {
    switch (metaClient.getTableType()) {
      case COPY_ON_WRITE:
        return new HoodieJavaCopyOnWriteTable<>(config, context, metaClient);
      case MERGE_ON_READ:
        return new HoodieJavaMergeOnReadTable<>(config, context, metaClient);
      default:
        throw new HoodieException("Unsupported table type :" + metaClient.getTableType());
    }
  }

  public static HoodieWriteMetadata<List<WriteStatus>> convertMetadata(
      HoodieWriteMetadata<HoodieData<WriteStatus>> metadata) {
    return metadata.clone(metadata.getWriteStatuses().collectAsList());
  }

  @Override
  protected HoodieIndex getIndex(HoodieWriteConfig config, HoodieEngineContext context) {
    return JavaHoodieIndexFactory.createIndex(config);
  }

  @Override
  protected Option<HoodieTableMetadataWriter> getMetadataWriter(String triggeringInstantTimestamp, HoodieFailedWritesCleaningPolicy failedWritesCleaningPolicy) {
    if (config.isMetadataTableEnabled() || metaClient.getTableConfig().isMetadataTableAvailable()) {
      // Create the metadata table writer. First time after the upgrade this creation might trigger
      // metadata table bootstrapping. Bootstrapping process could fail and checking the table
      // existence after the creation is needed.
      final HoodieTableMetadataWriter metadataWriter = JavaHoodieBackedTableMetadataWriter.create(
          context.getHadoopConf().get(), config, failedWritesCleaningPolicy, context,
          Option.of(triggeringInstantTimestamp));
      // even with metadata enabled, some index could have been disabled
      // delete metadata partitions corresponding to such indexes
      deleteMetadataIndexIfNecessary();
      try {
        if (isMetadataTableExists || metaClient.getFs().exists(new Path(
            HoodieTableMetadata.getMetadataTableBasePath(metaClient.getBasePath())))) {
          isMetadataTableExists = true;
          return Option.of(metadataWriter);
        }
      } catch (IOException e) {
        throw new HoodieMetadataException("Checking existence of metadata table failed", e);
      }
    } else {
      maybeDeleteMetadataTable();
    }

    return Option.empty();
  }
}
