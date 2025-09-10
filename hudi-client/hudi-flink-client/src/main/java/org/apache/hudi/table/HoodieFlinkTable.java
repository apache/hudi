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
import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.FlinkHoodieIndexFactory;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.utils.SerDeHelper;
import org.apache.hudi.metadata.FlinkHoodieBackedTableMetadataWriter;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import java.util.List;

/**
 * Impl of a flink hoodie table.
 */
public abstract class HoodieFlinkTable<T>
    extends HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>>
    implements ExplicitWriteHandleTable<T>, HoodieCompactionHandler<T> {

  protected HoodieFlinkTable(HoodieWriteConfig config, HoodieEngineContext context, HoodieTableMetaClient metaClient, Option<TransactionManager> txnManager) {
    super(config, context, metaClient, txnManager);
  }

  public static <T> HoodieFlinkTable<T> create(HoodieWriteConfig config, HoodieEngineContext context, TransactionManager txnManager) {
    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder()
            .setConf(context.getStorageConf().newInstance()).setBasePath(config.getBasePath())
            .setLoadActiveTimelineOnLoad(true).setConsistencyGuardConfig(config.getConsistencyGuardConfig())
            .setFileSystemRetryConfig(config.getFileSystemRetryConfig()).build();
    return HoodieFlinkTable.create(config, context, metaClient, Option.of(txnManager));
  }

  /**
   * Convenience method for read clients that don't need transaction management.
   */
  public static <T> HoodieFlinkTable<T> create(HoodieWriteConfig config, HoodieEngineContext context) {
    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder()
            .setConf(context.getStorageConf().newInstance()).setBasePath(config.getBasePath())
            .setLoadActiveTimelineOnLoad(true).setConsistencyGuardConfig(config.getConsistencyGuardConfig())
            .setFileSystemRetryConfig(config.getFileSystemRetryConfig()).build();
    return HoodieFlinkTable.create(config, context, metaClient, Option.empty());
  }

  public static <T> HoodieFlinkTable<T> create(HoodieWriteConfig config, HoodieEngineContext context, HoodieTableMetaClient metaClient) {
    return HoodieFlinkTable.create(config, context, metaClient, Option.empty());
  }

  public static <T> HoodieFlinkTable<T> create(HoodieWriteConfig config,
                                               HoodieEngineContext context,
                                               HoodieTableMetaClient metaClient,
                                               Option<TransactionManager> txnManager) {
    if (config.getSchemaEvolutionEnable()) {
      setLatestInternalSchema(config, metaClient);
    }
    final HoodieFlinkTable<T> hoodieFlinkTable;
    switch (metaClient.getTableType()) {
      case COPY_ON_WRITE:
        hoodieFlinkTable = new HoodieFlinkCopyOnWriteTable<>(config, context, metaClient, txnManager);
        break;
      case MERGE_ON_READ:
        hoodieFlinkTable = new HoodieFlinkMergeOnReadTable<>(config, context, metaClient, txnManager);
        break;
      default:
        throw new HoodieException("Unsupported table type :" + metaClient.getTableType());
    }
    return hoodieFlinkTable;
  }

  public static HoodieWriteMetadata<List<WriteStatus>> convertMetadata(
      HoodieWriteMetadata<HoodieData<WriteStatus>> metadata) {
    return metadata.clone(metadata.getWriteStatuses().collectAsList());
  }

  @Override
  protected HoodieIndex getIndex(HoodieWriteConfig config, HoodieEngineContext context) {
    return FlinkHoodieIndexFactory.createIndex((HoodieFlinkEngineContext) context, config);
  }

  /**
   * Fetch instance of {@link HoodieTableMetadataWriter}.
   *
   * @return instance of {@link HoodieTableMetadataWriter}
   */
  @Override
  protected Option<HoodieTableMetadataWriter> getMetadataWriter(
      String triggeringInstantTimestamp,
      HoodieFailedWritesCleaningPolicy failedWritesCleaningPolicy,
      boolean streamingWrites) {
    if (isMetadataTable()) {
      return Option.empty();
    }
    if (config.isMetadataTableEnabled() || getMetaClient().getTableConfig().isMetadataTableAvailable()) {
      return Option.of(FlinkHoodieBackedTableMetadataWriter.create(
          getContext().getStorageConf(), config, failedWritesCleaningPolicy, getContext(),
          Option.of(triggeringInstantTimestamp)));
    } else {
      return Option.empty();
    }
  }

  private static void setLatestInternalSchema(HoodieWriteConfig config, HoodieTableMetaClient metaClient) {
    Option<InternalSchema> internalSchema = new TableSchemaResolver(metaClient).getTableInternalSchemaFromCommitMetadata();
    if (internalSchema.isPresent()) {
      config.setInternalSchemaString(SerDeHelper.toJson(internalSchema.get()));
    }
  }
}
