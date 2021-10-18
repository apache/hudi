/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.client;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.callback.HoodieWriteCommitCallback;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.client.utils.TransactionUtils;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.metadata.SparkHoodieBackedTableMetadataWriter;
import org.apache.hudi.metrics.HoodieMetrics;
import org.apache.hudi.table.HoodieBaseTable;
import org.apache.hudi.table.HoodieSparkV2Table;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.upgrade.SparkUpgradeDowngradeHelper;
import org.apache.hudi.table.upgrade.UpgradeDowngrade;

import com.codahale.metrics.Timer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.util.Map;

public class SparkDataFrameWriteClient extends AbstractHoodieClient implements CleanDelegate {
  protected static final String LOOKUP_STR = "lookup";
  private static final Logger LOG = LogManager.getLogger(SparkDataFrameWriteClient.class);

  protected final transient HoodieMetrics metrics;
  protected transient Timer.Context writeTimer = null;
  protected transient Timer.Context compactionTimer;
  protected transient Timer.Context clusteringTimer;

  private transient WriteOperationType operationType;
  private transient HoodieWriteCommitCallback commitCallback;
  protected transient AsyncCleanerService asyncCleanerService;
  protected final TransactionManager txnManager;
  protected Option<Pair<HoodieInstant, Map<String, String>>> lastCompletedTxnAndMetadata = Option.empty();

  public SparkDataFrameWriteClient(HoodieEngineContext context, HoodieWriteConfig clientConfig) {
    this(context, clientConfig, Option.empty());
  }

  public SparkDataFrameWriteClient(HoodieEngineContext context, HoodieWriteConfig clientConfig,
      Option<EmbeddedTimelineService> timelineServer) {
    super(context, clientConfig, timelineServer);
    this.metrics = new HoodieMetrics(config);
    this.txnManager = new TransactionManager(config, fs);

    if (config.isMetadataTableEnabled()) {
      // If the metadata table does not exist, it should be bootstrapped here
      // TODO: Check if we can remove this requirement - auto bootstrap on commit
      SparkHoodieBackedTableMetadataWriter.create(context.getHadoopConf().get(), config, context);
    }
  }

  public static SparkConf registerClasses(SparkConf conf) {
    conf.registerKryoClasses(new Class[]{HoodieWriteConfig.class, HoodieRecord.class, HoodieKey.class});
    return conf;
  }

  public Dataset<HoodieRowWriteStatus> upsert(Dataset<Row> inputDf, String instantTime) {
    HoodieBaseTable table = getTableAndInitCtx(WriteOperationType.UPSERT, instantTime);
    table.validateUpsertSchema();
    preWrite(instantTime, WriteOperationType.UPSERT, table.getMetaClient());
    HoodieWriteMetadata<Dataset<HoodieRowWriteStatus>> result = table.upsert(context, instantTime, inputDf);
    if (result.getIndexLookupDuration().isPresent()) {
      metrics.updateIndexMetrics(LOOKUP_STR, result.getIndexLookupDuration().get().toMillis());
    }
    return postWrite(result, instantTime, table);
  }

  protected Dataset<HoodieRowWriteStatus> postWrite(HoodieWriteMetadata<Dataset<HoodieRowWriteStatus>> result,
      String instantTime, HoodieBaseTable table) {
    if (result.getIndexUpdateDuration().isPresent()) {
      metrics.updateIndexMetrics(operationType.name(), result.getIndexUpdateDuration().get().toMillis());
    }
    if (result.isCommitted()) {
      // Perform post commit operations.
      // TODO(rxu) org.apache.hudi.client.SparkRDDWriteClient.postWrite
    }
    return result.getWriteStatuses();
  }

  /**
   * Common method containing steps to be performed before write (upsert/insert/...
   * @param instantTime
   * @param writeOperationType
   * @param metaClient
   */
  protected void preWrite(String instantTime, WriteOperationType writeOperationType,
      HoodieTableMetaClient metaClient) {
    this.operationType = writeOperationType;
    this.lastCompletedTxnAndMetadata = TransactionUtils.getLastCompletedTxnInstantAndMetadata(metaClient);
    this.asyncCleanerService = AsyncCleanerService.startAsyncCleaningIfEnabled(this);
  }

  protected HoodieBaseTable getTableAndInitCtx(WriteOperationType operationType, String instantTime) {
    HoodieTableMetaClient metaClient = createMetaClient(true);
    UpgradeDowngrade upgradeDowngrade = new UpgradeDowngrade(
        metaClient, config, context, SparkUpgradeDowngradeHelper.getInstance());
    if (upgradeDowngrade.needsUpgradeOrDowngrade(HoodieTableVersion.current())) {
      if (config.getWriteConcurrencyMode().supportsOptimisticConcurrencyControl()) {
        this.txnManager.beginTransaction();
        try {
          // Ensure no inflight commits by setting EAGER policy and explicitly cleaning all failed commits
          // this.rollbackFailedWrites(getInstantsToRollback(metaClient, HoodieFailedWritesCleaningPolicy.EAGER));
          // TODO(rxu) impl. rollback
          new UpgradeDowngrade(
              metaClient, config, context, SparkUpgradeDowngradeHelper.getInstance())
              .run(HoodieTableVersion.current(), instantTime);
        } finally {
          this.txnManager.endTransaction();
        }
      } else {
        upgradeDowngrade.run(HoodieTableVersion.current(), instantTime);
      }
    }
    metaClient.validateTableProperties(config.getProps(), operationType);
    return getTableAndInitCtx(metaClient, operationType, instantTime);
  }

  private HoodieBaseTable getTableAndInitCtx(
      HoodieTableMetaClient metaClient, WriteOperationType operationType, String instantTime) {
    if (operationType == WriteOperationType.DELETE) {
      setWriteSchemaForDeletes(metaClient);
    }
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieBaseTable table = HoodieSparkV2Table.create(config, (HoodieSparkEngineContext) context, metaClient, config.isMetadataTableEnabled());
    if (table.getMetaClient().getCommitActionType().equals(HoodieTimeline.COMMIT_ACTION)) {
      writeTimer = metrics.getCommitCtx();
    } else {
      writeTimer = metrics.getDeltaCommitCtx();
    }
    table.getHoodieView().sync();
    return table;
  }

  /**
   * Sets write schema from last instant since deletes may not have schema set in the config.
   */
  protected void setWriteSchemaForDeletes(HoodieTableMetaClient metaClient) {
    try {
      HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
      Option<HoodieInstant> lastInstant =
          activeTimeline.filterCompletedInstants().filter(s -> s.getAction().equals(metaClient.getCommitActionType())
                  || s.getAction().equals(HoodieActiveTimeline.REPLACE_COMMIT_ACTION))
              .lastInstant();
      if (lastInstant.isPresent()) {
        HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(
            activeTimeline.getInstantDetails(lastInstant.get()).get(), HoodieCommitMetadata.class);
        if (commitMetadata.getExtraMetadata().containsKey(HoodieCommitMetadata.SCHEMA_KEY)) {
          config.setSchema(commitMetadata.getExtraMetadata().get(HoodieCommitMetadata.SCHEMA_KEY));
        } else {
          throw new HoodieIOException("Latest commit does not have any schema in commit metadata");
        }
      } else {
        throw new HoodieIOException("Deletes issued without any prior commits");
      }
    } catch (IOException e) {
      throw new HoodieIOException("IOException thrown while reading last commit metadata", e);
    }
  }

  @Override
  public HoodieCleanMetadata clean() {
    return clean(HoodieActiveTimeline.createNewInstantTime());
  }

  @Override
  public HoodieCleanMetadata clean(String cleanInstantTime) throws HoodieIOException {
    return clean(cleanInstantTime, true);
  }

  @Override
  public HoodieCleanMetadata clean(String cleanInstantTime, boolean scheduleInline) throws HoodieIOException {
    // TODO(rxu) impl. clean
    return null;
  }

  @Override
  public boolean isAutoClean() {
    return getConfig().isAutoClean();
  }

  @Override
  public boolean isAsyncClean() {
    return getConfig().isAsyncClean();
  }
}
