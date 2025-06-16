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

import org.apache.hudi.callback.common.WriteStatusValidator;
import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.JavaHoodieIndexFactory;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.JavaHoodieBackedTableMetadataWriter;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieJavaTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.upgrade.JavaUpgradeDowngradeHelper;

import com.codahale.metrics.Timer;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class HoodieJavaWriteClient<T> extends
    BaseHoodieWriteClient<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> {

  public HoodieJavaWriteClient(HoodieEngineContext context, HoodieWriteConfig writeConfig) {
    super(context, writeConfig, JavaUpgradeDowngradeHelper.getInstance());
    this.tableServiceClient = new HoodieJavaTableServiceClient<>(context, writeConfig, getTimelineServer());
  }

  @Override
  protected void updateColumnsToIndexWithColStats(HoodieTableMetaClient metaClient, List<String> columnsToIndex) {
    // no op
  }

  public HoodieJavaWriteClient(HoodieEngineContext context,
                               HoodieWriteConfig writeConfig,
                               boolean rollbackPending,
                               Option<EmbeddedTimelineService> timelineService) {
    super(context, writeConfig, timelineService, JavaUpgradeDowngradeHelper.getInstance());
    this.tableServiceClient = new HoodieJavaTableServiceClient<>(context, writeConfig, getTimelineServer());
  }

  @Override
  public List<HoodieRecord<T>> filterExists(List<HoodieRecord<T>> hoodieRecords) {
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieJavaTable<T> table = HoodieJavaTable.create(config, context);
    Timer.Context indexTimer = metrics.getIndexCtx();
    List<HoodieRecord<T>> recordsWithLocation = getIndex().tagLocation(HoodieListData.eager(hoodieRecords), context, table).collectAsList();
    metrics.updateIndexMetrics(LOOKUP_STR, metrics.getDurationInMs(indexTimer == null ? 0L : indexTimer.stop()));
    return recordsWithLocation.stream().filter(v1 -> !v1.isCurrentLocationKnown()).collect(Collectors.toList());
  }

  @Override
  protected HoodieIndex createIndex(HoodieWriteConfig writeConfig) {
    return JavaHoodieIndexFactory.createIndex(config);
  }

  @Override
  public boolean commit(String instantTime,
                        List<WriteStatus> writeStatuses,
                        Option<Map<String, String>> extraMetadata,
                        String commitActionType,
                        Map<String, List<String>> partitionToReplacedFileIds,
                        Option<BiConsumer<HoodieTableMetaClient, HoodieCommitMetadata>> extraPreCommitFunc,
                        Option<WriteStatusValidator> writeStatusValidatorOpt) {
    List<HoodieWriteStat> writeStats = writeStatuses.stream().map(WriteStatus::getStat).collect(Collectors.toList());
    return commitStats(instantTime, writeStats, extraMetadata, commitActionType, partitionToReplacedFileIds, extraPreCommitFunc);
  }

  @Override
  protected HoodieTable createTable(HoodieWriteConfig config) {
    return createTableAndValidate(config, HoodieJavaTable::create);
  }

  @Override
  protected HoodieTable createTable(HoodieWriteConfig config, HoodieTableMetaClient metaClient) {
    return createTableAndValidate(config, metaClient, HoodieJavaTable::create);
  }

  @Override
  public List<WriteStatus> upsert(List<HoodieRecord<T>> records,
                                  String instantTime) {
    HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table =
        initTable(WriteOperationType.UPSERT, Option.ofNullable(instantTime));
    table.validateUpsertSchema();
    preWrite(instantTime, WriteOperationType.UPSERT, table.getMetaClient());
    HoodieWriteMetadata<List<WriteStatus>> result = table.upsert(context, instantTime, records);
    if (result.getIndexLookupDuration().isPresent()) {
      metrics.updateIndexMetrics(LOOKUP_STR, result.getIndexLookupDuration().get().toMillis());
    }
    return postWrite(result, instantTime, table);
  }

  @Override
  public List<WriteStatus> upsertPreppedRecords(List<HoodieRecord<T>> preppedRecords,
                                                String instantTime) {
    HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table =
        initTable(WriteOperationType.UPSERT_PREPPED, Option.ofNullable(instantTime));
    table.validateUpsertSchema();
    preWrite(instantTime, WriteOperationType.UPSERT_PREPPED, table.getMetaClient());
    HoodieWriteMetadata<List<WriteStatus>> result = table.upsertPrepped(context,instantTime, preppedRecords);
    return postWrite(result, instantTime, table);
  }

  @Override
  public List<WriteStatus> insert(List<HoodieRecord<T>> records, String instantTime) {
    HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table =
        initTable(WriteOperationType.INSERT, Option.ofNullable(instantTime));
    table.validateUpsertSchema();
    preWrite(instantTime, WriteOperationType.INSERT, table.getMetaClient());
    HoodieWriteMetadata<List<WriteStatus>> result = table.insert(context, instantTime, records);
    if (result.getIndexLookupDuration().isPresent()) {
      metrics.updateIndexMetrics(LOOKUP_STR, result.getIndexLookupDuration().get().toMillis());
    }
    return postWrite(result, instantTime, table);
  }

  @Override
  public List<WriteStatus> insertPreppedRecords(List<HoodieRecord<T>> preppedRecords,
                                                String instantTime) {
    HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table =
        initTable(WriteOperationType.INSERT_PREPPED, Option.ofNullable(instantTime));
    table.validateInsertSchema();
    preWrite(instantTime, WriteOperationType.INSERT_PREPPED, table.getMetaClient());
    HoodieWriteMetadata<List<WriteStatus>> result = table.insertPrepped(context,instantTime, preppedRecords);
    return postWrite(result, instantTime, table);
  }

  @Override
  public List<WriteStatus> bulkInsert(List<HoodieRecord<T>> records,
                                      String instantTime) {
    return bulkInsert(records, instantTime, Option.empty());
  }

  @Override
  public List<WriteStatus> bulkInsert(List<HoodieRecord<T>> records,
                                      String instantTime,
                                      Option<BulkInsertPartitioner> userDefinedBulkInsertPartitioner) {
    HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table =
        initTable(WriteOperationType.BULK_INSERT, Option.ofNullable(instantTime));
    table.validateInsertSchema();
    preWrite(instantTime, WriteOperationType.BULK_INSERT, table.getMetaClient());
    HoodieWriteMetadata<List<WriteStatus>> result = table.bulkInsert(context, instantTime, records, userDefinedBulkInsertPartitioner);
    return postWrite(result, instantTime, table);
  }

  public void transitionInflight(String instantTime) {
    HoodieTableMetaClient metaClient = createMetaClient(true);
    metaClient.getActiveTimeline().transitionRequestedToInflight(
        metaClient.createNewInstant(HoodieInstant.State.REQUESTED, metaClient.getCommitActionType(), instantTime),
        Option.empty(), config.shouldAllowMultiWriteOnSameInstant());
  }

  @Override
  public List<WriteStatus> bulkInsertPreppedRecords(List<HoodieRecord<T>> preppedRecords,
                                                    String instantTime,
                                                    Option<BulkInsertPartitioner> bulkInsertPartitioner) {
    HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table =
        initTable(WriteOperationType.BULK_INSERT_PREPPED, Option.ofNullable(instantTime));
    table.validateInsertSchema();
    preWrite(instantTime, WriteOperationType.BULK_INSERT_PREPPED, table.getMetaClient());
    HoodieWriteMetadata<List<WriteStatus>> result = table.bulkInsertPrepped(context, instantTime, preppedRecords, bulkInsertPartitioner);
    return postWrite(result, instantTime, table);
  }

  @Override
  public List<WriteStatus> delete(List<HoodieKey> keys,
                                  String instantTime) {
    HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table =
        initTable(WriteOperationType.DELETE, Option.ofNullable(instantTime));
    preWrite(instantTime, WriteOperationType.DELETE, table.getMetaClient());
    HoodieWriteMetadata<List<WriteStatus>> result = table.delete(context,instantTime, keys);
    return postWrite(result, instantTime, table);
  }

  @Override
  public List<WriteStatus> deletePrepped(List<HoodieRecord<T>> preppedRecords, final String instantTime) {
    HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table =
        initTable(WriteOperationType.DELETE_PREPPED, Option.ofNullable(instantTime));
    preWrite(instantTime, WriteOperationType.DELETE_PREPPED, table.getMetaClient());
    HoodieWriteMetadata<List<WriteStatus>> result = table.deletePrepped(context,instantTime, preppedRecords);
    return postWrite(result, instantTime, table);
  }

  @Override
  protected void initMetadataTable(Option<String> instantTime, HoodieTableMetaClient metaClient) {
    // Initialize Metadata Table to make sure it's bootstrapped _before_ the operation,
    // if it didn't exist before
    // See https://issues.apache.org/jira/browse/HUDI-3343 for more details
    initializeMetadataTable(instantTime);
  }

  /**
   * Initialize the metadata table if needed. Creating the metadata table writer
   * will trigger the initial bootstrapping from the data table.
   *
   * @param inFlightInstantTimestamp - The in-flight action responsible for the metadata table initialization
   */
  private void initializeMetadataTable(Option<String> inFlightInstantTimestamp) {
    if (!config.isMetadataTableEnabled()) {
      return;
    }

    try (HoodieTableMetadataWriter writer = JavaHoodieBackedTableMetadataWriter.create(
        context.getStorageConf(), config, context, inFlightInstantTimestamp)) {
      if (writer.isInitialized()) {
        writer.performTableServices(inFlightInstantTimestamp);
      }
    } catch (Exception e) {
      throw new HoodieException("Failed to instantiate Metadata table ", e);
    }
  }
}
