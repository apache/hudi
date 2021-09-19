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

package org.apache.hudi.table.action.bootstrap;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.bootstrap.BootstrapWriteStatus;
import org.apache.hudi.client.utils.SparkValidatorUtils;
import org.apache.hudi.common.bootstrap.index.BootstrapIndex;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.BootstrapFileMapping;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieData;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.io.HoodieMergeHandle;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.SparkHoodieBackedTableMetadataWriter;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.commit.BaseCommitHelper;
import org.apache.hudi.table.action.commit.BaseMergeHelper;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.table.HoodieSparkTable.convertMetadata;
import static org.apache.hudi.table.action.commit.SparkCommitHelper.getRdd;

public class SparkBootstrapCommitHelper<T extends HoodieRecordPayload<T>> extends BaseCommitHelper<T> {

  private static final Logger LOG = LogManager.getLogger(SparkBootstrapCommitHelper.class);
  protected String bootstrapSchema = null;

  public SparkBootstrapCommitHelper(
      HoodieEngineContext context, HoodieWriteConfig config, HoodieTable table,
      Option<Map<String, String>> extraMetadata) {
    super(context, config, table, HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS,
        WriteOperationType.BOOTSTRAP, extraMetadata);
  }

  @Override
  public void init(HoodieWriteConfig config) {
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> execute(
      HoodieData<HoodieRecord<T>> inputRecords) {
    // NO_OP
    return null;
  }

  public void setBootstrapSchema(String bootstrapSchema) {
    this.bootstrapSchema = bootstrapSchema;
  }

  @Override
  public String getSchemaToStoreInCommit() {
    return bootstrapSchema;
  }

  @Override
  public void commit(
      Option<Map<String, String>> extraMetadata,
      HoodieWriteMetadata<HoodieData<WriteStatus>> result) {
    // Perform bootstrap index write and then commit. Make sure both record-key and bootstrap-index
    // is all done in a single job DAG.
    Map<String, List<Pair<BootstrapFileMapping, HoodieWriteStat>>> bootstrapSourceAndStats =
        getRdd(result.getWriteStatuses()).collect().stream()
            .map(w -> {
              BootstrapWriteStatus ws = (BootstrapWriteStatus) w;
              return Pair.of(ws.getBootstrapSourceFileMapping(), ws.getStat());
            }).collect(Collectors.groupingBy(w -> w.getKey().getPartitionPath()));
    HoodieTableMetaClient metaClient = table.getMetaClient();
    try (BootstrapIndex.IndexWriter indexWriter = BootstrapIndex.getBootstrapIndex(metaClient)
        .createWriter(metaClient.getTableConfig().getBootstrapBasePath().get())) {
      LOG.info("Starting to write bootstrap index for source " + config.getBootstrapSourceBasePath() + " in table "
          + config.getBasePath());
      indexWriter.begin();
      bootstrapSourceAndStats.forEach((key, value) -> indexWriter.appendNextPartition(key,
          value.stream().map(Pair::getKey).collect(Collectors.toList())));
      indexWriter.finish();
      LOG.info("Finished writing bootstrap index for source " + config.getBootstrapSourceBasePath() + " in table "
          + config.getBasePath());
    }

    commit(extraMetadata, result, bootstrapSourceAndStats.values().stream()
        .flatMap(f -> f.stream().map(Pair::getValue)).collect(Collectors.toList()));
    LOG.info("Committing metadata bootstrap !!");
  }

  protected void commit(
      Option<Map<String, String>> extraMetadata,
      HoodieWriteMetadata<HoodieData<WriteStatus>> result, List<HoodieWriteStat> stats) {
    String actionType = table.getMetaClient().getCommitActionType();
    LOG.info("Committing " + instantTime + ", action Type " + actionType);
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieSparkTable table = HoodieSparkTable.create(config, context);

    HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();

    result.setCommitted(true);
    stats.forEach(stat -> metadata.addWriteStat(stat.getPartitionPath(), stat));
    result.setWriteStats(stats);

    // Finalize write
    finalizeWrite(instantTime, stats, result);
    syncTableMetadata();
    // add in extra metadata
    if (extraMetadata.isPresent()) {
      extraMetadata.get().forEach(metadata::addMetadata);
    }
    metadata.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, getSchemaToStoreInCommit());
    metadata.setOperationType(operationType);

    try {
      activeTimeline.saveAsComplete(new HoodieInstant(true, actionType, instantTime),
          Option.of(metadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
      LOG.info("Committed " + instantTime);
    } catch (IOException e) {
      throw new HoodieCommitException("Failed to complete commit " + config.getBasePath() + " at time " + instantTime,
          e);
    }
    result.setCommitMetadata(Option.of(metadata));
  }

  @Override
  public Iterator<List<WriteStatus>> handleInsert(
      String idPfx, Iterator<HoodieRecord<T>> recordItr) throws Exception {
    throw new UnsupportedOperationException("Should not called in bootstrap code path");
  }

  @Override
  public Iterator<List<WriteStatus>> handleUpdate(
      String partitionPath, String fileId, Iterator<HoodieRecord<T>> recordItr) throws IOException {
    throw new UnsupportedOperationException("Should not called in bootstrap code path");
  }

  @Override
  protected void syncTableMetadata() {
    // Open up the metadata table again, for syncing
    try (HoodieTableMetadataWriter writer =
             SparkHoodieBackedTableMetadataWriter.create(context.getHadoopConf().get(), config, context)) {
      LOG.info("Successfully synced to metadata table");
    } catch (Exception e) {
      throw new HoodieMetadataException("Error syncing to metadata table.", e);
    }
  }

  @Override
  protected HoodieMergeHandle getUpdateHandle(String partitionPath, String fileId, Iterator<HoodieRecord<T>> recordItr) {
    throw new UnsupportedOperationException("Should not called in bootstrap code path");
  }

  @Override
  protected BaseMergeHelper getMergeHelper() {
    throw new UnsupportedOperationException("Should not called in bootstrap code path");
  }

  @Override
  protected void runPrecommitValidators(HoodieWriteMetadata<HoodieData<WriteStatus>> writeMetadata) {
    SparkValidatorUtils.runValidators(config, convertMetadata(writeMetadata), context, table, instantTime);
  }
}
