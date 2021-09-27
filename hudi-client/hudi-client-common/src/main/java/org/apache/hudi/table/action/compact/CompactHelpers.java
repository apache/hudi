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

package org.apache.hudi.table.action.compact;

import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.client.AbstractHoodieWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCompactionException;
import org.apache.hudi.table.HoodieCopyOnWriteTableOperation;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * Base class helps to perform compact.
 *
 * @param <T> Type of payload in {@link org.apache.hudi.common.model.HoodieRecord}
 */
public class CompactHelpers<T extends HoodieRecordPayload> {

  private static final Logger LOG = LoggerFactory.getLogger(CompactHelpers.class);

  private CompactHelpers() {
  }

  private static class CompactHelperHolder {
    private static final CompactHelpers COMPACT_HELPERS = new CompactHelpers();
  }

  public static CompactHelpers newInstance() {
    return CompactHelperHolder.COMPACT_HELPERS;
  }

  public HoodieWriteMetadata<HoodieData<WriteStatus>> compact(
      HoodieEngineContext context, HoodieTable table,
      HoodieCopyOnWriteTableOperation copyOnWriteTableOperation, HoodieWriteConfig config,
      String compactionInstantTime, AbstractHoodieWriteClient writeClient,
      TaskContextSupplier taskContextSupplier) {
    HoodieTimeline pendingCompactionTimeline = table.getActiveTimeline().filterPendingCompactionTimeline();
    HoodieInstant inflightInstant = HoodieTimeline.getCompactionInflightInstant(compactionInstantTime);
    if (pendingCompactionTimeline.containsInstant(inflightInstant)) {
      writeClient.rollbackInflightCompaction(inflightInstant, table);
      table.getMetaClient().reloadActiveTimeline();
    }

    HoodieWriteMetadata<HoodieData<WriteStatus>> compactionMetadata = new HoodieWriteMetadata<>();
    compactionMetadata.setWriteStatuses(context.createEmptyHoodieData());
    try {
      // generate compaction plan
      // should support configurable commit metadata
      HoodieCompactionPlan compactionPlan = CompactionUtils.getCompactionPlan(
          table.getMetaClient(), compactionInstantTime);

      if (compactionPlan == null || (compactionPlan.getOperations() == null)
          || (compactionPlan.getOperations().isEmpty())) {
        // do nothing.
        LOG.info("No compaction plan for instant " + compactionInstantTime);
        return compactionMetadata;
      }

      HoodieInstant instant = HoodieTimeline.getCompactionRequestedInstant(compactionInstantTime);
      // Mark instant as compaction inflight
      table.getActiveTimeline().transitionCompactionRequestedToInflight(instant);
      table.getMetaClient().reloadActiveTimeline();

      HoodieTableMetaClient metaClient = table.getMetaClient();
      TableSchemaResolver schemaUtil = new TableSchemaResolver(metaClient);

      // Here we firstly use the table schema as the reader schema to read
      // log file.That is because in the case of MergeInto, the config.getSchema may not
      // the same with the table schema.
      try {
        Schema readerSchema = schemaUtil.getTableAvroSchema(false);
        config.setSchema(readerSchema.toString());
      } catch (Exception e) {
        // If there is no commit in the table, just ignore the exception.
      }

      List<CompactionOperation> operations = compactionPlan.getOperations().stream()
          .map(CompactionOperation::convertFromAvroRecordInstance).collect(toList());
      LOG.info("Compacting " + operations + " files");

      context.setJobStatus(this.getClass().getSimpleName(), "Compacting file slices");

      HoodieData<WriteStatus> writeStatuses = context.parallelize(operations).map(operation -> new HoodieCompactor().compact(
          copyOnWriteTableOperation, metaClient, config, operation, compactionInstantTime, taskContextSupplier))
          .flatMap(List::iterator);

      writeStatuses.persist(config.getProps());
      context.setJobStatus(this.getClass().getSimpleName(), "Preparing compaction metadata");
      List<HoodieWriteStat> updateStatusMap = writeStatuses.map(WriteStatus::getStat).collectAsList();
      HoodieCommitMetadata metadata = new HoodieCommitMetadata(true);
      for (HoodieWriteStat stat : updateStatusMap) {
        metadata.addWriteStat(stat.getPartitionPath(), stat);
      }
      metadata.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, config.getSchema());

      compactionMetadata.setWriteStatuses(writeStatuses);
      compactionMetadata.setCommitted(false);
      compactionMetadata.setCommitMetadata(Option.of(metadata));
    } catch (IOException e) {
      throw new HoodieCompactionException("Could not compact " + config.getBasePath(), e);
    }
    return compactionMetadata;
  }

  public HoodieCommitMetadata createCompactionMetadata(HoodieTable table,
                                                       String compactionInstantTime,
                                                       HoodieData<WriteStatus> writeStatuses,
                                                       String schema) throws IOException {
    byte[] planBytes = table.getActiveTimeline().readCompactionPlanAsBytes(
        HoodieTimeline.getCompactionRequestedInstant(compactionInstantTime)).get();
    HoodieCompactionPlan compactionPlan = TimelineMetadataUtils.deserializeCompactionPlan(planBytes);
    List<HoodieWriteStat> updateStatusMap = writeStatuses.map(WriteStatus::getStat).collectAsList();
    org.apache.hudi.common.model.HoodieCommitMetadata metadata = new org.apache.hudi.common.model.HoodieCommitMetadata(true);
    for (HoodieWriteStat stat : updateStatusMap) {
      metadata.addWriteStat(stat.getPartitionPath(), stat);
    }
    metadata.addMetadata(org.apache.hudi.common.model.HoodieCommitMetadata.SCHEMA_KEY, schema);
    if (compactionPlan.getExtraMetadata() != null) {
      compactionPlan.getExtraMetadata().forEach(metadata::addMetadata);
    }
    return metadata;
  }

  public void completeInflightCompaction(HoodieTable table, String compactionCommitTime, HoodieCommitMetadata commitMetadata) {
    HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
    try {
      activeTimeline.transitionCompactionInflightToComplete(
          new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, compactionCommitTime),
          Option.of(commitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
    } catch (IOException e) {
      throw new HoodieCompactionException(
          "Failed to commit " + table.getMetaClient().getBasePath() + " at time " + compactionCommitTime, e);
    }
  }
}
