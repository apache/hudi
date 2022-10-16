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
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieCompactionException;
import org.apache.hudi.table.HoodieTable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Base class helps to perform compact.
 *
 * @param <T> Type of payload in {@link org.apache.hudi.common.model.HoodieRecord}
 * @param <I> Type of inputs
 * @param <K> Type of keys
 * @param <O> Type of outputs
 */
public class CompactHelpers<T extends HoodieRecordPayload, I, K, O> {

  private static final CompactHelpers SINGLETON_INSTANCE = new CompactHelpers();

  private CompactHelpers() {
  }

  public static CompactHelpers getInstance() {
    return SINGLETON_INSTANCE;
  }

  public HoodieCommitMetadata createCompactionMetadata(
      HoodieTable table, String compactionInstantTime, HoodieData<WriteStatus> writeStatuses,
      String schema) throws IOException {
    byte[] planBytes = table.getActiveTimeline().readCompactionPlanAsBytes(
        HoodieTimeline.getCompactionRequestedInstant(compactionInstantTime)).get();
    HoodieCompactionPlan compactionPlan = TimelineMetadataUtils.deserializeCompactionPlan(planBytes);
    List<HoodieWriteStat> updateStatusMap = writeStatuses.map(WriteStatus::getStat).collectAsList();
    HoodieCommitMetadata metadata = new HoodieCommitMetadata(true);
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
          HoodieTimeline.getCompactionInflightInstant(compactionCommitTime),
          Option.of(commitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
    } catch (IOException e) {
      throw new HoodieCompactionException(
          "Failed to commit " + table.getMetaClient().getBasePath() + " at time " + compactionCommitTime, e);
    }
  }

  public void completeInflightLogCompaction(HoodieTable table, String logCompactionCommitTime, HoodieCommitMetadata commitMetadata) {
    HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
    try {
      activeTimeline.transitionLogCompactionInflightToComplete(
          HoodieTimeline.getLogCompactionInflightInstant(logCompactionCommitTime),
          Option.of(commitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
    } catch (IOException e) {
      throw new HoodieCompactionException(
          "Failed to commit " + table.getMetaClient().getBasePath() + " at time " + logCompactionCommitTime, e);
    }
  }
}
