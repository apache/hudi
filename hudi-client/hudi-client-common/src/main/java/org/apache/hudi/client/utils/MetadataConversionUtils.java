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

package org.apache.hudi.client.utils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.apache.hudi.avro.model.HoodieArchivedMetaEntry;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.client.ReplaceArchivalHelper;
import org.apache.hudi.common.model.ActionType;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieRollingStatMetadata;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;

/**
 * Helper class to convert between different action related payloads and {@link HoodieArchivedMetaEntry}.
 */
public class MetadataConversionUtils {

  public static HoodieArchivedMetaEntry createMetaWrapper(HoodieInstant hoodieInstant, HoodieTableMetaClient metaClient) throws IOException {
    HoodieArchivedMetaEntry archivedMetaWrapper = new HoodieArchivedMetaEntry();
    archivedMetaWrapper.setCommitTime(hoodieInstant.getTimestamp());
    archivedMetaWrapper.setActionState(hoodieInstant.getState().name());
    switch (hoodieInstant.getAction()) {
      case HoodieTimeline.CLEAN_ACTION: {
        if (hoodieInstant.isCompleted()) {
          archivedMetaWrapper.setHoodieCleanMetadata(CleanerUtils.getCleanerMetadata(metaClient, hoodieInstant));
        } else {
          archivedMetaWrapper.setHoodieCleanerPlan(CleanerUtils.getCleanerPlan(metaClient, hoodieInstant));
        }
        archivedMetaWrapper.setActionType(ActionType.clean.name());
        break;
      }
      case HoodieTimeline.COMMIT_ACTION: {
        HoodieCommitMetadata commitMetadata = HoodieCommitMetadata
                .fromBytes(metaClient.getActiveTimeline().getInstantDetails(hoodieInstant).get(), HoodieCommitMetadata.class);
        archivedMetaWrapper.setHoodieCommitMetadata(convertCommitMetadata(commitMetadata));
        archivedMetaWrapper.setActionType(ActionType.commit.name());
        break;
      }
      case HoodieTimeline.DELTA_COMMIT_ACTION: {
        HoodieCommitMetadata deltaCommitMetadata = HoodieCommitMetadata
                .fromBytes(metaClient.getActiveTimeline().getInstantDetails(hoodieInstant).get(), HoodieCommitMetadata.class);
        archivedMetaWrapper.setHoodieCommitMetadata(convertCommitMetadata(deltaCommitMetadata));
        archivedMetaWrapper.setActionType(ActionType.deltacommit.name());
        break;
      }
      case HoodieTimeline.REPLACE_COMMIT_ACTION: {
        if (hoodieInstant.isCompleted()) {
          HoodieReplaceCommitMetadata replaceCommitMetadata = HoodieReplaceCommitMetadata
              .fromBytes(metaClient.getActiveTimeline().getInstantDetails(hoodieInstant).get(), HoodieReplaceCommitMetadata.class);
          archivedMetaWrapper.setHoodieReplaceCommitMetadata(ReplaceArchivalHelper.convertReplaceCommitMetadata(replaceCommitMetadata));
        } else if (hoodieInstant.isInflight()) {
          // inflight replacecommit files have the same meta data body as HoodieCommitMetadata
          // so we could re-use it without further creating an inflight extension.
          // Or inflight replacecommit files are empty under clustering circumstance
          Option<HoodieCommitMetadata> inflightCommitMetadata = getInflightReplaceMetadata(metaClient, hoodieInstant);
          if (inflightCommitMetadata.isPresent()) {
            archivedMetaWrapper.setHoodieInflightReplaceMetadata(convertCommitMetadata(inflightCommitMetadata.get()));
          }
        } else {
          // we may have cases with empty HoodieRequestedReplaceMetadata e.g. insert_overwrite_table or insert_overwrite
          // without clustering. However, we should revisit the requested commit file standardization
          Option<HoodieRequestedReplaceMetadata> requestedReplaceMetadata = getRequestedReplaceMetadata(metaClient, hoodieInstant);
          if (requestedReplaceMetadata.isPresent()) {
            archivedMetaWrapper.setHoodieRequestedReplaceMetadata(requestedReplaceMetadata.get());
          }
        }
        archivedMetaWrapper.setActionType(ActionType.replacecommit.name());
        break;
      }
      case HoodieTimeline.ROLLBACK_ACTION: {
        if (hoodieInstant.isCompleted()) {
          archivedMetaWrapper.setHoodieRollbackMetadata(TimelineMetadataUtils.deserializeAvroMetadata(
                  metaClient.getActiveTimeline().getInstantDetails(hoodieInstant).get(), HoodieRollbackMetadata.class));
        }
        archivedMetaWrapper.setActionType(ActionType.rollback.name());
        break;
      }
      case HoodieTimeline.SAVEPOINT_ACTION: {
        archivedMetaWrapper.setHoodieSavePointMetadata(TimelineMetadataUtils.deserializeAvroMetadata(
                metaClient.getActiveTimeline().getInstantDetails(hoodieInstant).get(), HoodieSavepointMetadata.class));
        archivedMetaWrapper.setActionType(ActionType.savepoint.name());
        break;
      }
      case HoodieTimeline.COMPACTION_ACTION: {
        HoodieCompactionPlan plan = CompactionUtils.getCompactionPlan(metaClient, hoodieInstant.getTimestamp());
        archivedMetaWrapper.setHoodieCompactionPlan(plan);
        archivedMetaWrapper.setActionType(ActionType.compaction.name());
        break;
      }
      case HoodieTimeline.LOG_COMPACTION_ACTION: {
        HoodieCompactionPlan plan = CompactionUtils.getLogCompactionPlan(metaClient, hoodieInstant.getTimestamp());
        archivedMetaWrapper.setHoodieCompactionPlan(plan);
        archivedMetaWrapper.setActionType(ActionType.logcompaction.name());
        break;
      }
      default: {
        throw new UnsupportedOperationException("Action not fully supported yet");
      }
    }
    return archivedMetaWrapper;
  }

  public static HoodieArchivedMetaEntry createMetaWrapperForEmptyInstant(HoodieInstant hoodieInstant) throws IOException {
    HoodieArchivedMetaEntry archivedMetaWrapper = new HoodieArchivedMetaEntry();
    archivedMetaWrapper.setCommitTime(hoodieInstant.getTimestamp());
    archivedMetaWrapper.setActionState(hoodieInstant.getState().name());
    switch (hoodieInstant.getAction()) {
      case HoodieTimeline.CLEAN_ACTION: {
        archivedMetaWrapper.setActionType(ActionType.clean.name());
        break;
      }
      case HoodieTimeline.COMMIT_ACTION: {
        archivedMetaWrapper.setActionType(ActionType.commit.name());
        break;
      }
      case HoodieTimeline.DELTA_COMMIT_ACTION: {
        archivedMetaWrapper.setActionType(ActionType.deltacommit.name());
        break;
      }
      case HoodieTimeline.REPLACE_COMMIT_ACTION: {
        archivedMetaWrapper.setActionType(ActionType.replacecommit.name());
        break;
      }
      case HoodieTimeline.ROLLBACK_ACTION: {
        archivedMetaWrapper.setActionType(ActionType.rollback.name());
        break;
      }
      case HoodieTimeline.SAVEPOINT_ACTION: {
        archivedMetaWrapper.setActionType(ActionType.savepoint.name());
        break;
      }
      case HoodieTimeline.COMPACTION_ACTION: {
        archivedMetaWrapper.setActionType(ActionType.compaction.name());
        break;
      }
      default: {
        throw new UnsupportedOperationException("Action not fully supported yet");
      }
    }
    return archivedMetaWrapper;
  }

  public static Option<HoodieCommitMetadata> getInflightReplaceMetadata(HoodieTableMetaClient metaClient, HoodieInstant instant) throws IOException {
    Option<byte[]> inflightContent = metaClient.getActiveTimeline().getInstantDetails(instant);
    if (!inflightContent.isPresent() || inflightContent.get().length == 0) {
      // inflight files can be empty in some certain cases, e.g. when users opt in clustering
      return Option.empty();
    }
    return Option.of(HoodieCommitMetadata.fromBytes(inflightContent.get(), HoodieCommitMetadata.class));
  }

  private static Option<HoodieRequestedReplaceMetadata> getRequestedReplaceMetadata(HoodieTableMetaClient metaClient, HoodieInstant instant) throws IOException {
    Option<byte[]> requestedContent = metaClient.getActiveTimeline().getInstantDetails(instant);
    if (!requestedContent.isPresent() || requestedContent.get().length == 0) {
      // requested commit files can be empty in some certain cases, e.g. insert_overwrite or insert_overwrite_table.
      // However, it appears requested files are supposed to contain meta data and we should revisit the standardization
      // of requested commit files
      // TODO revisit requested commit file standardization https://issues.apache.org/jira/browse/HUDI-1739
      return Option.empty();
    }
    return Option.of(TimelineMetadataUtils.deserializeRequestedReplaceMetadata(requestedContent.get()));
  }

  public static Option<HoodieCommitMetadata> getHoodieCommitMetadata(HoodieTableMetaClient metaClient, HoodieInstant hoodieInstant) throws IOException {
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    HoodieTimeline timeline = activeTimeline.getCommitsTimeline().filterCompletedInstants();

    if (hoodieInstant.getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION)) {
      return Option.of(HoodieReplaceCommitMetadata.fromBytes(timeline.getInstantDetails(hoodieInstant).get(),
          HoodieReplaceCommitMetadata.class));
    }
    return Option.of(HoodieCommitMetadata.fromBytes(timeline.getInstantDetails(hoodieInstant).get(),
        HoodieCommitMetadata.class));

  }

  public static org.apache.hudi.avro.model.HoodieCommitMetadata convertCommitMetadata(
          HoodieCommitMetadata hoodieCommitMetadata) {
    ObjectMapper mapper = new ObjectMapper();
    // Need this to ignore other public get() methods
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    org.apache.hudi.avro.model.HoodieCommitMetadata avroMetaData =
            mapper.convertValue(hoodieCommitMetadata, org.apache.hudi.avro.model.HoodieCommitMetadata.class);
    if (hoodieCommitMetadata.getCompacted()) {
      avroMetaData.setOperationType(WriteOperationType.COMPACT.name());
    }
    // Do not archive Rolling Stats, cannot set to null since AVRO will throw null pointer
    avroMetaData.getExtraMetadata().put(HoodieRollingStatMetadata.ROLLING_STAT_METADATA_KEY, "");
    return avroMetaData;
  }
}
