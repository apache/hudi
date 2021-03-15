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
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.CompactionUtils;

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
      case HoodieTimeline.COMMIT_ACTION:
      case HoodieTimeline.DELTA_COMMIT_ACTION: {
        HoodieCommitMetadata commitMetadata = HoodieCommitMetadata
                .fromBytes(metaClient.getActiveTimeline().getInstantDetails(hoodieInstant).get(), HoodieCommitMetadata.class);
        archivedMetaWrapper.setHoodieCommitMetadata(convertCommitMetadata(commitMetadata));
        archivedMetaWrapper.setActionType(ActionType.commit.name());
        break;
      }
      case HoodieTimeline.REPLACE_COMMIT_ACTION: {
        if (hoodieInstant.isCompleted()) {
          HoodieReplaceCommitMetadata replaceCommitMetadata = HoodieReplaceCommitMetadata
              .fromBytes(metaClient.getActiveTimeline().getInstantDetails(hoodieInstant).get(), HoodieReplaceCommitMetadata.class);
          archivedMetaWrapper.setHoodieReplaceCommitMetadata(ReplaceArchivalHelper.convertReplaceCommitMetadata(replaceCommitMetadata));
        } else {
          HoodieRequestedReplaceMetadata requestedReplaceMetadata =
              ClusteringUtils.getRequestedReplaceMetadata(metaClient, hoodieInstant).get();
          archivedMetaWrapper.setHoodieRequestedReplaceMetadata(requestedReplaceMetadata);
        }
        archivedMetaWrapper.setActionType(ActionType.replacecommit.name());
        break;
      }
      case HoodieTimeline.ROLLBACK_ACTION: {
        archivedMetaWrapper.setHoodieRollbackMetadata(TimelineMetadataUtils.deserializeAvroMetadata(
                metaClient.getActiveTimeline().getInstantDetails(hoodieInstant).get(), HoodieRollbackMetadata.class));
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
      default: {
        throw new UnsupportedOperationException("Action not fully supported yet");
      }
    }
    return archivedMetaWrapper;
  }

  public static HoodieArchivedMetaEntry createMetaWrapper(HoodieInstant hoodieInstant,
                                                          HoodieCommitMetadata hoodieCommitMetadata) {
    HoodieArchivedMetaEntry archivedMetaWrapper = new HoodieArchivedMetaEntry();
    archivedMetaWrapper.setCommitTime(hoodieInstant.getTimestamp());
    archivedMetaWrapper.setActionState(hoodieInstant.getState().name());
    archivedMetaWrapper.setHoodieCommitMetadata(convertCommitMetadata(hoodieCommitMetadata));
    archivedMetaWrapper.setActionType(ActionType.commit.name());
    return archivedMetaWrapper;
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