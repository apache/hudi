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

package org.apache.hudi.common.table.timeline;

import org.apache.hudi.avro.JsonEncoder;
import org.apache.hudi.avro.model.HoodieArchivedMetaEntry;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieLSMTimelineInstant;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.common.model.ActionType;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.JsonUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Helper class to convert between different action related payloads and {@link HoodieArchivedMetaEntry}.
 */
public class MetadataConversionUtils {

  public static HoodieArchivedMetaEntry createMetaWrapper(HoodieInstant hoodieInstant, HoodieTableMetaClient metaClient) throws IOException {
    Option<byte[]> instantDetails = metaClient.getActiveTimeline().getInstantDetails(hoodieInstant);
    if (hoodieInstant.isCompleted() && instantDetails.get().length == 0) {
      // in local FS and HDFS, there could be empty completed instants due to crash.
      // let's add an entry to the archival, even if not for the plan.
      return createMetaWrapperForEmptyInstant(hoodieInstant);
    }
    HoodieArchivedMetaEntry archivedMetaWrapper = new HoodieArchivedMetaEntry();
    archivedMetaWrapper.setCommitTime(hoodieInstant.getRequestTime());
    archivedMetaWrapper.setActionState(hoodieInstant.getState().name());
    archivedMetaWrapper.setStateTransitionTime(hoodieInstant.getCompletionTime());
    switch (hoodieInstant.getAction()) {
      case HoodieTimeline.CLEAN_ACTION: {
        if (hoodieInstant.isCompleted()) {
          archivedMetaWrapper.setHoodieCleanMetadata(CleanerUtils.getCleanerMetadata(metaClient, instantDetails.get()));
        } else {
          archivedMetaWrapper.setHoodieCleanerPlan(CleanerUtils.getCleanerPlan(metaClient, instantDetails.get()));
        }
        archivedMetaWrapper.setActionType(ActionType.clean.name());
        break;
      }
      case HoodieTimeline.COMMIT_ACTION: {
        HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(instantDetails.get(), HoodieCommitMetadata.class);
        archivedMetaWrapper.setHoodieCommitMetadata(convertCommitMetadata(commitMetadata));
        archivedMetaWrapper.setActionType(ActionType.commit.name());
        break;
      }
      case HoodieTimeline.DELTA_COMMIT_ACTION: {
        HoodieCommitMetadata deltaCommitMetadata = HoodieCommitMetadata.fromBytes(instantDetails.get(), HoodieCommitMetadata.class);
        archivedMetaWrapper.setHoodieCommitMetadata(convertCommitMetadata(deltaCommitMetadata));
        archivedMetaWrapper.setActionType(ActionType.deltacommit.name());
        break;
      }
      case HoodieTimeline.REPLACE_COMMIT_ACTION:
      case HoodieTimeline.CLUSTERING_ACTION: {
        if (hoodieInstant.isCompleted()) {
          HoodieReplaceCommitMetadata replaceCommitMetadata = HoodieReplaceCommitMetadata.fromBytes(instantDetails.get(), HoodieReplaceCommitMetadata.class);
          archivedMetaWrapper.setHoodieReplaceCommitMetadata(convertReplaceCommitMetadata(replaceCommitMetadata));
        } else if (hoodieInstant.isInflight()) {
          // inflight replacecommit files have the same metadata body as HoodieCommitMetadata
          // so we could re-use it without further creating an inflight extension.
          // Or inflight replacecommit files are empty under clustering circumstance
          Option<HoodieCommitMetadata> inflightCommitMetadata = getInflightCommitMetadata(instantDetails);
          if (inflightCommitMetadata.isPresent()) {
            archivedMetaWrapper.setHoodieInflightReplaceMetadata(convertCommitMetadata(inflightCommitMetadata.get()));
          }
        } else {
          // we may have cases with empty HoodieRequestedReplaceMetadata e.g. insert_overwrite_table or insert_overwrite
          // without clustering. However, we should revisit the requested commit file standardization
          Option<HoodieRequestedReplaceMetadata> requestedReplaceMetadata = getRequestedReplaceMetadata(instantDetails);
          if (requestedReplaceMetadata.isPresent()) {
            archivedMetaWrapper.setHoodieRequestedReplaceMetadata(requestedReplaceMetadata.get());
          }
        }
        archivedMetaWrapper.setActionType(
            hoodieInstant.getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION) ? ActionType.replacecommit.name() : ActionType.clustering.name());
        break;
      }
      case HoodieTimeline.ROLLBACK_ACTION: {
        if (hoodieInstant.isCompleted()) {
          archivedMetaWrapper.setHoodieRollbackMetadata(TimelineMetadataUtils.deserializeAvroMetadata(instantDetails.get(), HoodieRollbackMetadata.class));
        }
        archivedMetaWrapper.setActionType(ActionType.rollback.name());
        break;
      }
      case HoodieTimeline.SAVEPOINT_ACTION: {
        archivedMetaWrapper.setHoodieSavePointMetadata(TimelineMetadataUtils.deserializeAvroMetadata(instantDetails.get(), HoodieSavepointMetadata.class));
        archivedMetaWrapper.setActionType(ActionType.savepoint.name());
        break;
      }
      case HoodieTimeline.COMPACTION_ACTION: {
        if (hoodieInstant.isRequested()) {
          HoodieCompactionPlan plan = CompactionUtils.getCompactionPlan(metaClient, instantDetails);
          archivedMetaWrapper.setHoodieCompactionPlan(plan);
        }
        archivedMetaWrapper.setActionType(ActionType.compaction.name());
        break;
      }
      case HoodieTimeline.LOG_COMPACTION_ACTION: {
        if (hoodieInstant.isRequested()) {
          HoodieCompactionPlan plan = CompactionUtils.getCompactionPlan(metaClient, instantDetails);
          archivedMetaWrapper.setHoodieCompactionPlan(plan);
        }
        archivedMetaWrapper.setActionType(ActionType.logcompaction.name());
        break;
      }
      default: {
        throw new UnsupportedOperationException("Action not fully supported yet");
      }
    }
    return archivedMetaWrapper;
  }

  public static HoodieLSMTimelineInstant createLSMTimelineInstant(ActiveAction activeAction, HoodieTableMetaClient metaClient) {
    HoodieLSMTimelineInstant lsmTimelineInstant = new HoodieLSMTimelineInstant();
    lsmTimelineInstant.setInstantTime(activeAction.getInstantTime());
    lsmTimelineInstant.setCompletionTime(activeAction.getCompletionTime());
    lsmTimelineInstant.setAction(activeAction.getAction());
    activeAction.getCommitMetadata(metaClient).ifPresent(commitMetadata -> lsmTimelineInstant.setMetadata(ByteBuffer.wrap(commitMetadata)));
    lsmTimelineInstant.setVersion(LSMTimeline.LSM_TIMELINE_INSTANT_VERSION_1);
    switch (activeAction.getPendingAction()) {
      case HoodieTimeline.CLEAN_ACTION: {
        activeAction.getCleanPlan(metaClient).ifPresent(plan -> lsmTimelineInstant.setPlan(ByteBuffer.wrap(plan)));
        break;
      }
      case HoodieTimeline.REPLACE_COMMIT_ACTION:
      case HoodieTimeline.CLUSTERING_ACTION: {
        // we may have cases with empty HoodieRequestedReplaceMetadata e.g. insert_overwrite_table or insert_overwrite
        // without clustering. However, we should revisit the requested commit file standardization
        activeAction.getRequestedCommitMetadata(metaClient).ifPresent(metadata -> lsmTimelineInstant.setPlan(ByteBuffer.wrap(metadata)));
        // inflight replacecommit files have the same metadata body as HoodieCommitMetadata,
        // so we could re-use it without further creating an inflight extension.
        // Or inflight replacecommit files are empty under clustering circumstance.
        activeAction.getInflightCommitMetadata(metaClient).ifPresent(metadata -> lsmTimelineInstant.setPlan(ByteBuffer.wrap(metadata)));
        break;
      }
      case HoodieTimeline.COMPACTION_ACTION: {
        activeAction.getCompactionPlan(metaClient).ifPresent(plan -> lsmTimelineInstant.setPlan(ByteBuffer.wrap(plan)));
        break;
      }
      case HoodieTimeline.LOG_COMPACTION_ACTION: {
        activeAction.getLogCompactionPlan(metaClient).ifPresent(plan -> lsmTimelineInstant.setPlan(ByteBuffer.wrap(plan)));
        break;
      }
      default:
        // no operation
    }
    return lsmTimelineInstant;
  }

  public static HoodieArchivedMetaEntry createMetaWrapperForEmptyInstant(HoodieInstant hoodieInstant) {
    HoodieArchivedMetaEntry archivedMetaWrapper = new HoodieArchivedMetaEntry();
    archivedMetaWrapper.setCommitTime(hoodieInstant.getRequestTime());
    archivedMetaWrapper.setActionState(hoodieInstant.getState().name());
    archivedMetaWrapper.setStateTransitionTime(hoodieInstant.getCompletionTime());
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
      case HoodieTimeline.CLUSTERING_ACTION: {
        archivedMetaWrapper.setActionType(ActionType.clustering.name());
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

  private static Option<HoodieCommitMetadata> getInflightCommitMetadata(Option<byte[]> inflightContent) throws IOException {
    if (!inflightContent.isPresent() || inflightContent.get().length == 0) {
      // inflight files can be empty in some certain cases, e.g. when users opt in clustering
      return Option.empty();
    }
    return Option.of(HoodieCommitMetadata.fromBytes(inflightContent.get(), HoodieCommitMetadata.class));
  }

  private static Option<HoodieRequestedReplaceMetadata> getRequestedReplaceMetadata(Option<byte[]> requestedContent) throws IOException {
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
    HoodieTimeline timeline = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    return Option.of(TimelineUtils.getCommitMetadata(hoodieInstant, timeline));
  }

  /**
   * Convert commit metadata from json to avro.
   */
  public static <T extends SpecificRecordBase> T convertCommitMetadata(HoodieCommitMetadata hoodieCommitMetadata) {
    if (hoodieCommitMetadata instanceof HoodieReplaceCommitMetadata) {
      return (T) convertReplaceCommitMetadata((HoodieReplaceCommitMetadata) hoodieCommitMetadata);
    }
    hoodieCommitMetadata.getPartitionToWriteStats().remove(null);
    org.apache.hudi.avro.model.HoodieCommitMetadata avroMetaData = JsonUtils.getObjectMapper().convertValue(hoodieCommitMetadata, org.apache.hudi.avro.model.HoodieCommitMetadata.class);
    return (T) avroMetaData;
  }

  /**
   * Convert replacecommit metadata from json to avro.
   */
  private static org.apache.hudi.avro.model.HoodieReplaceCommitMetadata convertReplaceCommitMetadata(HoodieReplaceCommitMetadata replaceCommitMetadata) {
    replaceCommitMetadata.getPartitionToWriteStats().remove(null);
    replaceCommitMetadata.getPartitionToReplaceFileIds().remove(null);
    return JsonUtils.getObjectMapper().convertValue(replaceCommitMetadata, org.apache.hudi.avro.model.HoodieReplaceCommitMetadata.class);
  }

  /**
   * Convert commit metadata from avro to json.
   */
  public static <T extends SpecificRecordBase> byte[] convertCommitMetadataToJsonBytes(T avroMetaData, Class<T> clazz) {
    Schema avroSchema = clazz == org.apache.hudi.avro.model.HoodieReplaceCommitMetadata.class ? org.apache.hudi.avro.model.HoodieReplaceCommitMetadata.getClassSchema() :
        org.apache.hudi.avro.model.HoodieCommitMetadata.getClassSchema();
    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
      JsonEncoder jsonEncoder = new JsonEncoder(avroSchema, outputStream);
      DatumWriter<GenericRecord> writer = avroMetaData instanceof SpecificRecord ? new SpecificDatumWriter<>(avroSchema) : new GenericDatumWriter<>(avroSchema);
      writer.write(avroMetaData, jsonEncoder);
      jsonEncoder.flush();
      return outputStream.toByteArray();
    } catch (IOException e) {
      throw new HoodieIOException("Failed to convert to JSON.", e);
    }
  }
}
