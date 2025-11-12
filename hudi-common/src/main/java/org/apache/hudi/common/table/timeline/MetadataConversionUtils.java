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

import org.apache.hudi.avro.model.HoodieArchivedMetaEntry;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieLSMTimelineInstant;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.common.model.ActionType;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.versioning.v2.ArchivedTimelineV2;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.JsonUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Helper class to convert between different action related payloads and {@link HoodieArchivedMetaEntry}.
 */
public class MetadataConversionUtils {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataConversionUtils.class);

  public static HoodieArchivedMetaEntry createMetaWrapper(HoodieInstant hoodieInstant, HoodieTableMetaClient metaClient) {
    try {
      HoodieArchivedMetaEntry archivedMetaWrapper = new HoodieArchivedMetaEntry();
      archivedMetaWrapper.setCommitTime(hoodieInstant.requestedTime());
      archivedMetaWrapper.setActionState(hoodieInstant.getState().name());
      archivedMetaWrapper.setStateTransitionTime(hoodieInstant.getCompletionTime());
      CommitMetadataSerDe serDe = metaClient.getCommitMetadataSerDe();
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
          getCommitMetadata(metaClient, hoodieInstant, HoodieCommitMetadata.class)
              .ifPresent(commitMetadata -> archivedMetaWrapper.setHoodieCommitMetadata(convertCommitMetadataToAvro(commitMetadata)));
          archivedMetaWrapper.setActionType(ActionType.commit.name());
          break;
        }
        case HoodieTimeline.DELTA_COMMIT_ACTION: {
          getCommitMetadata(metaClient, hoodieInstant, HoodieCommitMetadata.class)
              .ifPresent(deltaCommitMetadata -> archivedMetaWrapper.setHoodieCommitMetadata(convertCommitMetadataToAvro(deltaCommitMetadata)));
          archivedMetaWrapper.setActionType(ActionType.deltacommit.name());
          break;
        }
        case HoodieTimeline.REPLACE_COMMIT_ACTION:
        case HoodieTimeline.CLUSTERING_ACTION: {
          if (hoodieInstant.isCompleted()) {
            getCommitMetadata(metaClient, hoodieInstant, HoodieReplaceCommitMetadata.class)
                .ifPresent(replaceCommitMetadata -> archivedMetaWrapper.setHoodieReplaceCommitMetadata(convertCommitMetadataToAvro(replaceCommitMetadata)));
          } else if (hoodieInstant.isInflight()) {
            // inflight replacecommit files have the same metadata body as HoodieCommitMetadata
            // so we could re-use it without further creating an inflight extension.
            // Or inflight replacecommit files are empty under clustering circumstance
            Option<HoodieCommitMetadata> inflightCommitMetadata = getCommitMetadata(metaClient, hoodieInstant, HoodieCommitMetadata.class);
            if (inflightCommitMetadata.isPresent()) {
              archivedMetaWrapper.setHoodieInflightReplaceMetadata(convertCommitMetadataToAvro(inflightCommitMetadata.get()));
            }
          } else {
            // we may have cases with empty HoodieRequestedReplaceMetadata e.g. insert_overwrite_table or insert_overwrite
            // without clustering. However, we should revisit the requested commit file standardization
            Option<HoodieRequestedReplaceMetadata> requestedReplaceMetadata = Option.of(metaClient.getActiveTimeline()
                .readRequestedReplaceMetadata(hoodieInstant));
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
            archivedMetaWrapper.setHoodieRollbackMetadata(
                metaClient.getActiveTimeline().readRollbackMetadata(hoodieInstant));
          }
          archivedMetaWrapper.setActionType(ActionType.rollback.name());
          break;
        }
        case HoodieTimeline.SAVEPOINT_ACTION: {
          archivedMetaWrapper.setHoodieSavePointMetadata(
              metaClient.getActiveTimeline().readSavepointMetadata(hoodieInstant));
          archivedMetaWrapper.setActionType(ActionType.savepoint.name());
          break;
        }
        case HoodieTimeline.COMPACTION_ACTION: {
          if (hoodieInstant.isRequested()) {
            HoodieCompactionPlan plan = CompactionUtils.getCompactionPlan(metaClient, hoodieInstant);
            archivedMetaWrapper.setHoodieCompactionPlan(plan);
          }
          archivedMetaWrapper.setActionType(ActionType.compaction.name());
          break;
        }
        case HoodieTimeline.LOG_COMPACTION_ACTION: {
          if (hoodieInstant.isRequested()) {
            HoodieCompactionPlan plan = CompactionUtils.getCompactionPlan(metaClient, hoodieInstant);
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
    } catch (IOException | HoodieIOException ex) {
      if (metaClient.getActiveTimeline().isEmpty(hoodieInstant)) {
        // in local FS and HDFS, there could be empty completed instants due to crash.
        // let's add an entry to the archival, even if not for the plan.
        return createMetaWrapperForEmptyInstant(hoodieInstant);
      }
      throw new HoodieException(ex);
    }
  }

  /**
   * Creates the legacy archived metadata entry from the new LSM-timeline read.
   *
   * <p>For legacy archive log, 3 entries are persisted for one instant, here only one summary entry is converted into.
   */
  public static HoodieArchivedMetaEntry createMetaWrapper(
      HoodieTableMetaClient metaClient, // should hold commit metadata serde as V2
      GenericRecord lsmTimelineRecord) throws IOException {
    ValidationUtils.checkState(metaClient.getTableConfig().getTableVersion().greaterThanOrEquals(HoodieTableVersion.EIGHT),
        "The meta client should be created from table version >= 8");
    ByteBuffer metadataBuffer = (ByteBuffer) lsmTimelineRecord.get(ArchivedTimelineV2.METADATA_ARCHIVED_META_FIELD);
    Option<byte[]> instantDetails = metadataBuffer != null ? Option.of(metadataBuffer.array()) : Option.empty();

    ByteBuffer planBuffer = (ByteBuffer) lsmTimelineRecord.get(ArchivedTimelineV2.PLAN_ARCHIVED_META_FIELD);
    Option<byte[]> planBytes = planBuffer != null ? Option.of(planBuffer.array()) : Option.empty();

    String instantTime = lsmTimelineRecord.get(ArchivedTimelineV2.INSTANT_TIME_ARCHIVED_META_FIELD).toString();
    String completionTime = lsmTimelineRecord.get(ArchivedTimelineV2.COMPLETION_TIME_ARCHIVED_META_FIELD).toString();

    HoodieArchivedMetaEntry archivedMetaWrapper = new HoodieArchivedMetaEntry();
    archivedMetaWrapper.setCommitTime(instantTime);
    archivedMetaWrapper.setActionState(HoodieInstant.State.COMPLETED.name());
    archivedMetaWrapper.setStateTransitionTime(completionTime);
    String actionType = lsmTimelineRecord.get(ArchivedTimelineV2.ACTION_ARCHIVED_META_FIELD).toString();
    HoodieInstant hoodieInstant = metaClient.getInstantGenerator().createNewInstant(HoodieInstant.State.COMPLETED, actionType, instantTime, completionTime);
    switch (actionType) {
      case HoodieTimeline.CLEAN_ACTION: {
        archivedMetaWrapper.setHoodieCleanMetadata(CleanerUtils.getCleanerMetadata(metaClient, new ByteArrayInputStream(instantDetails.get())));
        if (planBytes.isPresent()) {
          archivedMetaWrapper.setHoodieCleanerPlan(CleanerUtils.getCleanerPlan(metaClient, new ByteArrayInputStream(planBytes.get())));
        }
        archivedMetaWrapper.setActionType(ActionType.clean.name());
        break;
      }
      case HoodieTimeline.COMMIT_ACTION: {
        HoodieCommitMetadata commitMetadata = metaClient.getCommitMetadataSerDe().deserialize(hoodieInstant, new ByteArrayInputStream(instantDetails.get()),
            () -> instantDetails.get().length == 0, HoodieCommitMetadata.class);
        archivedMetaWrapper.setHoodieCommitMetadata(convertCommitMetadataToAvro(commitMetadata));
        archivedMetaWrapper.setActionType(ActionType.commit.name());

        if (planBytes.isPresent()) {
          // this should be a compaction
          HoodieCompactionPlan plan = CompactionUtils.getCompactionPlan(metaClient, new ByteArrayInputStream(planBytes.get()));
          archivedMetaWrapper.setHoodieCompactionPlan(plan);
        }
        break;
      }
      case HoodieTimeline.DELTA_COMMIT_ACTION: {
        HoodieCommitMetadata deltaCommitMetadata = metaClient.getCommitMetadataSerDe().deserialize(hoodieInstant, new ByteArrayInputStream(instantDetails.get()),
            () -> instantDetails.get().length == 0, HoodieCommitMetadata.class);
        archivedMetaWrapper.setHoodieCommitMetadata(convertCommitMetadataToAvro(deltaCommitMetadata));
        archivedMetaWrapper.setActionType(ActionType.deltacommit.name());

        if (planBytes.isPresent()) {
          // this should be a log compaction
          HoodieCompactionPlan plan = CompactionUtils.getCompactionPlan(metaClient, new ByteArrayInputStream(planBytes.get()));
          archivedMetaWrapper.setHoodieCompactionPlan(plan);
        }
        break;
      }
      case HoodieTimeline.REPLACE_COMMIT_ACTION:
      case HoodieTimeline.CLUSTERING_ACTION: {
        HoodieCommitMetadata replaceCommitMetadata = metaClient.getCommitMetadataSerDe().deserialize(hoodieInstant, new ByteArrayInputStream(instantDetails.get()),
            () -> instantDetails.get().length == 0, HoodieReplaceCommitMetadata.class);
        archivedMetaWrapper.setHoodieReplaceCommitMetadata(convertCommitMetadataToAvro(replaceCommitMetadata));

        // inflight replacecommit files have the same metadata body as HoodieCommitMetadata
        // so we could re-use it without further creating an inflight extension.
        // Or inflight replacecommit files are empty under clustering circumstance
        Option<HoodieCommitMetadata> inflightCommitMetadata = getInflightCommitMetadata(metaClient, hoodieInstant, instantDetails);
        if (inflightCommitMetadata.isPresent()) {
          archivedMetaWrapper.setHoodieInflightReplaceMetadata(convertCommitMetadataToAvro(inflightCommitMetadata.get()));
        }
        archivedMetaWrapper.setActionType(ActionType.replacecommit.name());
        break;
      }
      case HoodieTimeline.ROLLBACK_ACTION: {
        archivedMetaWrapper.setHoodieRollbackMetadata(
            TimelineMetadataUtils.deserializeAvroMetadata(new ByteArrayInputStream(instantDetails.get()), HoodieRollbackMetadata.class));
        archivedMetaWrapper.setActionType(ActionType.rollback.name());
        break;
      }
      case HoodieTimeline.SAVEPOINT_ACTION: {
        archivedMetaWrapper.setHoodieSavePointMetadata(
            TimelineMetadataUtils.deserializeAvroMetadata(new ByteArrayInputStream(instantDetails.get()), HoodieSavepointMetadata.class));
        archivedMetaWrapper.setActionType(ActionType.savepoint.name());
        break;
      }
      case HoodieTimeline.COMPACTION_ACTION: {
        // should be handled by commit_action branch though, this logic is redundant.
        HoodieCompactionPlan plan = CompactionUtils.getCompactionPlan(metaClient, new ByteArrayInputStream(planBytes.get()));
        archivedMetaWrapper.setHoodieCompactionPlan(plan);
        archivedMetaWrapper.setActionType(ActionType.compaction.name());
        break;
      }
      case HoodieTimeline.LOG_COMPACTION_ACTION: {
        HoodieCompactionPlan plan = CompactionUtils.getCompactionPlan(metaClient, new ByteArrayInputStream(planBytes.get()));
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

  private static Option<HoodieCommitMetadata> getInflightCommitMetadata(HoodieTableMetaClient metaClient, HoodieInstant instant,
                                                                        Option<byte[]> inflightContent) throws IOException {
    if (!inflightContent.isPresent() || inflightContent.get().length == 0) {
      // inflight files can be empty in some certain cases, e.g. when users opt in clustering
      return Option.empty();
    }
    return Option.of(metaClient.getCommitMetadataSerDe().deserialize(instant, new ByteArrayInputStream(inflightContent.get()),
        () -> inflightContent.get().length == 0, HoodieCommitMetadata.class));
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
    archivedMetaWrapper.setCommitTime(hoodieInstant.requestedTime());
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

  private static <T extends HoodieCommitMetadata> Option<T> getCommitMetadata(HoodieTableMetaClient metaClient, HoodieInstant instant, Class<T> clazz) throws IOException {
    T commitMetadata = metaClient.getActiveTimeline().readInstantContent(instant, clazz);
    // an empty file will return the default instance with an UNKNOWN operation type and in that case we return an empty option
    if (commitMetadata.getOperationType() == WriteOperationType.UNKNOWN) {
      return Option.empty();
    }
    return Option.of(commitMetadata);
  }

  public static Option<HoodieCommitMetadata> getHoodieCommitMetadata(HoodieTableMetaClient metaClient, HoodieInstant hoodieInstant) throws IOException {
    HoodieTimeline timeline = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    return Option.of(TimelineUtils.getCommitMetadata(hoodieInstant, timeline));
  }

  /**
   * Convert commit metadata from json to avro.
   */
  public static <T extends SpecificRecordBase> T convertCommitMetadataToAvro(HoodieCommitMetadata hoodieCommitMetadata) {
    removeNullKeyFromMapMembersForCommitMetadata(hoodieCommitMetadata);
    if (hoodieCommitMetadata instanceof HoodieReplaceCommitMetadata) {
      return (T) JsonUtils.getObjectMapper().convertValue(
          hoodieCommitMetadata, org.apache.hudi.avro.model.HoodieReplaceCommitMetadata.class);
    }
    org.apache.hudi.avro.model.HoodieCommitMetadata avroMetaData = JsonUtils.getObjectMapper().convertValue(
        hoodieCommitMetadata, org.apache.hudi.avro.model.HoodieCommitMetadata.class);
    return (T) avroMetaData;
  }

  public static <T> void removeNullKeyFromMapMembersForCommitMetadata(T metadata) {
    if (metadata instanceof HoodieReplaceCommitMetadata) {
      HoodieReplaceCommitMetadata hoodieCommitMetadata = (HoodieReplaceCommitMetadata) metadata;
      if (hoodieCommitMetadata.getPartitionToWriteStats().containsKey(null)) {
        LOG.info("partition path is null for {}", hoodieCommitMetadata.getPartitionToWriteStats().get(null));
        hoodieCommitMetadata.getPartitionToWriteStats().remove(null);
      }
      if (hoodieCommitMetadata.getPartitionToReplaceFileIds().containsKey(null)) {
        LOG.info("partition path is null for {}", hoodieCommitMetadata.getPartitionToReplaceFileIds().get(null));
        hoodieCommitMetadata.getPartitionToReplaceFileIds().remove(null);
      }
    } else if (metadata instanceof HoodieCommitMetadata) {
      HoodieCommitMetadata hoodieCommitMetadata = (HoodieCommitMetadata) metadata;
      if (hoodieCommitMetadata.getPartitionToWriteStats().containsKey(null)) {
        LOG.info("partition path is null for {}", hoodieCommitMetadata.getPartitionToWriteStats().get(null));
        hoodieCommitMetadata.getPartitionToWriteStats().remove(null);
      }
    }
  }

  /**
   * Convert replacecommit metadata from avro to pojo.
   */
  public static HoodieReplaceCommitMetadata convertReplaceCommitMetadataToPojo(org.apache.hudi.avro.model.HoodieReplaceCommitMetadata replaceCommitMetadata) {
    // While it is valid to have a null key in the hash map in java, avro map could not accommodate this, so we need to remove null key explicitly before the conversion.
    replaceCommitMetadata.getPartitionToWriteStats().remove(null);
    replaceCommitMetadata.getPartitionToReplaceFileIds().remove(null);
    return JsonUtils.getObjectMapper().convertValue(replaceCommitMetadata, HoodieReplaceCommitMetadata.class);
  }
}
