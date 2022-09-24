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

package org.apache.hudi.client.transaction;

import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.client.utils.MetadataConversionUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieMetadataWrapper;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMPACTION_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;
import static org.apache.hudi.common.util.CommitUtils.getFileIdWithoutSuffixAndRelativePathsFromSpecificRecord;

/**
 * This class is used to hold all information used to identify how to resolve conflicts between instants.
 * Since we interchange payload types between AVRO specific records and POJO's, this object serves as
 * a common payload to manage these conversions.
 */
public class ConcurrentOperation {

  private WriteOperationType operationType;
  private final HoodieMetadataWrapper metadataWrapper;
  private final Option<HoodieCommitMetadata> commitMetadataOption;
  private final String actionState;
  private final String actionType;
  private final String instantTime;
  private Set<String> mutatedFileIds = Collections.EMPTY_SET;

  public ConcurrentOperation(HoodieInstant instant, HoodieTableMetaClient metaClient) throws IOException {
    this.metadataWrapper = new HoodieMetadataWrapper(MetadataConversionUtils.createMetaWrapper(instant, metaClient));
    this.commitMetadataOption = Option.empty();
    this.actionState = instant.getState().name();
    this.actionType = instant.getAction();
    this.instantTime = instant.getTimestamp();
    init(instant);
  }

  public ConcurrentOperation(HoodieInstant instant, HoodieCommitMetadata commitMetadata) {
    this.commitMetadataOption = Option.of(commitMetadata);
    this.metadataWrapper = new HoodieMetadataWrapper(commitMetadata);
    this.actionState = instant.getState().name();
    this.actionType = instant.getAction();
    this.instantTime = instant.getTimestamp();
    init(instant);
  }

  public String getInstantActionState() {
    return actionState;
  }

  public String getInstantActionType() {
    return actionType;
  }

  public String getInstantTimestamp() {
    return instantTime;
  }

  public WriteOperationType getOperationType() {
    return operationType;
  }

  public Set<String> getMutatedFileIds() {
    return mutatedFileIds;
  }

  public Option<HoodieCommitMetadata> getCommitMetadataOption() {
    return commitMetadataOption;
  }

  private void init(HoodieInstant instant) {
    if (this.metadataWrapper.isAvroMetadata()) {
      switch (getInstantActionType()) {
        case COMPACTION_ACTION:
          this.operationType = WriteOperationType.COMPACT;
          this.mutatedFileIds = this.metadataWrapper.getMetadataFromTimeline().getHoodieCompactionPlan().getOperations()
              .stream()
              .map(op -> op.getFileId())
              .collect(Collectors.toSet());
          break;
        case COMMIT_ACTION:
        case DELTA_COMMIT_ACTION:
          this.mutatedFileIds = getFileIdWithoutSuffixAndRelativePathsFromSpecificRecord(this.metadataWrapper.getMetadataFromTimeline().getHoodieCommitMetadata()
              .getPartitionToWriteStats()).keySet();
          this.operationType = WriteOperationType.fromValue(this.metadataWrapper.getMetadataFromTimeline().getHoodieCommitMetadata().getOperationType());
          break;
        case REPLACE_COMMIT_ACTION:
          if (instant.isCompleted()) {
            this.mutatedFileIds = getFileIdWithoutSuffixAndRelativePathsFromSpecificRecord(
                this.metadataWrapper.getMetadataFromTimeline().getHoodieReplaceCommitMetadata().getPartitionToWriteStats()).keySet();
            this.operationType = WriteOperationType.fromValue(this.metadataWrapper.getMetadataFromTimeline().getHoodieReplaceCommitMetadata().getOperationType());
          } else {
            // we need to have different handling for requested and inflight replacecommit because
            // for requested replacecommit, clustering will generate a plan and HoodieRequestedReplaceMetadata will not be empty, but insert_overwrite/insert_overwrite_table could have empty content
            // for inflight replacecommit, clustering will have no content in metadata, but insert_overwrite/insert_overwrite_table will have some commit metadata
            HoodieRequestedReplaceMetadata requestedReplaceMetadata = this.metadataWrapper.getMetadataFromTimeline().getHoodieRequestedReplaceMetadata();
            org.apache.hudi.avro.model.HoodieCommitMetadata inflightCommitMetadata = this.metadataWrapper.getMetadataFromTimeline().getHoodieInflightReplaceMetadata();
            if (instant.isRequested()) {
              // for insert_overwrite/insert_overwrite_table clusteringPlan will be empty
              if (requestedReplaceMetadata != null && requestedReplaceMetadata.getClusteringPlan() != null) {
                this.mutatedFileIds = getFileIdsFromRequestedReplaceMetadata(requestedReplaceMetadata);
                this.operationType = WriteOperationType.CLUSTER;
              }
            } else {
              if (inflightCommitMetadata != null) {
                this.mutatedFileIds = getFileIdWithoutSuffixAndRelativePathsFromSpecificRecord(inflightCommitMetadata.getPartitionToWriteStats()).keySet();
                this.operationType = WriteOperationType.fromValue(this.metadataWrapper.getMetadataFromTimeline().getHoodieInflightReplaceMetadata().getOperationType());
              } else if (requestedReplaceMetadata != null) {
                // inflight replacecommit metadata is empty due to clustering, read fileIds from requested replacecommit
                this.mutatedFileIds = getFileIdsFromRequestedReplaceMetadata(requestedReplaceMetadata);
                this.operationType = WriteOperationType.CLUSTER;
              }
              // NOTE: it cannot be the case that instant is inflight, and both the requested and inflight replacecommit metadata are empty
            }
          }
          break;
        default:
          throw new IllegalArgumentException("Unsupported Action Type " + getInstantActionType());
      }
    } else {
      switch (getInstantActionType()) {
        case COMMIT_ACTION:
        case DELTA_COMMIT_ACTION:
          this.mutatedFileIds = CommitUtils.getFileIdWithoutSuffixAndRelativePaths(this.metadataWrapper.getCommitMetadata().getPartitionToWriteStats()).keySet();
          this.operationType = this.metadataWrapper.getCommitMetadata().getOperationType();
          break;
        default:
          throw new IllegalArgumentException("Unsupported Action Type " + getInstantActionType());
      }
    }
  }

  private static Set<String> getFileIdsFromRequestedReplaceMetadata(HoodieRequestedReplaceMetadata requestedReplaceMetadata) {
    return requestedReplaceMetadata
        .getClusteringPlan().getInputGroups()
        .stream()
        .flatMap(ig -> ig.getSlices().stream())
        .map(file -> file.getFileId())
        .collect(Collectors.toSet());
  }

  @Override
  public String toString() {
    return "{"
        + "actionType=" + this.getInstantActionType()
        + ", instantTime=" + this.getInstantTimestamp()
        + ", actionState=" + this.getInstantActionState()
        + '\'' + '}';
  }
}