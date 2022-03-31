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

package org.apache.hudi.common.testutils;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.common.data.HoodieList;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;

import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * {@link HoodieTestTable} impl used for testing metadata. This class does synchronous updates to HoodieTableMetadataWriter if non null.
 */
public class HoodieMetadataTestTable extends HoodieTestTable {

  private final HoodieTableMetadataWriter writer;

  protected HoodieMetadataTestTable(String basePath, FileSystem fs, HoodieTableMetaClient metaClient, HoodieTableMetadataWriter writer) {
    super(basePath, fs, metaClient);
    this.writer = writer;
  }

  public static HoodieTestTable of(HoodieTableMetaClient metaClient) {
    return HoodieMetadataTestTable.of(metaClient, null);
  }

  public static HoodieTestTable of(HoodieTableMetaClient metaClient, HoodieTableMetadataWriter writer) {
    testTableState = HoodieTestTableState.of();
    return new HoodieMetadataTestTable(metaClient.getBasePath(), metaClient.getRawFs(), metaClient, writer);
  }

  /**
   * Add commits to the requested partitions and update metadata table.
   *
   * @param commitTime                    - Commit time for the operation
   * @param operationType                 - Operation type
   * @param newPartitionsToAdd            - New partitions to add for the operation
   * @param partitionToFilesNameLengthMap - Map of partition names to its list of files name and length pair
   * @param bootstrap                     - Whether bootstrapping needed for the operation
   * @param createInflightCommit          - Whether in flight commit needed for the operation
   * @return Commit metadata for the commit operation performed.
   * @throws Exception
   */
  @Override
  public HoodieCommitMetadata doWriteOperation(String commitTime, WriteOperationType operationType,
                                               List<String> newPartitionsToAdd,
                                               Map<String, List<Pair<String, Integer>>> partitionToFilesNameLengthMap,
                                               boolean bootstrap, boolean createInflightCommit) throws Exception {
    HoodieCommitMetadata commitMetadata = super.doWriteOperation(commitTime, operationType, newPartitionsToAdd,
        partitionToFilesNameLengthMap, bootstrap, createInflightCommit);
    if (writer != null && !createInflightCommit) {
      writer.update(commitMetadata, HoodieList.of(Collections.EMPTY_LIST), commitTime, false);
    }
    return commitMetadata;
  }

  @Override
  public HoodieTestTable moveInflightCommitToComplete(String instantTime, HoodieCommitMetadata metadata) throws IOException {
    super.moveInflightCommitToComplete(instantTime, metadata);
    if (writer != null) {
      writer.update(metadata, HoodieList.of(Collections.EMPTY_LIST), instantTime, false);
    }
    return this;
  }

  public HoodieTestTable moveInflightCommitToComplete(String instantTime, HoodieCommitMetadata metadata, boolean ignoreWriter) throws IOException {
    super.moveInflightCommitToComplete(instantTime, metadata);
    if (!ignoreWriter && writer != null) {
      writer.update(metadata, HoodieList.of(Collections.EMPTY_LIST), instantTime, false);
    }
    return this;
  }

  @Override
  public HoodieTestTable moveInflightCompactionToComplete(String instantTime, HoodieCommitMetadata metadata) throws IOException {
    super.moveInflightCompactionToComplete(instantTime, metadata);
    if (writer != null) {
      writer.update(metadata, HoodieList.of(Collections.EMPTY_LIST), instantTime, true);
    }
    return this;
  }

  @Override
  public HoodieCleanMetadata doClean(String commitTime, Map<String, Integer> partitionFileCountsToDelete) throws IOException {
    HoodieCleanMetadata cleanMetadata = super.doClean(commitTime, partitionFileCountsToDelete);
    if (writer != null) {
      writer.update(cleanMetadata, commitTime);
    }
    return cleanMetadata;
  }

  public HoodieTestTable addCompaction(String instantTime, HoodieCommitMetadata commitMetadata) throws Exception {
    super.addCompaction(instantTime, commitMetadata);
    if (writer != null) {
      writer.update(commitMetadata, HoodieList.of(Collections.EMPTY_LIST), instantTime, true);
    }
    return this;
  }

  @Override
  public HoodieTestTable addRollback(String instantTime, HoodieRollbackMetadata rollbackMetadata) throws IOException {
    super.addRollback(instantTime, rollbackMetadata);
    if (writer != null) {
      writer.update(rollbackMetadata, instantTime);
    }
    return this;
  }

  @Override
  public HoodieTestTable addRestore(String instantTime, HoodieRestoreMetadata restoreMetadata) throws IOException {
    super.addRestore(instantTime, restoreMetadata);
    if (writer != null) {
      writer.update(restoreMetadata, instantTime);
    }
    return this;
  }

  @Override
  public HoodieTestTable addReplaceCommit(
      String instantTime,
      Option<HoodieRequestedReplaceMetadata> requestedReplaceMetadata,
      Option<HoodieCommitMetadata> inflightReplaceMetadata,
      HoodieReplaceCommitMetadata completeReplaceMetadata) throws Exception {
    super.addReplaceCommit(instantTime, requestedReplaceMetadata, inflightReplaceMetadata, completeReplaceMetadata);
    if (writer != null) {
      writer.update(completeReplaceMetadata, HoodieList.of(Collections.EMPTY_LIST), instantTime, true);
    }
    return this;
  }

}
