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

package org.apache.hudi.metadata;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieIndexPartitionInfo;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.VisibleForTesting;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * Interface that supports updating metadata for a given table, as actions complete.
 */
public interface HoodieTableMetadataWriter<I,O> extends Serializable, AutoCloseable {

  /**
   * Builds the given metadata partitions to create index.
   *
   * @param engineContext
   * @param indexPartitionInfos - information about partitions to build such as partition type and base instant time
   * @param instantTime The async index instant time from data table
   */
  void buildMetadataPartitions(HoodieEngineContext engineContext, List<HoodieIndexPartitionInfo> indexPartitionInfos, String instantTime) throws IOException;

  /**
   * Drop the given metadata partitions.
   *
   * @param metadataPartitions List of MDT partitions to drop
   * @throws IOException on failures
   */
  void dropMetadataPartitions(List<String> metadataPartitions) throws IOException;

  /**
   * Update the metadata table due to a COMMIT operation.
   * @param commitMetadata commit metadata of the operation of interest.
   * @param instantTime    instant time of the commit.
   */
  void update(HoodieCommitMetadata commitMetadata, String instantTime);

  /**
   * Update the metadata table due to a CLEAN operation.
   *
   * @param cleanMetadata clean metadata of the operation of interest.
   * @param instantTime   instant time of the commit.
   */
  void update(HoodieCleanMetadata cleanMetadata, String instantTime);

  /**
   * Update the metadata table due to a RESTORE operation.
   *
   * @param restoreMetadata restore metadata of the operation of interest.
   * @param instantTime     instant time of the commit.
   */
  void update(HoodieRestoreMetadata restoreMetadata, String instantTime);

  /**
   * Update the metadata table due to a ROLLBACK operation.
   *
   * @param rollbackMetadata rollback metadata of the operation of interest.
   * @param instantTime      instant time of the commit.
   */
  void update(HoodieRollbackMetadata rollbackMetadata, String instantTime);

  /**
   * Deletes the given metadata partitions. This path reuses DELETE_PARTITION operation.
   *
   * @param instantTime - instant time when replacecommit corresponding to the drop will be recorded in the metadata timeline
   * @param partitions  - list of {@link MetadataPartitionType} to drop
   */
  void deletePartitions(String instantTime, List<MetadataPartitionType> partitions);

  /**
   * Returns true if the metadata table is initialized.
   */
  boolean isInitialized();

  /**
   * Perform various table services like compaction, cleaning, archiving on the MDT if required.
   *
   * @param inFlightInstantTimestamp Timestamp of an instant which is in-progress. This instant is ignored while
   *                                 deciding if optimizations can be performed.
   * @param requiresTimelineRefresh set to true only if timeline requires reload, mainly used for testing
   */
  @VisibleForTesting
  void performTableServices(Option<String> inFlightInstantTimestamp, boolean requiresTimelineRefresh);

  /**
   * Perform various table services like compaction, cleaning, archiving on the MDT if required.
   *
   * @param inFlightInstantTimestamp Timestamp of an instant which is in-progress. This instant is ignored while
   *                                 deciding if optimizations can be performed.
   */
  default void performTableServices(Option<String> inFlightInstantTimestamp) {
    performTableServices(inFlightInstantTimestamp, false);
  }
}
