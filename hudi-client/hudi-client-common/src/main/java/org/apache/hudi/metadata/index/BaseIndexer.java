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

package org.apache.hudi.metadata.index;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.index.model.IndexInitializationContext;
import org.apache.hudi.metadata.index.model.IndexPartitionAndRecords;
import org.apache.hudi.metadata.index.model.IndexRestoreContext;

import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Base implementation of {@link Indexer} that handles common metadata-partition bootstrap flow,
 * including file-group initialization and commit.
 */
@Slf4j
public abstract class BaseIndexer implements Indexer {
  protected final HoodieEngineContext engineContext;
  protected final HoodieWriteConfig dataTableWriteConfig;
  protected final HoodieTableMetaClient dataTableMetaClient;

  protected BaseIndexer(
      HoodieEngineContext engineContext,
      HoodieWriteConfig dataTableWriteConfig,
      HoodieTableMetaClient dataTableMetaClient) {
    this.engineContext = engineContext;
    this.dataTableWriteConfig = dataTableWriteConfig;
    this.dataTableMetaClient = dataTableMetaClient;
  }

  /**
   * Resolves which partition of a definition-driven index type to initialize.
   * <p>
   * An indexing action names the partition in the context, and that partition is initialized
   * whenever its index definition exists, regardless of how many other definitions of the type
   * are still uninitialized. A regular write names nothing, and the partition is inferred from
   * the uninitialized definitions: exactly one means that one, any other count means nothing
   * is initialized. A requested partition without a definition goes through the same inference
   * (that is where a first-time index mints its definition from the write config), but the
   * inference only answers for the partition that was asked for: when it resolves to a single
   * partition that is a different index, or cannot resolve to exactly one at all, the action
   * fails rather than building another index while the requested partition is marked complete.
   *
   * @param context                  the initialization context
   * @param uninitializedPartitionsLookup the uninitialized partitions of this type, as the definition lookup reports
   *                                 them. Evaluated only when the request does not already answer itself: the lookup
   *                                 registers a new definition when it finds none uninitialized, and a run that
   *                                 initializes nothing must not leave one behind
   * @param indexType                the index type, for the messages
   * @return the partitions to initialize: exactly one, or none
   */
  protected Set<String> resolvePartitionsToInit(IndexInitializationContext context,
                                                Supplier<Set<String>> uninitializedPartitionsLookup,
                                                MetadataPartitionType indexType) {
    Option<String> requested = context.requestedIndexPartition();
    if (requested.isPresent()
        && dataTableMetaClient.getTableConfig().getMetadataPartitions().contains(requested.get())) {
      // Already initialized, typically by the write that committed between scheduling and running the
      // indexing action. Re-initializing would commit at an instant the metadata table already holds
      // completed, and the rollback-and-recommit inside that commit destroys the earlier commit's records.
      log.info("Metadata partition {} is already initialized, skipping", requested.get());
      return Collections.emptySet();
    }
    if (requested.isPresent() && dataTableMetaClient.getIndexForMetadataPartition(requested.get()).isPresent()) {
      return Collections.singleton(requested.get());
    }
    Set<String> uninitializedPartitions = uninitializedPartitionsLookup.get();
    // A single candidate answers for a request only when it is the requested partition itself, which is what a
    // first-time index looks like once the lookup has minted its definition. Any other single candidate is a
    // different index, and building it would leave the requested partition marked complete with nothing in it.
    if (uninitializedPartitions.size() == 1
        && (!requested.isPresent() || uninitializedPartitions.contains(requested.get()))) {
      return uninitializedPartitions;
    }
    if (requested.isPresent()) {
      throw new HoodieMetadataException(String.format(
          "Cannot initialize requested metadata partition %s: it has no index definition and the uninitialized %s definitions are %s, "
              + "so none can be inferred as the one meant", requested.get(), indexType.name(), uninitializedPartitions));
    }
    if (uninitializedPartitions.size() > 1) {
      log.warn("Skipping {} initialization as only one {} bootstrap at a time is supported for now. Provided: {}",
          indexType.name(), indexType.name(), uninitializedPartitions);
    }
    return Collections.emptySet();
  }

  /**
   * Hook invoked after the bootstrap bulk commit for an index partition succeeds.
   * <p>
   * The default implementation is a no-op. Subclasses can override this to perform index-specific
   * follow-up work (for example, index-definition
   * registration or post-commit validation).
   *
   * @param metadataMetaClient metadata table meta client used during initialization
   * @param records records committed during index partition initialization
   * @param fileGroupCount number of file groups created for the index partition
   * @param relativePartitionPath metadata table relative partition path being initialized
   */
  @Override
  public void postInitialization(HoodieTableMetaClient metadataMetaClient, HoodieData<HoodieRecord> records, int fileGroupCount, String relativePartitionPath) {
  }

  @Override
  public List<IndexPartitionAndRecords> buildRestore(IndexRestoreContext context) {
    return Collections.emptyList();
  }
}
