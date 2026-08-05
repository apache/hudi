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

package org.apache.hudi.metadata.index.vector;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.index.vector.VectorIndexOptions;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.index.BaseIndexer;
import org.apache.hudi.metadata.index.EngineIndexerSupport;
import org.apache.hudi.metadata.index.model.IndexCleanContext;
import org.apache.hudi.metadata.index.model.IndexInitializationContext;
import org.apache.hudi.metadata.index.model.IndexInitializationPlan;
import org.apache.hudi.metadata.index.model.IndexPartitionAndRecords;
import org.apache.hudi.metadata.index.model.IndexUpdateContext;
import org.apache.hudi.metadata.model.FileSliceAndPartition;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.metadata.MetadataPartitionType.VECTOR_INDEX;

/**
 * Implementation of {@link MetadataPartitionType#VECTOR_INDEX} index.
 * <p>
 * Owns partition sizing (IVF cluster to file-group mapping) and initialization-plan assembly.
 * The engine-specific record generation (reading the source VECTOR column, IVF clustering and
 * RaBitQ quantization) is dispatched through {@link EngineIndexerSupport}.
 * <p>
 * Index update, clean and restore lifecycle is handled in a follow-up change; for now those hooks
 * are no-ops so the initial bootstrap path is self-contained and reviewable.
 */
@Slf4j
public class VectorIndexer extends BaseIndexer {

  /**
   * Target size per metadata file group used by the sizing guard. Vector postings are routed to
   * file groups by cluster id, so this is only a sanity bound against gross over-provisioning.
   */
  private static final long TARGET_FILE_GROUP_BYTES = 512L * 1024L * 1024L;

  private final EngineIndexerSupport engineIndexerSupport;

  public VectorIndexer(
      HoodieEngineContext engineContext,
      HoodieWriteConfig dataTableWriteConfig,
      HoodieTableMetaClient dataTableMetaClient,
      EngineIndexerSupport engineIndexerSupport) {
    super(engineContext, dataTableWriteConfig, dataTableMetaClient);
    this.engineIndexerSupport = engineIndexerSupport;
  }

  @Override
  public List<IndexInitializationPlan> buildInitialization(IndexInitializationContext context) throws IOException {
    Set<String> completedPartitions = dataTableMetaClient.getTableConfig().getMetadataPartitions();
    Set<String> vectorIndexPartitionsToInit = dataTableMetaClient.getIndexMetadata()
        .map(metadata -> metadata.getIndexDefinitions().values().stream()
            .map(HoodieIndexDefinition::getIndexName)
            .filter(name -> name.startsWith(VECTOR_INDEX.getPartitionPath()))
            .filter(name -> !completedPartitions.contains(name))
            .collect(Collectors.toSet()))
        .orElse(Collections.emptySet());
    if (vectorIndexPartitionsToInit.size() != 1) {
      if (vectorIndexPartitionsToInit.size() > 1) {
        log.warn("Skipping vector index initialization as only one vector index bootstrap at a time "
            + "is supported for now. Provided: {}", vectorIndexPartitionsToInit);
      }
      return Collections.emptyList();
    }

    String indexName = vectorIndexPartitionsToInit.iterator().next();
    HoodieIndexDefinition indexDefinition = HoodieTableMetadataUtil.getHoodieIndexDefinition(indexName, dataTableMetaClient);
    ValidationUtils.checkState(indexDefinition != null, "Vector index definition is not present for index " + indexName);

    int fileGroupCount = resolveFileGroupCount(indexDefinition);

    HoodieSchema tableSchema = context.tableSchema().get()
        .orElseThrow(() -> new HoodieMetadataException("Table schema is not available for vector index initialization"));
    List<FileSliceAndPartition> fileSlices = context.latestFileSlices().get();

    // Initial bootstrap always allocates generation 1 (no pre-existing MDT partition to advance).
    HoodieData<HoodieRecord> records = engineIndexerSupport.generateVectorIndexRecords(
        indexDefinition, dataTableMetaClient, fileSlices, tableSchema, 1);

    return Collections.singletonList(IndexInitializationPlan.of(fileGroupCount, indexName, records));
  }

  /**
   * Sizes the vector metadata partition by IVF clusters. Posting rows are routed to file groups by
   * cluster id ({@code mapVectorPostingKeyToFileGroupIndex}), so
   * {@code fileGroupCount = ceil(numClusters / clustersPerFileGroup)} keeps one cluster per file group
   * by default. An explicit {@code hoodie.metadata.vector.index.file.group.count} overrides the derived
   * value.
   */
  private int resolveFileGroupCount(HoodieIndexDefinition indexDefinition) {
    VectorIndexOptions.ResolvedOptions resolvedOptions = VectorIndexOptions.resolve(indexDefinition.getIndexOptions());
    int numClusters = resolvedOptions.numClusters;
    int clustersPerFileGroup = 1;
    int configuredFileGroupCount = dataTableWriteConfig.getMetadataConfig().getVectorIndexFileGroupCount();
    int fileGroupCount = configuredFileGroupCount > 0
        ? configuredFileGroupCount
        : Math.max(1, (int) Math.ceil((double) numClusters / clustersPerFileGroup));

    long targetBlockBytes = 64L * 1024L;
    long projectedIndexBytes = Math.max(1L, (long) numClusters * targetBlockBytes);
    if ((long) fileGroupCount * TARGET_FILE_GROUP_BYTES > projectedIndexBytes * 10L) {
      log.warn("Vector index {} is initializing with {} file groups for projected index size {} bytes. "
              + "This exceeds the 10x sizing guard against 512 MiB target file groups; consider setting {} "
              + "to a smaller value before bootstrap.",
          indexDefinition.getIndexName(), fileGroupCount, projectedIndexBytes,
          org.apache.hudi.common.config.HoodieMetadataConfig.VECTOR_INDEX_FILE_GROUP_COUNT.key());
    }
    log.info("Initializing vector index {} with {} file groups "
            + "[numClusters={}, clustersPerFileGroup={}, configuredFileGroupCount={}]",
        indexDefinition.getIndexName(), fileGroupCount, numClusters, clustersPerFileGroup, configuredFileGroupCount);
    return fileGroupCount;
  }

  @Override
  public List<IndexPartitionAndRecords> buildUpdate(IndexUpdateContext context) {
    // Vector index incremental maintenance (append/delete postings, generation advance) is handled
    // in a follow-up change. Initialization is a full re-bootstrap until then.
    return Collections.emptyList();
  }

  @Override
  public List<IndexPartitionAndRecords> buildClean(IndexCleanContext context) {
    // See buildUpdate: lifecycle maintenance is handled in a follow-up change.
    return Collections.emptyList();
  }
}
