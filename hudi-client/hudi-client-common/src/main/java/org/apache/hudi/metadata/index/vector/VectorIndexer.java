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

import org.apache.hudi.avro.model.HoodieVectorIndexActiveManifest;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.index.vector.VectorIndexOptions;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.VectorIndexMetadataKey;
import org.apache.hudi.metadata.VectorMetadataRawKey;
import org.apache.hudi.metadata.index.BaseIndexer;
import org.apache.hudi.metadata.index.EngineIndexerSupport;
import org.apache.hudi.metadata.index.model.IndexCleanContext;
import org.apache.hudi.metadata.index.model.IndexInitializationContext;
import org.apache.hudi.metadata.index.model.IndexInitializationPlan;
import org.apache.hudi.metadata.index.model.IndexPartitionAndRecords;
import org.apache.hudi.metadata.index.model.IndexUpdateContext;
import org.apache.hudi.metadata.model.FileSliceAndPartition;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
 * Incremental maintenance follows the secondary-index/RLI file-group diff contract, while clean
 * and restore lifecycle work remains separate.
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
    if (!VECTOR_INDEX.isMetadataPartitionAvailable(dataTableMetaClient)) {
      return Collections.emptyList();
    }
    List<IndexPartitionAndRecords> partitionUpdates = new ArrayList<>();
    for (String indexPartition : vectorIndexPartitions()) {
      int generation = activeGeneration(context, indexPartition);
      if (hasMarker(context, indexPartition, generation)) {
        log.info("Skipping vector index update already marked for source instant {} in {}",
            context.instantTime(), indexPartition);
        continue;
      }
      try {
        HoodieIndexDefinition indexDefinition = HoodieTableMetadataUtil.getHoodieIndexDefinition(
            indexPartition, dataTableMetaClient);
        ValidationUtils.checkState(indexDefinition != null,
            "Vector index definition is not present for index " + indexPartition);
        HoodieSchema tableSchema = new TableSchemaResolver(dataTableMetaClient).getTableSchema();
        List<VectorIndexFileGroupUpdate> updates = buildFileGroupUpdates(context);
        HoodieData<HoodieRecord> records = engineIndexerSupport.generateVectorIndexUpdateRecords(
            indexDefinition, dataTableMetaClient, context.tableMetadata(), updates,
            tableSchema, generation, context.instantTime());
        HoodieRecord marker = HoodieMetadataPayload.createVectorIndexSourceInstantMarkerRecord(
            generation, context.instantTime(), indexPartition);
        partitionUpdates.add(IndexPartitionAndRecords.of(
            indexPartition,
            records.union(HoodieListData.eager(Collections.singletonList(marker)))));
      } catch (Exception e) {
        throw new HoodieMetadataException(
            "Failed to update vector index " + indexPartition + " for " + context.instantTime(), e);
      }
    }
    return partitionUpdates;
  }

  private List<VectorIndexFileGroupUpdate> buildFileGroupUpdates(IndexUpdateContext context) {
    Map<String, List<HoodieWriteStat>> byFileGroup = context.commitMetadata()
        .getPartitionToWriteStats().values().stream()
        .flatMap(Collection::stream)
        .collect(Collectors.groupingBy(
            stat -> stat.getPartitionPath() + '\u0000' + stat.getFileId(),
            LinkedHashMap::new,
            Collectors.toList()));
    HoodieTableFileSystemView view = context.lazyFileSystemView().get();
    List<VectorIndexFileGroupUpdate> updates = new ArrayList<>(byFileGroup.size());
    for (List<HoodieWriteStat> writeStats : byFileGroup.values()) {
      String partition = writeStats.get(0).getPartitionPath();
      String fileId = writeStats.get(0).getFileId();
      Option<FileSlice> previous = view.getLatestMergedFileSliceBeforeOrOn(
          partition, context.instantTime(), fileId).map(FileSlice::new);
      updates.add(new VectorIndexFileGroupUpdate(
          partition, previous, buildCurrentSlice(partition, fileId, context.instantTime(), previous, writeStats)));
    }
    return updates;
  }

  private FileSlice buildCurrentSlice(
      String partition,
      String fileId,
      String instantTime,
      Option<FileSlice> previous,
      List<HoodieWriteStat> writeStats) {
    StoragePath basePath = dataTableMetaClient.getBasePath();
    List<HoodieWriteStat> baseStats = writeStats.stream()
        .filter(stat -> !FSUtils.isLogFile(new StoragePath(basePath, stat.getPath())))
        .collect(Collectors.toList());
    ValidationUtils.checkArgument(baseStats.isEmpty() || baseStats.size() == 1,
        "Only one new base file is expected per vector-index file-group update");
    ValidationUtils.checkArgument(baseStats.isEmpty() || writeStats.size() == 1,
        "A vector-index file-group update cannot mix a base file and log files");
    if (!baseStats.isEmpty()) {
      HoodieWriteStat stat = baseStats.get(0);
      FileSlice current = new FileSlice(partition, instantTime, fileId);
      current.setBaseFile(new HoodieBaseFile(new StoragePathInfo(
          new StoragePath(basePath, stat.getPath()), stat.getFileSizeInBytes(),
          false, (short) 0, 0, 0)));
      return current;
    }

    List<HoodieLogFile> newLogFiles = writeStats.stream()
        .map(stat -> new HoodieLogFile(new StoragePathInfo(
            new StoragePath(basePath, stat.getPath()), stat.getFileSizeInBytes(),
            false, (short) 0, 0, 0)))
        .collect(Collectors.toList());
    String baseInstant = previous.map(FileSlice::getBaseInstantTime)
        .orElseGet(() -> newLogFiles.get(0).getDeltaCommitTime());
    ValidationUtils.checkArgument(newLogFiles.stream()
            .allMatch(logFile -> baseInstant.equals(logFile.getDeltaCommitTime())),
        "All log files in a vector-index update must belong to the same base file slice");
    FileSlice current = previous.map(FileSlice::new)
        .orElseGet(() -> new FileSlice(partition, baseInstant, fileId));
    newLogFiles.forEach(current::addLogFile);
    return current;
  }

  private int activeGeneration(IndexUpdateContext context, String indexPartition) {
    Object metadata = readExactMetadata(
        context, indexPartition, VectorIndexMetadataKey.activeManifest())
        .orElseThrow(() -> new HoodieMetadataException(
            "Active vector generation pointer is missing for " + indexPartition));
    ValidationUtils.checkState(metadata instanceof HoodieVectorIndexActiveManifest,
        "Unexpected active vector generation payload for " + indexPartition);
    Integer generation = ((HoodieVectorIndexActiveManifest) metadata).getActiveGeneration();
    ValidationUtils.checkState(generation != null,
        "Vector index has no ACTIVE generation for " + indexPartition);
    return generation;
  }

  private boolean hasMarker(
      IndexUpdateContext context, String indexPartition, int generation) {
    return readExactMetadata(
        context,
        indexPartition,
        VectorIndexMetadataKey.sourceInstantMarker(generation, context.instantTime())).isPresent();
  }

  private Option<Object> readExactMetadata(
      IndexUpdateContext context, String indexPartition, String exactKey) {
    return Option.fromJavaOptional(context.tableMetadata()
        .getRecordsByKeyPrefixes(
            HoodieListData.eager(Collections.singletonList(new VectorMetadataRawKey(exactKey))),
            indexPartition,
            true)
        .collectAsList().stream()
        .filter(record -> exactKey.equals(record.getRecordKey()))
        .map(record -> ((HoodieMetadataPayload) record.getData()).getVectorIndexMetadata())
        .filter(Option::isPresent)
        .map(Option::get)
        .findFirst());
  }

  private Set<String> vectorIndexPartitions() {
    return dataTableMetaClient.getTableConfig().getMetadataPartitions().stream()
        .filter(partition -> partition.startsWith(VECTOR_INDEX.getPartitionPath()))
        .collect(Collectors.toSet());
  }

  @Override
  public List<IndexPartitionAndRecords> buildClean(IndexCleanContext context) {
    // Vector delta compaction and cleaner lifecycle are handled separately.
    return Collections.emptyList();
  }
}
