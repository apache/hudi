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

package org.apache.hudi.metadata.index.fulltext;

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.HoodieStorageUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.core.io.storage.HoodieAvroFileReader;
import org.apache.hudi.core.io.storage.HoodieIOFactory;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.metadata.FullTextIndexUtils;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.index.BaseIndexer;
import org.apache.hudi.metadata.index.model.IndexCleanContext;
import org.apache.hudi.metadata.index.model.IndexInitializationContext;
import org.apache.hudi.metadata.index.model.IndexInitializationPlan;
import org.apache.hudi.metadata.index.model.IndexPartitionAndRecords;
import org.apache.hudi.metadata.index.model.IndexUpdateContext;
import org.apache.hudi.metadata.model.FileSliceAndPartition;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_FULL_TEXT_INDEX_PREFIX;
import static org.apache.hudi.metadata.MetadataPartitionType.FULL_TEXT_INDEX;

/**
 * Implementation of {@link MetadataPartitionType#FULL_TEXT_INDEX}.
 *
 * <p>Every immutable base file is a posting unit. A commit indexes each base file it writes and
 * retires the units it supersedes (a rewritten base file, or file groups replaced by clustering,
 * insert overwrite or delete partition). Readers only trust units that are live in the file slices
 * they query, so a missed retirement leaves stale entries that are ignored, never wrong results.
 */
@Slf4j
public class FullTextIndexer extends BaseIndexer {

  public static final String OPTION_FILE_GROUP_COUNT = "file_group_count";
  private static final int DEFAULT_FILE_GROUP_COUNT = 4;

  // Lets tests leave retired units in place to prove that readers ignore stale entries.
  @VisibleForTesting
  public static volatile boolean skipRetirementForTesting = false;

  public FullTextIndexer(
      HoodieEngineContext engineContext,
      HoodieWriteConfig dataTableWriteConfig,
      HoodieTableMetaClient dataTableMetaClient) {
    super(engineContext, dataTableWriteConfig, dataTableMetaClient);
  }

  @Override
  public List<IndexInitializationPlan> buildInitialization(IndexInitializationContext context) throws IOException {
    Set<String> partitionsToInit = HoodieTableMetadataUtil.getFullTextIndexPartitionsToInit(dataTableMetaClient);
    if (partitionsToInit.isEmpty()) {
      return Collections.emptyList();
    }
    if (partitionsToInit.size() > 1) {
      log.warn("Only one full-text index bootstrap at a time is supported. Provided: {}", partitionsToInit);
      return Collections.emptyList();
    }
    String indexName = partitionsToInit.iterator().next();
    HoodieIndexDefinition indexDefinition = HoodieTableMetadataUtil.getHoodieIndexDefinition(indexName, dataTableMetaClient);
    ValidationUtils.checkState(indexDefinition != null, "Full-text index definition is not present for index " + indexName);

    StoragePath basePath = dataTableMetaClient.getBasePath();
    List<FileUnit> units = new ArrayList<>();
    for (FileSliceAndPartition sliceAndPartition : context.latestFileSlices().get()) {
      FileSlice slice = sliceAndPartition.getFileSlice();
      Option<HoodieBaseFile> baseFile = slice.getBaseFile();
      if (baseFile.isPresent()) {
        String relativePath = FSUtils.getRelativePartitionPath(basePath, baseFile.get().getStoragePath());
        units.add(new FileUnit(relativePath, slice.getFileId(), FullTextIndexUtils.baseUnit(baseFile.get().getCommitTime()), false));
      }
    }
    int fileGroupCount = Integer.parseInt(indexDefinition.getIndexOptions().getOrDefault(OPTION_FILE_GROUP_COUNT,
        String.valueOf(DEFAULT_FILE_GROUP_COUNT)));
    return Collections.singletonList(IndexInitializationPlan.of(fileGroupCount, indexName, toRecords(units, indexDefinition)));
  }

  @Override
  public List<IndexPartitionAndRecords> buildUpdate(IndexUpdateContext context) {
    if (!FULL_TEXT_INDEX.isMetadataPartitionAvailable(dataTableMetaClient)) {
      return Collections.emptyList();
    }
    HoodieCommitMetadata commitMetadata = context.commitMetadata();
    if (commitMetadata.getOperationType() == WriteOperationType.COMPACT) {
      // Copy-on-write tables have no compaction; merge-on-read tables are rejected at index creation.
      return Collections.emptyList();
    }
    List<FileUnit> units = collectUnits(commitMetadata, context.lazyFileSystemView().get());
    // Inflight partitions are included so that commits landing while the index is being built are indexed.
    Set<String> partitions = new HashSet<>(dataTableMetaClient.getTableConfig().getMetadataPartitions());
    partitions.addAll(dataTableMetaClient.getTableConfig().getMetadataPartitionsInflight());
    return partitions.stream()
        .filter(partition -> partition.startsWith(PARTITION_NAME_FULL_TEXT_INDEX_PREFIX))
        .map(partition -> IndexPartitionAndRecords.of(partition,
            toRecords(units, HoodieTableMetadataUtil.getHoodieIndexDefinition(partition, dataTableMetaClient))))
        .collect(Collectors.toList());
  }

  @Override
  public List<IndexPartitionAndRecords> buildClean(IndexCleanContext context) {
    return Collections.emptyList();
  }

  private List<FileUnit> collectUnits(HoodieCommitMetadata commitMetadata, HoodieTableFileSystemView fileSystemView) {
    List<FileUnit> units = new ArrayList<>();
    for (Map.Entry<String, List<HoodieWriteStat>> entry : commitMetadata.getPartitionToWriteStats().entrySet()) {
      for (HoodieWriteStat stat : entry.getValue()) {
        String path = stat.getPath();
        if (path == null || !FSUtils.isBaseFile(new StoragePath(path).getName())) {
          continue;
        }
        String fileName = new StoragePath(path).getName();
        units.add(new FileUnit(path, stat.getFileId(), FullTextIndexUtils.baseUnit(FSUtils.getCommitTime(fileName)), false));
        String prevBaseFile = stat.getPrevBaseFile();
        if (!skipRetirementForTesting && prevBaseFile != null && !prevBaseFile.isEmpty()) {
          String prevPath = entry.getKey().isEmpty() ? prevBaseFile : entry.getKey() + StoragePath.SEPARATOR + prevBaseFile;
          units.add(new FileUnit(prevPath, stat.getFileId(), FullTextIndexUtils.baseUnit(FSUtils.getCommitTime(prevBaseFile)), true));
        }
      }
    }
    if (!skipRetirementForTesting && commitMetadata instanceof HoodieReplaceCommitMetadata) {
      StoragePath basePath = dataTableMetaClient.getBasePath();
      for (Map.Entry<String, List<String>> entry : ((HoodieReplaceCommitMetadata) commitMetadata).getPartitionToReplaceFileIds().entrySet()) {
        for (String fileId : entry.getValue()) {
          Option<HoodieBaseFile> baseFile = fileSystemView.getLatestBaseFile(entry.getKey(), fileId);
          if (baseFile.isPresent()) {
            String relativePath = FSUtils.getRelativePartitionPath(basePath, baseFile.get().getStoragePath());
            units.add(new FileUnit(relativePath, fileId, FullTextIndexUtils.baseUnit(baseFile.get().getCommitTime()), true));
          } else {
            log.warn("No base file found for replaced file group {} in partition {}; its full-text entries stay until reindex", fileId, entry.getKey());
          }
        }
      }
    }
    return units;
  }

  private HoodieData<HoodieRecord> toRecords(List<FileUnit> units, HoodieIndexDefinition indexDefinition) {
    if (units.isEmpty()) {
      return engineContext.emptyHoodieData();
    }
    String column = indexDefinition.getSourceFields().get(0);
    double denseRatio = Double.parseDouble(indexDefinition.getIndexOptions().getOrDefault(FullTextIndexUtils.OPTION_DENSE_RATIO,
        String.valueOf(FullTextIndexUtils.DEFAULT_DENSE_RATIO)));
    String indexPartition = indexDefinition.getIndexName();
    StorageConfiguration<?> storageConf = dataTableMetaClient.getStorageConf();
    String basePath = dataTableMetaClient.getBasePath().toString();
    HoodieFileFormat baseFileFormat = dataTableMetaClient.getTableConfig().getBaseFileFormat();
    int parallelism = Math.max(1, Math.min(units.size(), dataTableWriteConfig.getMetadataConfig().getSecondaryIndexParallelism()));
    return engineContext.parallelize(units, parallelism).flatMap(unit ->
        recordsForUnit(unit, basePath, storageConf, baseFileFormat, column, denseRatio, indexPartition).iterator());
  }

  /**
   * Tokenizes one data file's indexed column in row order and returns its presence and positions entries,
   * or tombstones for them when the unit is being retired.
   */
  static List<HoodieRecord> recordsForUnit(FileUnit unit, String basePath, StorageConfiguration<?> storageConf,
                                           HoodieFileFormat baseFileFormat, String column, double denseRatio,
                                           String indexPartition) {
    StoragePath path = new StoragePath(basePath, unit.relativePath);
    Map<String, RoaringBitmap> postings = new LinkedHashMap<>();
    long rowCount = 0;
    boolean covered = true;
    try {
      HoodieStorage storage = HoodieStorageUtils.getStorage(path, storageConf);
      if (unit.retire && !storage.exists(path)) {
        // Readers only prune units whose marker they see, so retiring the marker alone is enough.
        return Collections.singletonList(HoodieMetadataPayload.createFullTextIndexRecord(
            FullTextIndexUtils.markerKey(unit.fileId, unit.unit), indexPartition, 0L, 0L, false, null, true));
      }
      HoodieAvroFileReader reader = (HoodieAvroFileReader) HoodieIOFactory.getIOFactory(storage)
          .getReaderFactory(HoodieRecord.HoodieRecordType.AVRO)
          .getFileReader(new HoodieConfig(), path, baseFileFormat, Option.empty());
      try {
        HoodieSchema fileSchema = reader.getSchema();
        if (fileSchema.toAvroSchema().getField(column) == null) {
          // The column may exist under another name in this file (a rename with schema on read), so the file
          // is left uncovered: no marker, never pruned.
          covered = false;
          rowCount = reader.getTotalRecords();
        } else {
          HoodieSchema projected = HoodieSchemaUtils.projectSchema(fileSchema, Collections.singletonList(column));
          try (ClosableIterator<IndexedRecord> iterator = reader.getIndexedRecordIterator(fileSchema, projected)) {
            while (iterator.hasNext()) {
              IndexedRecord record = iterator.next();
              Schema.Field field = record.getSchema().getField(column);
              Object value = field == null ? null : record.get(field.pos());
              if (value != null) {
                int position = (int) rowCount;
                for (String token : FullTextIndexUtils.tokenize(value.toString())) {
                  postings.computeIfAbsent(token, t -> new RoaringBitmap()).add(position);
                }
              }
              rowCount++;
            }
          }
        }
      } finally {
        reader.close();
      }
    } catch (IOException e) {
      throw new HoodieIOException("Failed to build full-text index entries for " + path, e);
    }

    List<HoodieRecord> records = new ArrayList<>(postings.size() * 2 + 1);
    if (covered || unit.retire) {
      records.add(HoodieMetadataPayload.createFullTextIndexRecord(
          FullTextIndexUtils.markerKey(unit.fileId, unit.unit), indexPartition, rowCount, 0L, false, null, unit.retire));
    }
    for (Map.Entry<String, RoaringBitmap> posting : postings.entrySet()) {
      String term = posting.getKey();
      long cardinality = posting.getValue().getLongCardinality();
      boolean dense = cardinality > rowCount * denseRatio;
      records.add(HoodieMetadataPayload.createFullTextIndexRecord(
          FullTextIndexUtils.presenceKey(term, unit.fileId, unit.unit), indexPartition, rowCount, cardinality, dense, null, unit.retire));
      if (dense) {
        continue;
      }
      if (unit.retire) {
        records.add(HoodieMetadataPayload.createFullTextIndexRecord(
            FullTextIndexUtils.positionsKey(term, unit.fileId, unit.unit), indexPartition, rowCount, cardinality, false, null, true));
      } else {
        records.add(HoodieMetadataPayload.createFullTextIndexRecord(
            FullTextIndexUtils.positionsKey(term, unit.fileId, unit.unit), indexPartition, rowCount, cardinality, false,
            FullTextIndexUtils.serialize(posting.getValue()), false));
      }
    }
    return records;
  }

  /**
   * One data file to index, or to retire.
   */
  @AllArgsConstructor
  static class FileUnit implements Serializable {
    private static final long serialVersionUID = 1L;
    final String relativePath;
    final String fileId;
    final String unit;
    final boolean retire;
  }
}
