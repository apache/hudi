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

import org.apache.hudi.avro.HoodieAvroReaderContext;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.data.HoodieListPairData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.function.SerializableBiFunction;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.function.SerializableFunctionUnchecked;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.table.read.FileGroupReaderSchemaHandler;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.table.read.buffer.FileGroupRecordBufferLoader;
import org.apache.hudi.common.table.read.buffer.ReusableFileGroupRecordBufferLoader;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.HoodieDataUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.ClosableSortedDedupingIterator;
import org.apache.hudi.common.util.collection.CloseableFilterIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.EmptyIterator;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.expression.BindVisitor;
import org.apache.hudi.expression.Expression;
import org.apache.hudi.expression.Literal;
import org.apache.hudi.expression.Predicate;
import org.apache.hudi.expression.Predicates;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.io.storage.HoodieAvroFileReader;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.common.config.HoodieMetadataConfig.DEFAULT_METADATA_ENABLE_FULL_SCAN_LOG_FILES;
import static org.apache.hudi.common.util.ValidationUtils.checkState;
import static org.apache.hudi.metadata.HoodieMetadataPayload.KEY_FIELD_NAME;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_BLOOM_FILTERS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_FILES;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.existingIndexVersionOrDefault;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getFileSystemViewForMetadataTable;
import static org.apache.hudi.metadata.MetadataPartitionType.RECORD_INDEX;

/**
 * Table metadata provided by an internal DFS backed Hudi metadata table.
 */
public class HoodieBackedTableMetadata extends BaseTableMetadata {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieBackedTableMetadata.class);
  private static final Schema SCHEMA = HoodieAvroUtils.addMetadataFields(HoodieMetadataRecord.getClassSchema());

  private final String metadataBasePath;
  private final HoodieDataCleanupManager dataCleanupManager = new HoodieDataCleanupManager();

  private HoodieTableMetaClient metadataMetaClient;
  private Set<String> validInstantTimestamps = null;
  private HoodieTableFileSystemView metadataFileSystemView;
  // should we reuse the open file handles, across calls
  private final boolean reuse;
  private final transient Map<HoodieFileGroupId, Pair<HoodieAvroFileReader, ReusableFileGroupRecordBufferLoader<IndexedRecord>>> reusableFileReaders = new ConcurrentHashMap<>();

  // Latest file slices in the metadata partitions
  private final Map<String, List<FileSlice>> partitionFileSliceMap = new ConcurrentHashMap<>();

  private final Map<String, List<FileSlice>> partitionedRLIFileSliceMap = new ConcurrentHashMap<>();

  public HoodieBackedTableMetadata(HoodieEngineContext engineContext,
                                   HoodieStorage storage,
                                   HoodieMetadataConfig metadataConfig,
                                   String datasetBasePath) {
    this(engineContext, storage, metadataConfig, datasetBasePath, false);
  }

  public HoodieBackedTableMetadata(HoodieEngineContext engineContext,
                                   HoodieStorage storage,
                                   HoodieMetadataConfig metadataConfig,
                                   String datasetBasePath, boolean reuse) {
    super(engineContext, storage, metadataConfig, datasetBasePath);
    this.reuse = reuse;
    this.metadataBasePath = HoodieTableMetadata.getMetadataTableBasePath(dataBasePath.toString());

    initIfNeeded();
  }

  private void initIfNeeded() {
    if (!isMetadataTableInitialized) {
      if (!HoodieTableMetadata.isMetadataTable(metadataBasePath)) {
        LOG.info("Metadata table is disabled.");
      }
    } else if (this.metadataMetaClient == null) {
      try {
        this.metadataMetaClient = HoodieTableMetaClient.builder()
            .setStorage(storage)
            .setBasePath(metadataBasePath)
            .build();
      } catch (TableNotFoundException e) {
        LOG.warn("Metadata table was not found at path {}", metadataBasePath);
        this.isMetadataTableInitialized = false;
        this.metadataMetaClient = null;
        this.metadataFileSystemView = null;
        this.validInstantTimestamps = null;
      } catch (Exception e) {
        LOG.error("Failed to initialize metadata table at path {}", metadataBasePath, e);
        this.isMetadataTableInitialized = false;
        this.metadataMetaClient = null;
        this.metadataFileSystemView = null;
        this.validInstantTimestamps = null;
      }
    }
  }

  @Override
  protected Option<HoodieMetadataPayload> readFilesIndexRecords(String key, String partitionName) {
    HoodiePairData<String, HoodieMetadataPayload> recordsData = readIndexRecordsWithKeys(
        HoodieListData.eager(Collections.singletonList(new FilesIndexRawKey(key))), partitionName);
    try {
      List<HoodieMetadataPayload> records = recordsData.values().collectAsList();
      ValidationUtils.checkArgument(records.size() <= 1, () -> "Found more than 1 record for record key " + key);
      return records.isEmpty() ? Option.empty() : Option.ofNullable(records.get(0));
    } finally {
      recordsData.unpersistWithDependencies();
    }
  }

  @Override
  public List<String> getPartitionPathWithPathPrefixUsingFilterExpression(List<String> relativePathPrefixes,
                                                                          Types.RecordType partitionFields,
                                                                          Expression expression) throws IOException {
    Expression boundedExpr = expression.accept(new BindVisitor(partitionFields, caseSensitive));
    List<String> selectedPartitionPaths = getPartitionPathWithPathPrefixes(relativePathPrefixes);

    // Can only prune partitions if the number of partition levels matches partition fields
    // Here we'll check the first selected partition to see whether the numbers match.
    if (hiveStylePartitioningEnabled
        && getPathPartitionLevel(partitionFields, selectedPartitionPaths.get(0)) == partitionFields.fields().size()) {
      return selectedPartitionPaths.stream()
          .filter(p ->
              (boolean) boundedExpr.eval(extractPartitionValues(partitionFields, p, urlEncodePartitioningEnabled)))
          .collect(Collectors.toList());
    }

    return selectedPartitionPaths;
  }

  @Override
  public List<String> getPartitionPathWithPathPrefixes(List<String> relativePathPrefixes) throws IOException {
    // TODO: consider skipping this method for non-partitioned table and simplify the checks
    return getAllPartitionPaths().stream()
        .filter(p -> relativePathPrefixes.stream().anyMatch(relativePathPrefix ->
            // Partition paths stored in metadata table do not have the slash at the end.
            // If the relativePathPrefix is empty, return all partition paths;
            // else if the relative path prefix is the same as the path, this is an exact match;
            // else, we need to make sure the path is a subdirectory of relativePathPrefix, by
            // checking if the path starts with relativePathPrefix appended by a slash ("/").
            StringUtils.isNullOrEmpty(relativePathPrefix)
                || p.equals(relativePathPrefix) || p.startsWith(relativePathPrefix + "/")))
        .collect(Collectors.toList());
  }

  @Override
  public HoodieData<HoodieRecord<HoodieMetadataPayload>> getRecordsByKeyPrefixes(
      HoodieData<? extends RawKey> rawKeys,
      String partitionName,
      boolean shouldLoadInMemory) {
    // Apply key encoding
    List<String> sortedKeyPrefixes = new ArrayList<>(rawKeys.map(key -> key.encode()).collectAsList());
    // Sort the prefixes so that keys are looked up in order
    // Sort must come after encoding.
    Collections.sort(sortedKeyPrefixes);

    // NOTE: Since we partition records to a particular file-group by full key, we will have
    //       to scan all file-groups for all key-prefixes as each of these might contain some
    //       records matching the key-prefix
    List<FileSlice> partitionFileSlices = partitionFileSliceMap.computeIfAbsent(partitionName,
        k -> HoodieTableMetadataUtil.getPartitionLatestMergedFileSlices(metadataMetaClient, getMetadataFileSystemView(), partitionName));
    checkState(!partitionFileSlices.isEmpty(), () -> "Number of file slices for partition " + partitionName + " should be > 0");

    return (shouldLoadInMemory ? HoodieListData.lazy(partitionFileSlices) :
        getEngineContext().parallelize(partitionFileSlices))
        .flatMap(
            (SerializableFunction<FileSlice, Iterator<HoodieRecord<HoodieMetadataPayload>>>) fileSlice ->
                readSliceAndFilterByKeysIntoList(partitionName, sortedKeyPrefixes, fileSlice,
                    metadataRecord -> {
                      HoodieMetadataPayload payload = new HoodieMetadataPayload(Option.of(metadataRecord));
                      String rowKey = payload.key != null ? payload.key : metadataRecord.get(KEY_FIELD_NAME).toString();
                      HoodieKey key = new HoodieKey(rowKey, partitionName);
                      return new HoodieAvroRecord<>(key, payload);
                    }, false))
        .filter(r -> !r.getData().isDeleted());
  }

  private static TreeSet<String> getDistinctSortedKeysForSingleSlice(HoodieData<String> keys) {
    List<String> keysList = keys.collectAsList();
    return new TreeSet<>(keysList);
  }

  /**
   * All keys to be looked up go through the following steps:
   * 1. [encode] escape/encode the key if needed
   * 2. [hash to file group] compute the hash of the key to
   * 3. [lookup within file groups] lookup the key in the file group
   * 4. the record is returned
   */
  private HoodieData<HoodieRecord<HoodieMetadataPayload>> lookupIndexRecords(HoodieData<String> keys, String partitionName, List<FileSlice> fileSlices,
                                                                             Option<String> dataTablePartition) {
    if (dataTablePartition.isPresent()) {
      // assume is partitioned rli if a data table partition name is provided
      // filter to only the files in the partition
      List<FileSlice> fileSlicesForDataPartition = fileSlices.stream()
          .filter(fileSlice -> HoodieTableMetadataUtil.getDataTablePartitionNameFromFileGroupName(fileSlice.getFileId()).equals(dataTablePartition.get()))
          .collect(Collectors.toList());
      // all keys will be from the same shard index so just calculate the first key and reduce partitionFileSlices to 1
      TreeSet<String> distinctSortedKeys = getDistinctSortedKeysForSingleSlice(keys);
      int fileGroupIndex = HoodieTableMetadataUtil.mapRecordKeyToFileGroupIndex(distinctSortedKeys.stream().findFirst().get(), fileSlicesForDataPartition.size());
      return readSliceAndFilterByKeysIntoList(partitionName, distinctSortedKeys, fileSlicesForDataPartition.get(fileGroupIndex), false);
    } else if (partitionName.equals(RECORD_INDEX.getPartitionPath()) && !fileSlices.isEmpty() && HoodieTableMetadataUtil.verifyRLIFile(fileSlices.get(0).getFileId(), true)) {
      if (keys.isEmpty()) {
        return HoodieListData.lazy(Collections.emptyList());
      }
      throw new IllegalArgumentException("File pruning with partitioned rli has not yet been implemented");
    }
    boolean isSecondaryIndex = MetadataPartitionType.fromPartitionPath(partitionName).equals(MetadataPartitionType.SECONDARY_INDEX);
    final int numFileSlices = fileSlices.size();
    if (numFileSlices == 1) {
      return readSliceAndFilterByKeysIntoList(partitionName, getDistinctSortedKeysForSingleSlice(keys), fileSlices.get(0), !isSecondaryIndex);
    }

    // For SI v2, there are 2 cases require different implementation:
    // SI write path concatenates secKey$recordKey, the secKey needs extracted for hashing;
    // SI read path gives secKey only, no need for secKey extraction.
    SerializableBiFunction<String, Integer, Integer> mappingFunction = HoodieTableMetadataUtil::mapRecordKeyToFileGroupIndex;
    keys = repartitioningIfNeeded(keys, partitionName, numFileSlices, mappingFunction);
    HoodiePairData<Integer, String> persistedInitialPairData = keys
        // Tag key with file group index
        .mapToPair(encodedKey -> {
          return new ImmutablePair<>(mappingFunction.apply(encodedKey, numFileSlices), encodedKey);
        });
    persistedInitialPairData.persist("MEMORY_AND_DISK_SER");
    dataCleanupManager.trackPersistedData(persistedInitialPairData);

    // Use the new processValuesOfTheSameShards API instead of explicit rangeBasedRepartitionForEachKey
    SerializableFunction<Iterator<String>, Iterator<HoodieRecord<HoodieMetadataPayload>>> processFunction =
        sortedKeys -> {
          List<String> keysList = new ArrayList<>();
          // Decorate with sorted stream deduplication.
          try (ClosableSortedDedupingIterator<String> distinctSortedKeyIter = new ClosableSortedDedupingIterator<>(sortedKeys)) {
            if (!distinctSortedKeyIter.hasNext()) {
              return Collections.emptyIterator();
            }
            distinctSortedKeyIter.forEachRemaining(keysList::add);
          }
          FileSlice fileSlice = fileSlices.get(mappingFunction.apply(keysList.get(0), numFileSlices));
          return lookupRecordsItr(partitionName, keysList, fileSlice, !isSecondaryIndex);
        };
    List<Integer> keySpace = IntStream.range(0, numFileSlices).boxed().collect(Collectors.toList());
    return getEngineContext().mapGroupsByKey(persistedInitialPairData, processFunction, keySpace, true);
  }

  /**
   * Reads record keys from record-level index. Deleted records are filtered out.
   * <p>
   * If the Metadata Table is not enabled, an exception is thrown to distinguish this from the absence of the key.
   *
   * @param recordKeys List of mapping from keys to the record location.
   */
  @Override
  public HoodiePairData<String, HoodieRecordGlobalLocation> readRecordIndexLocationsWithKeys(HoodieData<String> recordKeys) {
    return readRecordIndexLocationsWithKeys(recordKeys, Option.empty());
  }

  @Override
  public HoodiePairData<String, HoodieRecordGlobalLocation> readRecordIndexLocationsWithKeys(HoodieData<String> recordKeys, Option<String> dataTablePartition) {
    // If record index is not initialized yet, we cannot return an empty result here unlike the code for reading from other
    // indexes. This is because results from this function are used for upserts and returning an empty result here would lead
    // to existing records being inserted again causing duplicates.
    // The caller is required to check for record index existence in MDT before calling this method.
    ValidationUtils.checkState(dataMetaClient.getTableConfig().isMetadataPartitionAvailable(RECORD_INDEX),
        "Record index is not initialized in MDT");

    return dataCleanupManager.ensureDataCleanupOnException(v -> {
      // TODO [HUDI-9544]: Metric does not work for rdd based API due to lazy evaluation.
      HoodieData<RecordIndexRawKey> rawKeys = recordKeys.map(RecordIndexRawKey::new);
      return readIndexRecordsWithKeys(rawKeys, MetadataPartitionType.RECORD_INDEX.getPartitionPath(), dataTablePartition)
          .mapToPair((Pair<String, HoodieMetadataPayload> p) -> Pair.of(p.getLeft(), p.getRight().getRecordGlobalLocation()));
    });
  }

  /**
   * Reads record keys from record-level index. Deleted records are filtered out.
   * <p>
   * If the Metadata Table is not enabled, an exception is thrown to distinguish this from the absence of the key.
   *
   * @param recordKeys List of locations of record for the record keys.
   */
  @Override
  public HoodieData<HoodieRecordGlobalLocation> readRecordIndexLocations(HoodieData<String> recordKeys) {
    // If record index is not initialized yet, we cannot return an empty result here unlike the code for reading from other
    // indexes. This is because results from this function are used for upserts and returning an empty result here would lead
    // to existing records being inserted again causing duplicates.
    // The caller is required to check for record index existence in MDT before calling this method.
    ValidationUtils.checkState(dataMetaClient.getTableConfig().isMetadataPartitionAvailable(RECORD_INDEX),
        "Record index is not initialized in MDT");

    return dataCleanupManager.ensureDataCleanupOnException(v -> {
      HoodieData<RecordIndexRawKey> rawKeys = recordKeys.map(RecordIndexRawKey::new);
      return readIndexRecords(rawKeys, RECORD_INDEX.getPartitionPath(), Option.empty())
          .map(r -> r.getData().getRecordGlobalLocation());
    });
  }

  /**
   * Get record-location using secondary-index and record-index. Deleted records are filtered out.
   * <p>
   * If the Metadata Table is not enabled, an exception is thrown to distinguish this from the absence of the key.
   *
   * @param secondaryKeys The list of secondary keys to read
   */
  @Override
  public HoodiePairData<String, HoodieRecordGlobalLocation> readSecondaryIndexLocationsWithKeys(HoodieData<String> secondaryKeys, String partitionName) {
    HoodieIndexVersion indexVersion = existingIndexVersionOrDefault(partitionName, dataMetaClient);

    return dataCleanupManager.ensureDataCleanupOnException(v -> {
      if (indexVersion.equals(HoodieIndexVersion.V1)) {
        return readSecondaryIndexLocationsWithKeysV1(secondaryKeys, partitionName);
      } else if (indexVersion.equals(HoodieIndexVersion.V2)) {
        return readSecondaryIndexLocationsWithKeysV2(secondaryKeys, partitionName);
      } else {
        throw new IllegalArgumentException("readSecondaryIndex does not support index with version " + indexVersion);
      }
    });
  }

  /**
   * Get record-location using secondary-index and record-index. Deleted records are filtered out.
   * <p>
   * If the Metadata Table is not enabled, an exception is thrown to distinguish this from the absence of the key.
   *
   * @param secondaryKeys The list of secondary keys to read
   */
  @Override
  public HoodieData<HoodieRecordGlobalLocation> readSecondaryIndexLocations(HoodieData<String> secondaryKeys, String partitionName) {
    HoodieIndexVersion indexVersion = existingIndexVersionOrDefault(partitionName, dataMetaClient);

    return dataCleanupManager.ensureDataCleanupOnException(v -> {
      if (indexVersion.equals(HoodieIndexVersion.V1)) {
        return readSecondaryIndexLocationsWithKeysV1(secondaryKeys, partitionName).values();
      } else if (indexVersion.equals(HoodieIndexVersion.V2)) {
        return readRecordIndexLocations(readSecondaryIndexDataTableRecordKeysV2(secondaryKeys, partitionName));
      } else {
        throw new IllegalArgumentException("readSecondaryIndexResult does not support index with version " + indexVersion);
      }
    });
  }

  @Override
  public HoodiePairData<String, HoodieMetadataPayload> readIndexRecordsWithKeys(
      HoodieData<? extends RawKey> rawKeys, String partitionName) {
    return readIndexRecordsWithKeys(rawKeys, partitionName, Option.empty());
  }

  @Override
  public HoodiePairData<String, HoodieMetadataPayload> readIndexRecordsWithKeys(
      HoodieData<? extends RawKey> rawKeys, String partitionName, Option<String> dataTablePartition) {
    return readIndexRecords(rawKeys, partitionName, dataTablePartition)
        .mapToPair(record -> Pair.of(record.getRecordKey(), record.getData()));
  }

  public HoodieData<String> readSecondaryIndexDataTableRecordKeysV2(HoodieData<String> secondaryKeys, String partitionName) {
    return dataCleanupManager.ensureDataCleanupOnException(v -> {
      HoodieData<SecondaryIndexPrefixRawKey> rawKeys = secondaryKeys.map(SecondaryIndexPrefixRawKey::new);
      return readIndexRecordsWithKeys(rawKeys, partitionName, Option.empty())
            .map(hoodieRecord -> SecondaryIndexKeyUtils.getRecordKeyFromSecondaryIndexKey(hoodieRecord.getLeft()));
    });
  }

  private HoodiePairData<String, HoodieRecordGlobalLocation> readSecondaryIndexLocationsWithKeysV2(HoodieData<String> secondaryKeys, String partitionName) {
    return readRecordIndexLocationsWithKeys(readSecondaryIndexDataTableRecordKeysV2(secondaryKeys, partitionName));
  }

  private HoodiePairData<String, HoodieRecordGlobalLocation> readSecondaryIndexLocationsWithKeysV1(HoodieData<String> secondaryKeys, String partitionName) {
    // For secondary index v1 we keep the old implementation.
    ValidationUtils.checkState(secondaryKeys instanceof HoodieListData, "readSecondaryIndex only support HoodieListData at the moment");
    ValidationUtils.checkState(dataMetaClient.getTableConfig().isMetadataPartitionAvailable(RECORD_INDEX),
        "Record index is not initialized in MDT");
    ValidationUtils.checkState(
        dataMetaClient.getTableConfig().getMetadataPartitions().contains(partitionName),
        () -> "Secondary index is not initialized in MDT for: " + partitionName);
    // Fetch secondary-index records
    Map<String, Set<String>> secondaryKeyRecords = HoodieDataUtils.collectPairDataAsMap(
        readSecondaryIndexDataTableRecordKeysWithKeys(HoodieListData.eager(secondaryKeys.collectAsList()), partitionName));
    // Now collect the record-keys and fetch the RLI records
    List<String> recordKeys = new ArrayList<>();
    secondaryKeyRecords.values().forEach(recordKeys::addAll);
    return readRecordIndexLocationsWithKeys(HoodieListData.eager(recordKeys));
  }

  protected HoodieData<HoodieRecord<HoodieMetadataPayload>> readIndexRecords(HoodieData<? extends RawKey> rawKeys,
                                                                             String partitionName,
                                                                             Option<String> dataTablePartition) {
    List<FileSlice> fileSlices = partitionFileSliceMap.computeIfAbsent(partitionName,
        k -> HoodieTableMetadataUtil.getPartitionLatestMergedFileSlices(metadataMetaClient, getMetadataFileSystemView(), partitionName));
    checkState(!fileSlices.isEmpty(), "No file slices found for partition: " + partitionName);

    // Convert RawKey to String using encode()
    HoodieData<String> keys = rawKeys.map(key -> key.encode());
    return lookupIndexRecords(keys, partitionName, fileSlices, dataTablePartition);
  }

  // When testing we noticed that the parallelism can be very low which hurts the performance. so we should start with a reasonable
  // level of parallelism in that case.
  private HoodieData<String> repartitioningIfNeeded(
      HoodieData<String> keys, String partitionName, int numFileSlices, SerializableBiFunction<String, Integer, Integer> mappingFunction) {
    if (keys instanceof HoodieListData) {
      int parallelism;
      parallelism = (int) keys.map(k -> mappingFunction.apply(k, numFileSlices)).distinct().count();
      // In case of empty lookup set, we should avoid RDD with 0 partitions.
      parallelism = Math.max(parallelism, 1);
      LOG.info("Repartitioning keys for partition {} from list data with parallelism: {}", partitionName, parallelism);
      keys = getEngineContext().parallelize(keys.collectAsList(), parallelism);
    } else if (keys.getNumPartitions() < metadataConfig.getRepartitionMinPartitionsThreshold()) {
      LOG.info("Repartitioning keys for partition {} to {} partitions", partitionName, metadataConfig.getRepartitionDefaultPartitions());
      keys = keys.repartition(metadataConfig.getRepartitionDefaultPartitions());
    }
    return keys;
  }

  private ClosableIterator<IndexedRecord> readSliceWithFilter(Predicate predicate, FileSlice fileSlice) throws IOException {
    Option<HoodieInstant> latestMetadataInstant =
        metadataMetaClient.getActiveTimeline().filterCompletedInstants().lastInstant();
    String latestMetadataInstantTime =
        latestMetadataInstant.map(HoodieInstant::requestedTime).orElse(SOLO_COMMIT_TIMESTAMP);
    // Only those log files which have a corresponding completed instant on the dataset should be read
    // This is because the metadata table is updated before the dataset instants are committed.
    Set<String> validInstantTimestamps = getValidInstantTimestamps();
    Option<InstantRange> instantRange = Option.of(InstantRange.builder()
        .rangeType(InstantRange.RangeType.EXACT_MATCH)
        .explicitInstants(validInstantTimestamps).build());

    // If reuse is enabled and full scan is allowed for the partition, we can reuse the file readers for base files and the reader context for the log files.
    Map<StoragePath, HoodieAvroFileReader> baseFileReaders = Collections.emptyMap();
    ReusableFileGroupRecordBufferLoader<IndexedRecord> recordBufferLoader = null;
    boolean shouldReuse = reuse && isFullScanAllowedForPartition(fileSlice.getPartitionPath());
    TypedProperties fileGroupReaderProps = ConfigUtils.buildFileGroupReaderProperties(metadataConfig, shouldReuse);
    if (shouldReuse) {
      Pair<HoodieAvroFileReader, ReusableFileGroupRecordBufferLoader<IndexedRecord>> readers =
          reusableFileReaders.computeIfAbsent(fileSlice.getFileGroupId(), fgId -> {
            try {
              HoodieAvroFileReader baseFileReader = null;
              if (fileSlice.getBaseFile().isPresent()) {
                HoodieConfig fileGroupReaderConfig = new HoodieConfig(fileGroupReaderProps);
                baseFileReader = (HoodieAvroFileReader) HoodieIOFactory.getIOFactory(storage).getReaderFactory(HoodieRecord.HoodieRecordType.AVRO)
                    .getFileReader(fileGroupReaderConfig, fileSlice.getBaseFile().get().getStoragePath(), metadataMetaClient.getTableConfig().getBaseFileFormat(), Option.empty());
              }
              return Pair.of(baseFileReader, buildReusableRecordBufferLoader(fileSlice, latestMetadataInstantTime, instantRange));
            } catch (IOException ex) {
              throw new HoodieIOException("Error opening readers for metadata table partition " + fileSlice.getPartitionPath(), ex);
            }
          });
      if (fileSlice.getBaseFile().isPresent()) {
        baseFileReaders = Collections.singletonMap(fileSlice.getBaseFile().get().getStoragePath(), readers.getLeft());
      }

      ValidationUtils.checkArgument(predicate instanceof Predicates.In, "For Metadata Table Reuse, key filter should be based on full keys");
      recordBufferLoader = readers.getRight();
    }

    HoodieReaderContext<IndexedRecord> readerContext = new HoodieAvroReaderContext(
        storageConf,
        metadataMetaClient.getTableConfig(),
        instantRange,
        Option.of(predicate),
        baseFileReaders,
        fileGroupReaderProps);

    HoodieFileGroupReader<IndexedRecord> fileGroupReader = HoodieFileGroupReader.<IndexedRecord>newBuilder()
        .withReaderContext(readerContext)
        .withHoodieTableMetaClient(metadataMetaClient)
        .withLatestCommitTime(latestMetadataInstantTime)
        .withFileSlice(fileSlice)
        .withDataSchema(SCHEMA)
        .withRequestedSchema(SCHEMA)
        .withProps(fileGroupReaderProps)
        .withRecordBufferLoader(recordBufferLoader)
        .withEnableOptimizedLogBlockScan(metadataConfig.isOptimizedLogBlocksScanEnabled())
        .build();

    return fileGroupReader.getClosableIterator();
  }

  private ReusableFileGroupRecordBufferLoader<IndexedRecord> buildReusableRecordBufferLoader(FileSlice fileSlice, String latestMetadataInstantTime,
                                                                                             Option<InstantRange> instantRangeOption) {
    // initialize without any filters
    HoodieReaderContext<IndexedRecord> readerContext = new HoodieAvroReaderContext(
        storageConf,
        metadataMetaClient.getTableConfig(),
        instantRangeOption,
        Option.empty(),
        ConfigUtils.buildFileGroupReaderProperties(metadataConfig, true));
    readerContext.initRecordMerger(metadataConfig.getProps());
    readerContext.setHasBootstrapBaseFile(false);
    readerContext.setHasLogFiles(fileSlice.hasLogFiles());
    readerContext.setSchemaHandler(new FileGroupReaderSchemaHandler<>(readerContext, SCHEMA, SCHEMA, Option.empty(),
        metadataConfig.getProps(), metadataMetaClient));
    readerContext.setShouldMergeUseRecordPosition(false);
    readerContext.setLatestCommitTime(latestMetadataInstantTime);
    return FileGroupRecordBufferLoader.createReusable(readerContext);
  }

  private HoodieData<HoodieRecord<HoodieMetadataPayload>> readSliceAndFilterByKeysIntoList(String partitionName,
                                                                                           Collection<String> sortedKeys,
                                                                                           FileSlice fileSlice,
                                                                                           boolean isFullKey) {
    return HoodieListData.lazy(lookupRecordsItr(partitionName, sortedKeys, fileSlice, isFullKey));
  }

  private ClosableIterator<Pair<String, HoodieRecord<HoodieMetadataPayload>>> readSliceAndFilterByKeys(String partitionName,
                                                                                                       List<String> sortedKeys,
                                                                                                       FileSlice fileSlice) {
    boolean isSecondaryIndex = MetadataPartitionType.fromPartitionPath(partitionName).equals(MetadataPartitionType.SECONDARY_INDEX);
    return new CloseableFilterIterator<>(
        readSliceAndFilterByKeysIntoList(partitionName, sortedKeys, fileSlice, metadataRecord -> {
          HoodieMetadataPayload payload = new HoodieMetadataPayload(Option.of(metadataRecord));
          String rowKey = payload.key != null ? payload.key : metadataRecord.get(KEY_FIELD_NAME).toString();
          HoodieKey hoodieKey = new HoodieKey(rowKey, partitionName);
          return Pair.of(rowKey, new HoodieAvroRecord<>(hoodieKey, payload));
        }, !isSecondaryIndex),
        p -> !p.getValue().getData().isDeleted());
  }

  private ClosableIterator<HoodieRecord<HoodieMetadataPayload>> lookupRecordsItr(String partitionName,
                                                                                 Collection<String> keys,
                                                                                 FileSlice fileSlice,
                                                                                 boolean isFullKey) {
    return new CloseableFilterIterator<>(
      readSliceAndFilterByKeysIntoList(partitionName, keys, fileSlice, metadataRecord -> {
        HoodieMetadataPayload payload = new HoodieMetadataPayload(Option.of(metadataRecord));
        return new HoodieAvroRecord<>(new HoodieKey(payload.key, partitionName), payload);
      }, isFullKey),
        r -> !r.getData().isDeleted());
  }

  /**
   * Lookup records and produce a lazy iterator of mapped HoodieRecords.
   * @param isFullKey If true, perform exact key match. If false, perform prefix match.
   */
  private <T> ClosableIterator<T> readSliceAndFilterByKeysIntoList(String partitionName,
                                                                   Collection<String> sortedKeys,
                                                                   FileSlice fileSlice,
                                                                   SerializableFunctionUnchecked<GenericRecord, T> transformer,
                                                                   boolean isFullKey) {
    // If no keys to lookup, we must return early, otherwise, the hfile lookup will return all records.
    if (sortedKeys.isEmpty()) {
      return new EmptyIterator<>();
    }
    try {
      Predicate predicate = buildPredicate(partitionName, sortedKeys, isFullKey);
      ClosableIterator<IndexedRecord> rawIterator = readSliceWithFilter(predicate, fileSlice);

      return new CloseableMappingIterator<>(rawIterator, record -> {
        GenericRecord metadataRecord = (GenericRecord) record;
        return transformer.apply(metadataRecord);
      });
    } catch (IOException e) {
      throw new HoodieIOException("Error merging records from metadata table for " + sortedKeys.size() + " keys", e);
    }
  }

  /**
   * Builds a predicate for querying metadata partitions based on the partition type and lookup strategy.
   *
   * Secondary index partitions are treated as a special case where the isFullKey parameter is ignored.
   * This is because secondary index lookups are neither pure prefix lookups nor full key lookups,
   * but rather use a customized key format: {secondary_key}+${record_key}. The lookup always uses
   * prefix matching to find all record keys associated with a given secondary key value.
   *
   * @param partitionName The name of the metadata partition
   * @param sortedKeys The list of keys to search for
   * @param isFullKey Whether to perform exact key matching (ignored for secondary index partitions)
   * @return A predicate for filtering records
   */
  static Predicate buildPredicate(String partitionName, Collection<String> sortedKeys, boolean isFullKey) {
    if (isFullKey) {
      ValidationUtils.checkArgument(
          !MetadataPartitionType.fromPartitionPath(partitionName).equals(MetadataPartitionType.SECONDARY_INDEX),
          "Secondary index should never use full-key lookup");
      // For non-secondary index with full key matching
      return Predicates.in(null, sortedKeys.stream()
          .map(Literal::from)
          .collect(Collectors.toList()));
    } else {
      // For non-secondary index with prefix matching
      return Predicates.startsWithAny(null, sortedKeys.stream()
          .map(Literal::from)
          .collect(Collectors.toList()));
    }
  }

  private Set<String> getValidInstantTimestamps() {
    if (validInstantTimestamps == null) {
      validInstantTimestamps = HoodieTableMetadataUtil.getValidInstantTimestamps(dataMetaClient, metadataMetaClient);
    }
    return validInstantTimestamps;
  }

  // NOTE: We're allowing eager full-scan of the log-files only for "files" partition.
  //       Other partitions (like "column_stats", "bloom_filters") will have to be fetched
  //       t/h point-lookups
  private boolean isFullScanAllowedForPartition(String partitionName) {
    switch (partitionName) {
      case PARTITION_NAME_FILES:
        return DEFAULT_METADATA_ENABLE_FULL_SCAN_LOG_FILES;

      case PARTITION_NAME_COLUMN_STATS:
      case PARTITION_NAME_BLOOM_FILTERS:
      default:
        return false;
    }
  }

  @Override
  public void close() {
    partitionFileSliceMap.clear();
    partitionedRLIFileSliceMap.clear();
    if (this.metadataFileSystemView != null) {
      this.metadataFileSystemView.close();
      this.metadataFileSystemView = null;
    }
    closeReusableReaders();
  }

  /**
   * Close and clear all the partitions readers.
   */
  private void closeReusableReaders() {
    if (reuse) {
      reusableFileReaders.values().forEach(pair -> {
        if (pair.getLeft() != null) {
          // Close the base file reader
          pair.getLeft().close();
        }
        if (pair.getRight() != null) {
          // Close the file group record buffer
          pair.getRight().close();
        }
      });
      reusableFileReaders.clear();
    }
  }

  public boolean enabled() {
    return isMetadataTableInitialized;
  }

  public HoodieTableMetaClient getMetadataMetaClient() {
    return metadataMetaClient;
  }

  public HoodieTableFileSystemView getMetadataFileSystemView() {
    if (metadataFileSystemView == null) {
      metadataFileSystemView = getFileSystemViewForMetadataTable(metadataMetaClient);
    }
    return metadataFileSystemView;
  }

  public Map<String, String> stats() {
    Set<String> allMetadataPartitionPaths = Arrays.stream(MetadataPartitionType.getValidValues()).map(MetadataPartitionType::getPartitionPath).collect(Collectors.toSet());
    return metrics.map(m -> m.getStats(true, metadataMetaClient, this, allMetadataPartitionPaths)).orElseGet(HashMap::new);
  }

  @Override
  public Option<String> getSyncedInstantTime() {
    if (metadataMetaClient != null) {
      Option<HoodieInstant> latestInstant = metadataMetaClient.getActiveTimeline().getDeltaCommitTimeline().filterCompletedInstants().lastInstant();
      if (latestInstant.isPresent()) {
        return Option.of(latestInstant.get().requestedTime());
      }
    }
    return Option.empty();
  }

  @Override
  public Option<String> getLatestCompactionTime() {
    if (metadataMetaClient != null) {
      Option<HoodieInstant> latestCompaction = metadataMetaClient.getActiveTimeline().getCommitAndReplaceTimeline().filterCompletedInstants().lastInstant();
      if (latestCompaction.isPresent()) {
        return Option.of(latestCompaction.get().requestedTime());
      }
    }
    return Option.empty();
  }

  @Override
  public void reset() {
    initIfNeeded();
    dataMetaClient.reloadActiveTimeline();
    if (metadataMetaClient != null) {
      metadataMetaClient.reloadActiveTimeline();
      if (metadataFileSystemView != null) {
        metadataFileSystemView.close();
      }
      metadataFileSystemView = null;
    }
    validInstantTimestamps = null;
    partitionFileSliceMap.clear();
    partitionedRLIFileSliceMap.clear();
    closeReusableReaders();
  }

  public List<FileSlice> getFilegroupsForPartition(MetadataPartitionType partition) {
    partitionFileSliceMap.computeIfAbsent(partition.getPartitionPath(),
        k -> HoodieTableMetadataUtil.getPartitionLatestMergedFileSlices(metadataMetaClient,
            getMetadataFileSystemView(), partition.getPartitionPath()));
    return partitionFileSliceMap.get(partition.getPartitionPath());
  }

  @Override
  public int getNumFileGroupsForPartition(MetadataPartitionType partition) {
    return getFilegroupsForPartition(partition).size();
  }

  @Override
  public Map<String, List<FileSlice>> getBucketizedFileGroupsForPartitionedRLI(MetadataPartitionType partition) {
    if (!partition.equals(RECORD_INDEX)) {
      throw new IllegalArgumentException("This method should only be used by RLI");
    }
    if (partitionedRLIFileSliceMap.isEmpty()) {
      List<FileSlice> fileSlices = getFilegroupsForPartition(partition);
      if (fileSlices.isEmpty()) {
        return Collections.emptyMap();
      }
      if (HoodieTableMetadataUtil.verifyRLIFile(fileSlices.get(0).getFileId(), false)) {
        throw new IllegalArgumentException("This method should only be used with partitioned RLI");
      }
      fileSlices.forEach(s -> partitionedRLIFileSliceMap.computeIfAbsent(HoodieTableMetadataUtil.getDataTablePartitionNameFromFileGroupName(s.getFileId()), x -> new ArrayList<>()).add(s));
    }
    return partitionedRLIFileSliceMap;
  }

  @Override
  public Map<Pair<String, StoragePath>, List<StoragePathInfo>> listPartitions(List<Pair<String, StoragePath>> partitionPathList) throws IOException {
    Map<String, Pair<String, StoragePath>> absoluteToPairMap = partitionPathList.stream()
        .collect(Collectors.toMap(
            pair -> pair.getRight().toString(),
            Function.identity()
        ));
    return getAllFilesInPartitions(
        partitionPathList.stream().map(pair -> pair.getRight().toString()).collect(Collectors.toList()))
        .entrySet().stream().collect(Collectors.toMap(
            entry -> absoluteToPairMap.get(entry.getKey()),
            Map.Entry::getValue
        ));
  }

  @Override
  public HoodiePairData<String, String> readSecondaryIndexDataTableRecordKeysWithKeys(HoodieData<String> secondaryKeys, String partitionName) {
    HoodieIndexVersion indexVersion = existingIndexVersionOrDefault(partitionName, dataMetaClient);

    return dataCleanupManager.ensureDataCleanupOnException(v -> {
      if (indexVersion.equals(HoodieIndexVersion.V1)) {
        return readSecondaryIndexDataTableRecordKeysWithKeysV1(secondaryKeys, partitionName);
      } else if (indexVersion.equals(HoodieIndexVersion.V2)) {
        return readSecondaryIndexDataTableRecordKeysWithKeysV2(secondaryKeys, partitionName);
      } else {
        throw new IllegalArgumentException("getSecondaryIndexRecords does not support index with version " + indexVersion);
      }
    });
  }

  private HoodiePairData<String, String> readSecondaryIndexDataTableRecordKeysWithKeysV1(HoodieData<String> keys, String partitionName) {
    if (keys.isEmpty()) {
      return HoodieListPairData.eager(Collections.emptyList());
    }

    HoodieData<SecondaryIndexPrefixRawKey> rawKeys = keys.map(SecondaryIndexPrefixRawKey::new);
    return getRecordsByKeyPrefixes(rawKeys, partitionName, false)
        .mapToPair(hoodieRecord -> SecondaryIndexKeyUtils.getSecondaryKeyRecordKeyPair(hoodieRecord.getRecordKey()));
  }

  private HoodiePairData<String, String> readSecondaryIndexDataTableRecordKeysWithKeysV2(HoodieData<String> secondaryKeys, String partitionName) {
    if (secondaryKeys.isEmpty()) {
      return HoodieListPairData.eager(Collections.emptyList());
    }

    HoodieData<SecondaryIndexPrefixRawKey> rawKeys = secondaryKeys.map(SecondaryIndexPrefixRawKey::new);
    return readIndexRecords(rawKeys, partitionName, Option.empty())
        .mapToPair(hoodieRecord -> SecondaryIndexKeyUtils.getSecondaryKeyRecordKeyPair(hoodieRecord.getRecordKey()));
  }
}
