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
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.data.HoodieListPairData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.InstantComparison;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.ClosableSortedDedupingIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.expression.BindVisitor;
import org.apache.hudi.expression.Expression;
import org.apache.hudi.expression.Literal;
import org.apache.hudi.expression.Predicate;
import org.apache.hudi.expression.Predicates;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.io.storage.HoodieSeekingFileReader;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.util.Transient;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.common.config.HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED;
import static org.apache.hudi.common.config.HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE;
import static org.apache.hudi.common.config.HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE;
import static org.apache.hudi.common.config.HoodieMemoryConfig.SPILLABLE_MAP_BASE_PATH;
import static org.apache.hudi.common.config.HoodieMetadataConfig.DEFAULT_METADATA_ENABLE_FULL_SCAN_LOG_FILES;
import static org.apache.hudi.common.util.ValidationUtils.checkState;
import static org.apache.hudi.metadata.HoodieMetadataPayload.KEY_FIELD_NAME;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_BLOOM_FILTERS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_FILES;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.existingIndexVersionOrDefault;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getFileSystemViewForMetadataTable;

/**
 * Table metadata provided by an internal DFS backed Hudi metadata table.
 */
public class HoodieBackedTableMetadata extends BaseTableMetadata {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieBackedTableMetadata.class);

  private final String metadataBasePath;

  private HoodieTableMetaClient metadataMetaClient;
  private Set<String> validInstantTimestamps = null;
  private HoodieTableFileSystemView metadataFileSystemView;
  // should we reuse the open file handles, across calls
  private final boolean reuse;

  // Readers for the latest file slice corresponding to file groups in the metadata partition
  private final Transient<Map<Pair<String, String>, Pair<HoodieSeekingFileReader<?>, HoodieMetadataLogRecordReader>>> partitionReaders =
      Transient.lazy(ConcurrentHashMap::new);

  // Latest file slices in the metadata partitions
  private final Map<String, List<FileSlice>> partitionFileSliceMap = new ConcurrentHashMap<>();

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
  protected Option<HoodieRecord<HoodieMetadataPayload>> getRecordByKey(String key, String partitionName) {
    Map<String, HoodieRecord<HoodieMetadataPayload>> recordsByKeys = getRecordsByKeysWithMapping(
        HoodieListData.eager(Collections.singletonList(key)), partitionName).collectAsMapWithOverwriteStrategy();
    return Option.ofNullable(recordsByKeys.get(key));
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
  public HoodieData<HoodieRecord<HoodieMetadataPayload>> getRecordsByKeyPrefixes(HoodieData<String> keyPrefixes,
                                                                                 String partitionName,
                                                                                 boolean shouldLoadInMemory) {
    ValidationUtils.checkState(keyPrefixes instanceof HoodieListData, "getRecordsByKeyPrefixes only support HoodieListData at the moment");
    // Sort the prefixes so that keys are looked up in order
    List<String> sortedKeyPrefixes = new ArrayList<>(keyPrefixes.collectAsList());
    Collections.sort(sortedKeyPrefixes);

    // NOTE: Since we partition records to a particular file-group by full key, we will have
    //       to scan all file-groups for all key-prefixes as each of these might contain some
    //       records matching the key-prefix
    List<FileSlice> partitionFileSlices = partitionFileSliceMap.computeIfAbsent(partitionName,
        k -> HoodieTableMetadataUtil.getPartitionLatestMergedFileSlices(metadataMetaClient, getMetadataFileSystemView(), partitionName));
    checkState(!partitionFileSlices.isEmpty(), "Number of file slices for partition " + partitionName + " should be > 0");

    return (shouldLoadInMemory ? HoodieListData.lazy(partitionFileSlices) :
        getEngineContext().parallelize(partitionFileSlices))
        .flatMap(
            (SerializableFunction<FileSlice, Iterator<HoodieRecord<HoodieMetadataPayload>>>) fileSlice -> {
              return getByKeyPrefixes(fileSlice, sortedKeyPrefixes, partitionName);
            });
  }

  private Iterator<HoodieRecord<HoodieMetadataPayload>> getByKeyPrefixes(FileSlice fileSlice,
                                                                         List<String> sortedKeyPrefixes,
                                                                         String partitionName) throws IOException {
    Option<HoodieInstant> latestMetadataInstant =
        metadataMetaClient.getActiveTimeline().filterCompletedInstants().lastInstant();
    String latestMetadataInstantTime =
        latestMetadataInstant.map(HoodieInstant::requestedTime).orElse(SOLO_COMMIT_TIMESTAMP);
    Schema schema = HoodieAvroUtils.addMetadataFields(HoodieMetadataRecord.getClassSchema());
    // Only those log files which have a corresponding completed instant on the dataset should be read
    // This is because the metadata table is updated before the dataset instants are committed.
    Set<String> validInstantTimestamps = getValidInstantTimestamps();
    InstantRange instantRange = InstantRange.builder()
        .rangeType(InstantRange.RangeType.EXACT_MATCH)
        .explicitInstants(validInstantTimestamps).build();
    HoodieReaderContext<IndexedRecord> readerContext = new HoodieAvroReaderContext(
        storageConf,
        metadataMetaClient.getTableConfig(),
        Option.of(instantRange),
        Option.of(transformKeyPrefixesToPredicate(sortedKeyPrefixes)));
    HoodieFileGroupReader<IndexedRecord> fileGroupReader = HoodieFileGroupReader.<IndexedRecord>newBuilder()
        .withReaderContext(readerContext)
        .withHoodieTableMetaClient(metadataMetaClient)
        .withLatestCommitTime(latestMetadataInstantTime)
        .withFileSlice(fileSlice)
        .withDataSchema(schema)
        .withRequestedSchema(schema)
        .withProps(buildFileGroupReaderProperties(metadataConfig))
        .withStart(0)
        .withLength(Long.MAX_VALUE)
        .withShouldUseRecordPosition(false)
        .build();
    ClosableIterator<IndexedRecord> it = fileGroupReader.getClosableIterator();
    return new CloseableMappingIterator<>(
        it,
        metadataRecord -> {
          HoodieMetadataPayload payload = new HoodieMetadataPayload(Option.of((GenericRecord) metadataRecord));
          String rowKey = payload.key != null
              ? payload.key : ((GenericRecord) metadataRecord).get(KEY_FIELD_NAME).toString();
          HoodieKey key = new HoodieKey(rowKey, partitionName);
          return new HoodieAvroRecord<>(key, payload);
        });
  }

  private Predicate transformKeysToPredicate(List<String> keys) {
    List<Expression> right = keys.stream().map(k -> Literal.from(k)).collect(Collectors.toList());
    return Predicates.in(null, right);
  }

  private Predicate transformKeyPrefixesToPredicate(List<String> keyPrefixes) {
    List<Expression> right = keyPrefixes.stream().map(kp -> Literal.from(kp)).collect(Collectors.toList());
    return Predicates.startsWithAny(null, right);
  }

  private HoodiePairData<String, HoodieRecord<HoodieMetadataPayload>> doLookupWithMapping(
      HoodieData<String> keys, String partitionName, List<FileSlice> fileSlices, boolean isSecondaryIndex) {

    final int numFileSlices = fileSlices.size();
    if (numFileSlices == 1) {
      TreeSet<String> distinctSortedKeys = new TreeSet<>(keys.collectAsList());
      return lookupRecordsWithMapping(partitionName, new ArrayList<>(distinctSortedKeys), fileSlices.get(0), !isSecondaryIndex);
    }

    HoodieIndexVersion indexVersion = getExistingHoodieIndexVersionOrDefault(partitionName, metadataMetaClient);
    keys = repartitioningIfNeeded(keys, partitionName, numFileSlices, indexVersion);

    if (keys instanceof HoodieListData) {
      Map<String, HoodieRecord<HoodieMetadataPayload>> result = new HashMap<>();
      List<String> keyList = keys.collectAsList();
      ArrayList<ArrayList<String>> partitionedKeys = partitionKeysByFileSlices(keyList, numFileSlices, partitionName, indexVersion);
      List<HoodiePairData<String, HoodieRecord<HoodieMetadataPayload>>> partialResults =
          getEngineContext().map(partitionedKeys, keysList -> {
            if (keysList.isEmpty()) {
              return HoodieListPairData.eager(Collections.emptyList());
            }
            int shardIndex = HoodieTableMetadataUtil.mapRecordKeyToFileGroupIndex(keysList.get(0), numFileSlices, partitionName, indexVersion);
            Collections.sort(keysList);
            return lookupRecordsWithMapping(partitionName, keysList, fileSlices.get(shardIndex), !isSecondaryIndex);
          }, partitionedKeys.size());

      partialResults.stream()
          .flatMap(p -> p.collectAsList().stream())
          .forEach(pair -> result.put(pair.getKey(), pair.getValue()));
      return HoodieListPairData.eagerMapKV(result);
    }

    keys = adaptiveSortDedupRepartition(keys, partitionName, numFileSlices, indexVersion);
    return keys.mapPartitions(iter -> {
      if (!iter.hasNext()) {
        return Collections.emptyIterator();
      }
      List<String> sortedKeys = new ArrayList<>();
      iter.forEachRemaining(sortedKeys::add);
      FileSlice fileSlice = fileSlices.get(
          HoodieTableMetadataUtil.mapRecordKeyToFileGroupIndex(sortedKeys.get(0), numFileSlices, partitionName, indexVersion));
      return lookupRecordsWithMappingIter(partitionName, sortedKeys, fileSlice, !isSecondaryIndex);
    }, false).mapToPair(e -> new ImmutablePair<>(e.getKey(), e.getValue()));
  }

  private HoodieData<HoodieRecord<HoodieMetadataPayload>> doLookupWithoutMapping(
      HoodieData<String> keys, String partitionName, List<FileSlice> fileSlices, boolean isSecondaryIndex) {

    final int numFileSlices = fileSlices.size();
    if (numFileSlices == 1) {
      TreeSet<String> distinctSortedKeys = new TreeSet<>(keys.collectAsList());
      return lookupRecordsWithoutMapping(partitionName, new ArrayList<>(distinctSortedKeys), fileSlices.get(0), !isSecondaryIndex);
    }

    HoodieIndexVersion indexVersion = getExistingHoodieIndexVersionOrDefault(partitionName, metadataMetaClient);
    keys = repartitioningIfNeeded(keys, partitionName, numFileSlices, indexVersion);

    if (keys instanceof HoodieListData) {
      List<String> keyList = keys.collectAsList();
      List<HoodieRecord<HoodieMetadataPayload>> result = new ArrayList<>();
      ArrayList<ArrayList<String>> partitionedKeys = partitionKeysByFileSlices(keyList, numFileSlices, partitionName, indexVersion);
      List<HoodieListData<HoodieRecord<HoodieMetadataPayload>>> partialResults =
          getEngineContext().map(partitionedKeys, keysList -> {
            if (keysList.isEmpty()) {
              return HoodieListData.eager(Collections.emptyList());
            }
            int shardIndex = HoodieTableMetadataUtil.mapRecordKeyToFileGroupIndex(keysList.get(0), numFileSlices, partitionName, indexVersion);
            return lookupRecordsWithoutMapping(
                partitionName, keysList.stream().sorted().distinct().collect(Collectors.toList()), fileSlices.get(shardIndex), !isSecondaryIndex);
          }, partitionedKeys.size());

      partialResults.forEach(data -> result.addAll(data.collectAsList()));
      return HoodieListData.eager(result);
    }

    keys = adaptiveSortDedupRepartition(keys, partitionName, numFileSlices, indexVersion);
    return keys.mapPartitions(iter -> {
      if (!iter.hasNext()) {
        return Collections.emptyIterator();
      }
      List<String> sortedKeys = new ArrayList<>();
      iter.forEachRemaining(sortedKeys::add);
      FileSlice fileSlice = fileSlices.get(
          HoodieTableMetadataUtil.mapRecordKeyToFileGroupIndex(sortedKeys.get(0), numFileSlices, partitionName, indexVersion));
      return lookupRecordsWithoutMappingIter(partitionName, sortedKeys, fileSlice, !isSecondaryIndex);
    }, false);
  }

  protected HoodieData<HoodieRecord<HoodieMetadataPayload>> readIndexWithoutMapping(
      HoodieData<String> keys, String partitionName) {
    if (keys instanceof HoodieListData && keys.isEmpty()) {
      return getEngineContext().emptyHoodieData();
    }

    List<FileSlice> fileSlices = partitionFileSliceMap.computeIfAbsent(partitionName,
        k -> HoodieTableMetadataUtil.getPartitionLatestMergedFileSlices(metadataMetaClient, getMetadataFileSystemView(), partitionName));
    checkState(!fileSlices.isEmpty(), "No file slices found for partition: " + partitionName);

    boolean isSecondaryIndex = MetadataPartitionType.SECONDARY_INDEX.isPartitionType(partitionName);
    return doLookupWithoutMapping(keys, partitionName, fileSlices, isSecondaryIndex);
  }

  @Override
  protected HoodiePairData<String, HoodieRecord<HoodieMetadataPayload>> getRecordsByKeysWithMapping(HoodieData<String> keys, String partitionName) {
    if (keys instanceof HoodieListData && keys.isEmpty()) {
      return getEngineContext().emptyHoodiePairData();
    }

    List<FileSlice> fileSlices = partitionFileSliceMap.computeIfAbsent(partitionName,
        k -> HoodieTableMetadataUtil.getPartitionLatestMergedFileSlices(metadataMetaClient, getMetadataFileSystemView(), partitionName));
    checkState(!fileSlices.isEmpty(), "No file slices found for partition: " + partitionName);

    boolean isSecondaryIndex = MetadataPartitionType.SECONDARY_INDEX.isPartitionType(partitionName);
    return doLookupWithMapping(keys, partitionName, fileSlices, isSecondaryIndex);
  }

  @Override
  public HoodieData<HoodieRecordGlobalLocation> readRecordIndexWithoutMapping(HoodieData<String> recordKeys) {
    // If record index is not initialized yet, we cannot return an empty result here unlike the code for reading from other
    // indexes. This is because results from this function are used for upserts and returning an empty result here would lead
    // to existing records being inserted again causing duplicates.
    // The caller is required to check for record index existence in MDT before calling this method.
    ValidationUtils.checkState(dataMetaClient.getTableConfig().isMetadataPartitionAvailable(MetadataPartitionType.RECORD_INDEX),
        "Record index is not initialized in MDT");
    return readIndexWithoutMapping(recordKeys, MetadataPartitionType.RECORD_INDEX.getPartitionPath())
        .map(r -> r.getData().getRecordGlobalLocation());
  }

  /**
   * Get record-location using secondary-index and record-index
   * <p>
   * If the Metadata Table is not enabled, an exception is thrown to distinguish this from the absence of the key.
   *
   * @param secondaryKeys The list of secondary keys to read
   */
  @Override
  public HoodiePairData<String, HoodieRecordGlobalLocation> readSecondaryIndexWithMapping(HoodieData<String> secondaryKeys, String partitionName) {
    ValidationUtils.checkState(secondaryKeys instanceof HoodieListData, "readSecondaryIndex only support HoodieListData at the moment");
    ValidationUtils.checkState(dataMetaClient.getTableConfig().isMetadataPartitionAvailable(MetadataPartitionType.RECORD_INDEX),
        "Record index is not initialized in MDT");
    ValidationUtils.checkState(
        dataMetaClient.getTableConfig().getMetadataPartitions().contains(partitionName),
        "Secondary index is not initialized in MDT for: " + partitionName);
    // Fetch secondary-index records
    Map<String, Set<String>> secondaryKeyRecords = getSecondaryIndexRecords(
        HoodieListData.eager(secondaryKeys.collectAsList()), partitionName).collectAsMapWithOverwriteStrategy();
    // Now collect the record-keys and fetch the RLI records
    List<String> recordKeys = new ArrayList<>();
    secondaryKeyRecords.values().forEach(recordKeys::addAll);
    return readRecordIndexWithMapping(HoodieListData.eager(recordKeys));
  }

  @Override
  public HoodieData<HoodieRecordGlobalLocation> readSecondaryIndexWithoutMapping(HoodieData<String> secondaryKeys, String partitionName) {
    HoodieIndexVersion indexVersion = getExistingHoodieIndexVersionOrDefault(partitionName, metadataMetaClient);
    ValidationUtils.checkState(indexVersion.greaterThanOrEquals(HoodieIndexVersion.SECONDARY_INDEX_TWO),
        "readSecondaryIndex only support HoodieListData at the moment");
    return readRecordIndexWithoutMapping(getRecordKeysFromSecondaryIndex(secondaryKeys, partitionName));
  }

  public HoodieData<String> getRecordKeysFromSecondaryIndex(HoodieData<String> secondaryKeys, String partitionName) {
    // don't dedup because we should dedup after the repartition, why?
    // getRecordsFast - prefix match the whole record
    return readIndexWithoutMapping(secondaryKeys, partitionName).map(
        hoodieRecord -> {
          // TODO: can we ever get any deleted data?
          if (hoodieRecord != null && !hoodieRecord.getData().isDeleted()) {
            // extract the record key part out of it.
            return SecondaryIndexKeyUtils.getRecordKeyFromSecondaryIndexKey(hoodieRecord.getRecordKey());
          }
          return null;
        }
    );
  }

  static HoodieData<String> adaptiveSortDedupRepartition(
      HoodieData<String> keys, String partitionName, int numFileSlices, HoodieIndexVersion tableVersion) {
    HoodiePairData<Integer, String> persistedInitialPairData = keys.filter(Objects::nonNull)
        // Tag key with file group index
        .mapToPair(recordKey -> new ImmutablePair<>(
            HoodieTableMetadataUtil.mapRecordKeyToFileGroupIndex(recordKey, numFileSlices, partitionName, tableVersion),
            recordKey));
    persistedInitialPairData.persist("MEMORY_AND_DISK_SER");
    // Split into dynamic number of partitions, it guarantees that
    // - keys in each partition is sorted
    // - each partition only looks up 1 file index group
    // - for partitions that look up the same file index group, the value range they look up does not overlap
    keys = persistedInitialPairData
        // Actions are triggered implicitly for sampling
        .rangeBasedRepartitionForEachKey(numFileSlices, 0.01, 10000, 42)
        .values()
        // Deduplicate sorted data, the lookup key set is prepared by unknown source so we should avoid making
        // assumptions.
        .mapPartitions(ClosableSortedDedupingIterator::new, true /* deduplicating data does not change data partitioning integrity */);
    return keys;
  }

  // When testing we noticed that the parallelism can be very low which hurts the performance. so we should start with a reasonable
  // level of parallelism in that case.
  private HoodieData<String> repartitioningIfNeeded(
      HoodieData<String> keys, String partitionName, int numFileSlices, HoodieIndexVersion version) {
    if (keys instanceof HoodieListData) {
      int parallelism = (int) keys.map(k ->
            HoodieTableMetadataUtil.mapRecordKeyToFileGroupIndex(k, numFileSlices, partitionName, version)).distinct().count();
      LOG.info("getRecordFast repartition HoodieListData to JavaRDD: exit, partitionName {}, num partitions: {}",
          partitionName, parallelism);
      keys = engineContext.parallelize(keys.collectAsList(), parallelism);
    } else if (keys.getNumPartitions() < 100) {
      LOG.info("getRecordFast repartition HoodieNonListData. partitionName {}, num partitions: {}", partitionName, 200);
      keys = keys.repartition(200);
    }
    return keys;
  }

  private static ArrayList<ArrayList<String>> partitionKeysByFileSlices(List<String> keys, int numFileSlices, String partitionName, HoodieIndexVersion version) {
    ArrayList<ArrayList<String>> partitionedKeys = new ArrayList<>(numFileSlices);
    for (int i = 0; i < numFileSlices; ++i) {
      partitionedKeys.add(new ArrayList<>());
    }
    keys.forEach(key -> {
      int shardIndex = HoodieTableMetadataUtil.mapRecordKeyToFileGroupIndex(key, numFileSlices, partitionName, version);
      partitionedKeys.get(shardIndex).add(key);
    });
    return partitionedKeys;
  }

  private HoodieFileGroupReader<IndexedRecord> buildFileGroupReader(List<String> sortedKeys,
                                                                    FileSlice fileSlice,
                                                                    boolean isFullKey) {
    Option<HoodieInstant> latestMetadataInstant =
        metadataMetaClient.getActiveTimeline().filterCompletedInstants().lastInstant();
    String latestMetadataInstantTime =
        latestMetadataInstant.map(HoodieInstant::requestedTime).orElse(SOLO_COMMIT_TIMESTAMP);
    Schema schema = HoodieAvroUtils.addMetadataFields(HoodieMetadataRecord.getClassSchema());
    // Only those log files which have a corresponding completed instant on the dataset should be read
    // This is because the metadata table is updated before the dataset instants are committed.
    Set<String> validInstantTimestamps = getValidInstantTimestamps();
    InstantRange instantRange = InstantRange.builder()
        .rangeType(InstantRange.RangeType.EXACT_MATCH)
        .explicitInstants(validInstantTimestamps).build();
    HoodieReaderContext<IndexedRecord> readerContext = new HoodieAvroReaderContext(
        storageConf,
        metadataMetaClient.getTableConfig(),
        Option.of(instantRange),
        Option.of(
            isFullKey
                ? transformKeysToPredicate(sortedKeys)
                : transformKeyPrefixesToPredicate(sortedKeys)));
    return HoodieFileGroupReader.<IndexedRecord>newBuilder()
        .withReaderContext(readerContext)
        .withHoodieTableMetaClient(metadataMetaClient)
        .withLatestCommitTime(latestMetadataInstantTime)
        .withFileSlice(fileSlice)
        .withDataSchema(schema)
        .withRequestedSchema(schema)
        .withProps(buildFileGroupReaderProperties(metadataConfig))
        .withStart(0)
        .withLength(Long.MAX_VALUE)
        .withShouldUseRecordPosition(false)
        .build();
  }

  private HoodiePairData<String, HoodieRecord<HoodieMetadataPayload>> lookupRecordsWithMapping(
      String partitionName,
      List<String> sortedKeys,
      FileSlice fileSlice,
      Boolean isFullKey) {
    Map<String, HoodieRecord<HoodieMetadataPayload>> map = new HashMap<>();
    try (ClosableIterator<Pair<String, HoodieRecord<HoodieMetadataPayload>>> iterator =
             lookupRecordsWithMappingIter(partitionName, sortedKeys, fileSlice, isFullKey)) {
      iterator.forEachRemaining(entry -> map.put(entry.getKey(), entry.getValue()));
    }
    return HoodieListPairData.eagerMapKV(map);
  }

  private HoodieListData<HoodieRecord<HoodieMetadataPayload>> lookupRecordsWithoutMapping(
      String partitionName,
      List<String> sortedKeys,
      FileSlice fileSlice,
      Boolean isFullKey) {
    List<HoodieRecord<HoodieMetadataPayload>> res = new ArrayList<>();
    try (ClosableIterator<HoodieRecord<HoodieMetadataPayload>> iterator =
             lookupRecordsWithoutMappingIter(partitionName, sortedKeys, fileSlice, isFullKey)) {
      iterator.forEachRemaining(entry -> res.add(entry));
    }
    return HoodieListData.eager(res);
  }

  private ClosableIterator<Pair<String, HoodieRecord<HoodieMetadataPayload>>> lookupRecordsWithMappingIter(
      String partitionName,
      List<String> sortedKeys,
      FileSlice fileSlice,
      Boolean isFullKey) {
    return lookupRecords(sortedKeys, fileSlice, isFullKey, metadataRecord -> {
      HoodieMetadataPayload payload = new HoodieMetadataPayload(Option.of(metadataRecord));
      String rowKey = payload.key != null ? payload.key : metadataRecord.get(KEY_FIELD_NAME).toString();
      HoodieKey hoodieKey = new HoodieKey(rowKey, partitionName);
      return Pair.of(rowKey, new HoodieAvroRecord<>(hoodieKey, payload));
    });
  }

  private ClosableIterator<HoodieRecord<HoodieMetadataPayload>> lookupRecordsWithoutMappingIter(
      String partitionName,
      List<String> keys,
      FileSlice fileSlice,
      Boolean isFullKey) {
    return lookupRecords(keys, fileSlice, isFullKey,
        metadataRecord -> {
          HoodieMetadataPayload payload = new HoodieMetadataPayload(Option.of(metadataRecord));
          return new HoodieAvroRecord<>(new HoodieKey(payload.key, partitionName), payload);
        });
  }

  /**
   * Lookup records and produce a lazy iterator of mapped HoodieRecords.
   */
  private <T> ClosableIterator<T> lookupRecords(
      List<String> sortedKeys,
      FileSlice fileSlice,
      Boolean isFullKey,
      RecordLookupTransformer<T> transformer) {

    try {
      HoodieFileGroupReader<IndexedRecord> fileGroupReader = buildFileGroupReader(sortedKeys, fileSlice, isFullKey);
      ClosableIterator<IndexedRecord> rawIterator = fileGroupReader.getClosableIterator();

      return new CloseableMappingIterator<>(rawIterator, record -> {
        GenericRecord metadataRecord = (GenericRecord) record;
        try {
          return transformer.apply(metadataRecord);
        } catch (IOException e) {
          throw new HoodieIOException("Error processing record with key " + new HoodieMetadataPayload(Option.of(metadataRecord)).key, e);
        }
      });
    } catch (IOException e) {
      throw new HoodieIOException("Error merging records from metadata table for " + sortedKeys.size() + " keys", e);
    }
  }

  // Functional interface for generic payload transformation
  @FunctionalInterface
  private interface RecordLookupTransformer<T> {
    T apply(GenericRecord payload) throws IOException;
  }

  private Set<String> getValidInstantTimestamps() {
    if (validInstantTimestamps == null) {
      validInstantTimestamps = HoodieTableMetadataUtil.getValidInstantTimestamps(dataMetaClient, metadataMetaClient);
    }
    return validInstantTimestamps;
  }

  public Pair<HoodieMetadataLogRecordReader, Long> getLogRecordScanner(List<HoodieLogFile> logFiles,
                                                                       String partitionName,
                                                                       Option<Boolean> allowFullScanOverride,
                                                                       Option<String> timeTravelInstant) {
    HoodieTimer timer = HoodieTimer.start();
    List<String> sortedLogFilePaths = logFiles.stream()
        .sorted(HoodieLogFile.getLogFileComparator())
        .map(o -> o.getPath().toString())
        .collect(Collectors.toList());

    // Only those log files which have a corresponding completed instant on the dataset should be read
    // This is because the metadata table is updated before the dataset instants are committed.
    Set<String> validInstantTimestamps = getValidInstantTimestamps();

    Option<HoodieInstant> latestMetadataInstant = metadataMetaClient.getActiveTimeline().filterCompletedInstants().lastInstant();
    String latestMetadataInstantTime = latestMetadataInstant.map(HoodieInstant::requestedTime).orElse(SOLO_COMMIT_TIMESTAMP);
    if (timeTravelInstant.isPresent()) {
      latestMetadataInstantTime = InstantComparison.minTimestamp(latestMetadataInstantTime, timeTravelInstant.get());
    }

    boolean allowFullScan = allowFullScanOverride.orElseGet(() -> isFullScanAllowedForPartition(partitionName));

    // Load the schema
    Schema schema = HoodieAvroUtils.addMetadataFields(HoodieMetadataRecord.getClassSchema());
    HoodieCommonConfig commonConfig = HoodieCommonConfig.newBuilder().fromProperties(metadataConfig.getProps()).build();
    HoodieMetadataLogRecordReader logRecordScanner = HoodieMetadataLogRecordReader.newBuilder()
        .withStorage(metadataMetaClient.getStorage())
        .withBasePath(metadataBasePath)
        .withLogFilePaths(sortedLogFilePaths)
        .withReaderSchema(schema)
        .withLatestInstantTime(latestMetadataInstantTime)
        .withMaxMemorySizeInBytes(metadataConfig.getMaxReaderMemory())
        .withBufferSize(metadataConfig.getMaxReaderBufferSize())
        .withSpillableMapBasePath(metadataConfig.getSplliableMapDir())
        .withDiskMapType(commonConfig.getSpillableDiskMapType())
        .withBitCaskDiskMapCompressionEnabled(commonConfig.isBitCaskDiskMapCompressionEnabled())
        .withLogBlockTimestamps(validInstantTimestamps)
        .enableFullScan(allowFullScan)
        .withPartition(partitionName)
        .withEnableOptimizedLogBlocksScan(metadataConfig.isOptimizedLogBlocksScanEnabled())
        .withTableMetaClient(metadataMetaClient)
        .build();

    Long logScannerOpenMs = timer.endTimer();
    LOG.info("Opened {} metadata log files (dataset instant={}, metadata instant={}) in {} ms",
        sortedLogFilePaths.size(), getLatestDataInstantTime(), latestMetadataInstantTime, logScannerOpenMs);
    return Pair.of(logRecordScanner, logScannerOpenMs);
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
    closePartitionReaders();
    partitionFileSliceMap.clear();
    if (this.metadataFileSystemView != null) {
      this.metadataFileSystemView.close();
      this.metadataFileSystemView = null;
    }
  }

  /**
   * Close the file reader and the record scanner for the given file slice.
   *
   * @param partitionFileSlicePair - Partition and FileSlice
   */
  private synchronized void close(Pair<String, String> partitionFileSlicePair) {
    Pair<HoodieSeekingFileReader<?>, HoodieMetadataLogRecordReader> readers =
        partitionReaders.get().remove(partitionFileSlicePair);
    closeReader(readers);
  }

  /**
   * Close and clear all the partitions readers.
   */
  private void closePartitionReaders() {
    for (Pair<String, String> partitionFileSlicePair : partitionReaders.get().keySet()) {
      close(partitionFileSlicePair);
    }
    partitionReaders.get().clear();
  }

  private void closeReader(Pair<HoodieSeekingFileReader<?>, HoodieMetadataLogRecordReader> readers) {
    if (readers != null) {
      try {
        if (readers.getKey() != null) {
          readers.getKey().close();
        }
        if (readers.getValue() != null) {
          readers.getValue().close();
        }
      } catch (Exception e) {
        throw new HoodieException("Error closing resources during metadata table merge", e);
      }
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
    // the cached reader has max instant time restriction, they should be cleared
    // because the metadata timeline may have changed.
    closePartitionReaders();
    partitionFileSliceMap.clear();
  }

  @Override
  public int getNumFileGroupsForPartition(MetadataPartitionType partition) {
    partitionFileSliceMap.computeIfAbsent(partition.getPartitionPath(),
        k -> HoodieTableMetadataUtil.getPartitionLatestMergedFileSlices(metadataMetaClient,
            getMetadataFileSystemView(), partition.getPartitionPath()));
    return partitionFileSliceMap.get(partition.getPartitionPath()).size();
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
  public HoodiePairData<String, Set<String>> getSecondaryIndexRecords(HoodieData<String> keys, String partitionName) {
    if (keys.isEmpty()) {
      return HoodieListPairData.eager(Collections.emptyList());
    }

    // As the first step, always convert keys to the escaped version.
    keys = keys.map(SecondaryIndexKeyUtils::escapeSpecialChars);
    List<Pair<String, String>> pairs = getRecordsByKeyPrefixes(keys, partitionName, false).map(record -> {
          if (!record.getData().isDeleted()) {
            String recordKey = SecondaryIndexKeyUtils.getRecordKeyFromSecondaryIndexKey(record.getRecordKey());
            String secondaryKey = SecondaryIndexKeyUtils.getSecondaryKeyFromSecondaryIndexKey(record.getRecordKey());
            return Pair.of(secondaryKey, recordKey);
          }
          return null;
        })
        .filter(Objects::nonNull)
        .collectAsList();

    Map<String, Set<String>> res = new HashMap<>();
    for (Pair<String, String> pair : pairs) {
      String key = pair.getKey();
      res.computeIfAbsent(key, k -> new HashSet<>()).add(pair.getValue());
    }
    return HoodieListPairData.eagerMapKV(res);
  }

  /**
   * Derive necessary properties for FG reader.
   */
  TypedProperties buildFileGroupReaderProperties(HoodieMetadataConfig metadataConfig) {
    HoodieCommonConfig commonConfig = HoodieCommonConfig.newBuilder()
        .fromProperties(metadataConfig.getProps()).build();
    TypedProperties props = new TypedProperties();
    props.setProperty(
        MAX_MEMORY_FOR_MERGE.key(),
        Long.toString(metadataConfig.getMaxReaderMemory()));
    props.setProperty(
        SPILLABLE_MAP_BASE_PATH.key(),
        metadataConfig.getSplliableMapDir());
    props.setProperty(
        SPILLABLE_DISK_MAP_TYPE.key(),
        commonConfig.getSpillableDiskMapType().name());
    props.setProperty(
        DISK_MAP_BITCASK_COMPRESSION_ENABLED.key(),
        Boolean.toString(commonConfig.isBitCaskDiskMapCompressionEnabled()));
    return props;
  }
}
