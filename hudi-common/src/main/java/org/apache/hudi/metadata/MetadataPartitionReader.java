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

import org.apache.hudi.common.avro.HoodieAvroReaderContext;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.expression.Predicate;
import org.apache.hudi.common.function.SerializableFunctionUnchecked;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.table.read.buffer.ReusableFileGroupRecordBufferLoader;
import org.apache.hudi.common.table.read.lsm.HoodieLsmFileGroupReader;
import org.apache.hudi.common.table.read.lsm.LsmReaderUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableFilterIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.EmptyIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.core.io.storage.HoodieAvroFileReader;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.StoragePath;

import lombok.Getter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.ValidationUtils.checkState;
import static org.apache.hudi.metadata.HoodieBackedTableMetadata.SCHEMA;
import static org.apache.hudi.metadata.HoodieBackedTableMetadata.buildPredicate;
import static org.apache.hudi.metadata.HoodieMetadataPayload.KEY_FIELD_NAME;

/**
 * Reads records by key from the latest file slices of one metadata table partition.
 *
 * <p>Everything it holds is resolved on the driver by {@link HoodieBackedTableMetadata}: the metadata table
 * meta client with its timeline loaded, the file group reader properties, the latest completed metadata
 * table instant, the instants whose log blocks are valid, and the partition's file slices. Distributed
 * lookups ship this object instead of the table metadata that created it, so a task neither carries the
 * data table meta client nor lists the data table timeline. The object is never mutated after creation,
 * so tasks can share one deserialized instance.
 */
public class MetadataPartitionReader implements Serializable {
  private static final long serialVersionUID = 1L;

  private final HoodieTableMetaClient metadataMetaClient;
  private final TypedProperties fileGroupReaderProps;
  private final String latestMetadataInstantTime;
  private final Set<String> validInstantTimestamps;
  @Getter
  private final String partitionName;
  @Getter
  private final List<FileSlice> fileSlices;

  MetadataPartitionReader(HoodieTableMetaClient metadataMetaClient,
                          TypedProperties fileGroupReaderProps,
                          String latestMetadataInstantTime,
                          Set<String> validInstantTimestamps,
                          String partitionName,
                          List<FileSlice> fileSlices) {
    this.metadataMetaClient = metadataMetaClient;
    this.fileGroupReaderProps = fileGroupReaderProps;
    this.latestMetadataInstantTime = latestMetadataInstantTime;
    this.validInstantTimestamps = validInstantTimestamps;
    this.partitionName = partitionName;
    this.fileSlices = fileSlices;
  }

  /**
   * Looks up keys of any file group of the partition: groups them by file group, then reads each file group once.
   * Deleted records are dropped.
   */
  public List<HoodieRecord<HoodieMetadataPayload>> lookupRecords(Collection<? extends RawKey> rawKeys) {
    return lookupEncodedKeys(rawKeys.stream().map(RawKey::encode).collect(Collectors.toList()));
  }

  private List<HoodieRecord<HoodieMetadataPayload>> lookupEncodedKeys(Collection<String> keys) {
    checkState(!fileSlices.isEmpty(), () -> "No file slices found for partition: " + partitionName);
    boolean isFullKey = !isSecondaryIndex();
    Map<Integer, TreeSet<String>> keysByFileGroup = new TreeMap<>();
    for (String key : keys) {
      keysByFileGroup.computeIfAbsent(HoodieTableMetadataUtil.mapRecordKeyToFileGroupIndex(key, fileSlices.size()),
          k -> new TreeSet<>(StringUtils.UTF8_LEXICOGRAPHIC_COMPARATOR)).add(key);
    }
    List<HoodieRecord<HoodieMetadataPayload>> records = new ArrayList<>();
    keysByFileGroup.forEach((fileGroupIndex, sortedKeys) -> {
      try (ClosableIterator<HoodieRecord<HoodieMetadataPayload>> it = lookupRecords(sortedKeys, fileSlices.get(fileGroupIndex), isFullKey)) {
        it.forEachRemaining(records::add);
      }
    });
    return records;
  }

  /**
   * Reads the bloom filters of the given (partition, file name) pairs from this bloom filter index partition.
   * Files without a bloom filter are absent from the result.
   */
  public Map<Pair<String, String>, BloomFilter> getBloomFilters(List<Pair<String, String>> partitionNameFileNameList) {
    Map<String, Pair<String, String>> fileToKeyMap = new HashMap<>();
    partitionNameFileNameList.forEach(partitionNameFileName -> fileToKeyMap.put(
        new BloomFilterIndexRawKey(partitionNameFileName.getLeft(), partitionNameFileName.getRight()).encode(), partitionNameFileName));
    Map<String, HoodieMetadataPayload> recordsByKey = new LinkedHashMap<>();
    lookupEncodedKeys(fileToKeyMap.keySet()).forEach(record -> recordsByKey.put(record.getRecordKey(), record.getData()));
    return BaseTableMetadata.toBloomFilters(fileToKeyMap, recordsByKey.entrySet().stream()
        .map(entry -> Pair.of(entry.getKey(), entry.getValue()))
        .collect(Collectors.toList()));
  }

  /**
   * Looks up record keys in the partitioned record index file group that the keys of the given data table
   * partition hash to. Deleted records are dropped.
   */
  public ClosableIterator<HoodieRecord<HoodieMetadataPayload>> lookupRecordsInDataTablePartition(String dataTablePartition,
                                                                                                Collection<? extends RawKey> rawKeys) {
    return lookupEncodedKeysInDataTablePartition(dataTablePartition, rawKeys.stream().map(RawKey::encode).collect(Collectors.toList()));
  }

  ClosableIterator<HoodieRecord<HoodieMetadataPayload>> lookupEncodedKeysInDataTablePartition(String dataTablePartition,
                                                                                             Collection<String> keys) {
    List<FileSlice> fileSlicesForDataPartition = fileSlices.stream()
        .filter(fileSlice -> HoodieTableMetadataUtil.getDataTablePartitionNameFromFileGroupName(fileSlice.getFileId()).equals(dataTablePartition))
        .collect(Collectors.toList());
    // All keys belong to the same shard, so the first key picks the file group.
    TreeSet<String> distinctSortedKeys = new TreeSet<>(StringUtils.UTF8_LEXICOGRAPHIC_COMPARATOR);
    distinctSortedKeys.addAll(keys);
    int fileGroupIndex = HoodieTableMetadataUtil.mapRecordKeyToFileGroupIndex(distinctSortedKeys.first(), fileSlicesForDataPartition.size());
    return lookupRecords(distinctSortedKeys, fileSlicesForDataPartition.get(fileGroupIndex), true);
  }

  /**
   * Looks up keys that all hash to the same file group; the keys must be distinct and sorted in UTF-8 byte order.
   * Deleted records are dropped.
   */
  ClosableIterator<HoodieRecord<HoodieMetadataPayload>> lookupRecordsInFileGroupOf(List<String> sortedKeys) {
    FileSlice fileSlice = fileSlices.get(HoodieTableMetadataUtil.mapRecordKeyToFileGroupIndex(sortedKeys.get(0), fileSlices.size()));
    return lookupRecords(sortedKeys, fileSlice, !isSecondaryIndex());
  }

  /**
   * Reads the records of one file slice whose keys start with any of the given sorted prefixes. Deleted
   * records are kept.
   */
  ClosableIterator<HoodieRecord<HoodieMetadataPayload>> readRecordsByKeyPrefixes(Collection<String> sortedKeyPrefixes, FileSlice fileSlice) {
    return readSliceAndFilterByKeys(sortedKeyPrefixes, fileSlice, metadataRecord -> {
      HoodieMetadataPayload payload = new HoodieMetadataPayload(Option.of(metadataRecord));
      String rowKey = payload.key != null ? payload.key : metadataRecord.get(KEY_FIELD_NAME).toString();
      return new HoodieAvroRecord<>(new HoodieKey(rowKey, partitionName), payload);
    }, false, this::readSliceWithFilter);
  }

  /**
   * Looks up the given sorted keys in one file slice. Deleted records are dropped.
   *
   * @param isFullKey If true, perform exact key match. If false, perform prefix match.
   */
  ClosableIterator<HoodieRecord<HoodieMetadataPayload>> lookupRecords(Collection<String> sortedKeys, FileSlice fileSlice, boolean isFullKey) {
    return lookupRecords(sortedKeys, fileSlice, isFullKey, this::readSliceWithFilter);
  }

  /**
   * Same as {@link #lookupRecords(Collection, FileSlice, boolean)}, reading the file slice with the given function.
   */
  ClosableIterator<HoodieRecord<HoodieMetadataPayload>> lookupRecords(Collection<String> sortedKeys, FileSlice fileSlice, boolean isFullKey,
                                                                      FileSliceReadFunction readFunction) {
    return new CloseableFilterIterator<>(
        readSliceAndFilterByKeys(sortedKeys, fileSlice, metadataRecord -> {
          HoodieMetadataPayload payload = new HoodieMetadataPayload(Option.of(metadataRecord));
          return new HoodieAvroRecord<>(new HoodieKey(payload.key, partitionName), payload);
        }, isFullKey, readFunction),
        r -> !r.getData().isDeleted());
  }

  private <T> ClosableIterator<T> readSliceAndFilterByKeys(Collection<String> sortedKeys,
                                                           FileSlice fileSlice,
                                                           SerializableFunctionUnchecked<GenericRecord, T> transformer,
                                                           boolean isFullKey,
                                                           FileSliceReadFunction readFunction) {
    // If no keys to lookup, we must return early, otherwise, the hfile lookup will return all records.
    if (sortedKeys.isEmpty()) {
      return new EmptyIterator<>();
    }
    try {
      Predicate predicate = buildPredicate(partitionName, sortedKeys, isFullKey);
      ClosableIterator<IndexedRecord> rawIterator = readFunction.read(predicate, fileSlice);
      return new CloseableMappingIterator<>(rawIterator, record -> transformer.apply((GenericRecord) record));
    } catch (IOException e) {
      throw new HoodieIOException("Error merging records from metadata table for " + sortedKeys.size() + " keys", e);
    }
  }

  /**
   * Reads the records of one file slice that match the predicate.
   */
  ClosableIterator<IndexedRecord> readSliceWithFilter(Predicate predicate, FileSlice fileSlice) throws IOException {
    return readSliceWithFilter(predicate, fileSlice, false, Collections.emptyMap(), null, TypedProperties.copy(fileGroupReaderProps));
  }

  /**
   * Reads one file slice, optionally through base file readers and a record buffer loader reused across calls.
   */
  ClosableIterator<IndexedRecord> readSliceWithFilter(Predicate predicate,
                                                      FileSlice fileSlice,
                                                      boolean reuse,
                                                      Map<StoragePath, HoodieAvroFileReader> baseFileReaders,
                                                      ReusableFileGroupRecordBufferLoader<IndexedRecord> recordBufferLoader,
                                                      TypedProperties props) throws IOException {
    boolean useLsmReader = !reuse
        && LsmReaderUtils.shouldUseLsmReader(metadataMetaClient.getTableConfig(), HoodieReaderConfig.REALTIME_PAYLOAD_COMBINE);
    HoodieReaderContext<IndexedRecord> readerContext = new HoodieAvroReaderContext(
        metadataMetaClient.getStorageConf(),
        metadataMetaClient.getTableConfig(),
        getInstantRange(),
        Option.of(predicate),
        baseFileReaders,
        props);
    if (useLsmReader) {
      return HoodieLsmFileGroupReader.<IndexedRecord>builder()
          .withReaderContext(readerContext)
          .withHoodieTableMetaClient(metadataMetaClient)
          .withLatestCommitTime(latestMetadataInstantTime)
          .withBaseFileOption(fileSlice.getBaseFile())
          .withLogFiles(fileSlice.getLogFiles())
          .withPartitionPath(fileSlice.getPartitionPath())
          .withDataSchema(SCHEMA)
          .withRequestedSchema(SCHEMA)
          .withProps(props)
          .build()
          .getClosableIterator();
    } else {
      return HoodieFileGroupReader.<IndexedRecord>builder()
          .withReaderContext(readerContext)
          .withHoodieTableMetaClient(metadataMetaClient)
          .withLatestCommitTime(latestMetadataInstantTime)
          .withBaseFileOption(fileSlice.getBaseFile())
          .withLogFiles(fileSlice.getLogFiles())
          .withPartitionPath(fileSlice.getPartitionPath())
          .withDataSchema(SCHEMA)
          .withRequestedSchema(SCHEMA)
          .withProps(props)
          .withRecordBufferLoader(recordBufferLoader)
          .build()
          .getClosableIterator();
    }
  }

  /**
   * Only log blocks of instants that are completed on the data table (or are metadata-table-only instants
   * such as indexing) are read, because the metadata table is updated before the data table commits.
   */
  Option<InstantRange> getInstantRange() {
    return Option.of(InstantRange.builder()
        .rangeType(InstantRange.RangeType.EXACT_MATCH)
        .explicitInstants(validInstantTimestamps).build());
  }

  String getLatestMetadataInstantTime() {
    return latestMetadataInstantTime;
  }

  private boolean isSecondaryIndex() {
    return MetadataPartitionType.fromPartitionPath(partitionName).equals(MetadataPartitionType.SECONDARY_INDEX);
  }

  /**
   * Reads the records of one file slice that match a predicate.
   */
  @FunctionalInterface
  interface FileSliceReadFunction {
    ClosableIterator<IndexedRecord> read(Predicate predicate, FileSlice fileSlice) throws IOException;
  }
}
