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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SpillableMapUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.expression.BindVisitor;
import org.apache.hudi.expression.Expression;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.io.storage.HoodieSeekingFileReader;
import org.apache.hudi.util.Transient;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.hudi.common.config.HoodieMetadataConfig.DEFAULT_METADATA_ENABLE_FULL_SCAN_LOG_FILES;
import static org.apache.hudi.common.util.CollectionUtils.toStream;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_BLOOM_FILTERS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_FILES;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getFileSystemView;

/**
 * Table metadata provided by an internal DFS backed Hudi metadata table.
 */
public class HoodieBackedTableMetadata extends BaseTableMetadata {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieBackedTableMetadata.class);

  private final String metadataBasePath;

  private HoodieTableMetaClient metadataMetaClient;
  private HoodieTableConfig metadataTableConfig;

  private HoodieTableFileSystemView metadataFileSystemView;
  // should we reuse the open file handles, across calls
  private final boolean reuse;

  // Readers for the latest file slice corresponding to file groups in the metadata partition
  private final Transient<Map<Pair<String, String>, Pair<HoodieSeekingFileReader<?>, HoodieMetadataLogRecordReader>>> partitionReaders =
      Transient.lazy(ConcurrentHashMap::new);

  // Latest file slices in the metadata partitions
  private final Map<String, List<FileSlice>> partitionFileSliceMap = new ConcurrentHashMap<>();

  public HoodieBackedTableMetadata(HoodieEngineContext engineContext, HoodieMetadataConfig metadataConfig, String datasetBasePath) {
    this(engineContext, metadataConfig, datasetBasePath, false);
  }

  public HoodieBackedTableMetadata(HoodieEngineContext engineContext, HoodieMetadataConfig metadataConfig,
                                   String datasetBasePath, boolean reuse) {
    super(engineContext, metadataConfig, datasetBasePath);
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
        this.metadataMetaClient = HoodieTableMetaClient.builder().setConf(getHadoopConf()).setBasePath(metadataBasePath).build();
        this.metadataFileSystemView = getFileSystemView(metadataMetaClient);
        this.metadataTableConfig = metadataMetaClient.getTableConfig();
      } catch (TableNotFoundException e) {
        LOG.warn("Metadata table was not found at path " + metadataBasePath);
        this.isMetadataTableInitialized = false;
        this.metadataMetaClient = null;
        this.metadataFileSystemView = null;
        this.metadataTableConfig = null;
      } catch (Exception e) {
        LOG.error("Failed to initialize metadata table at path " + metadataBasePath, e);
        this.isMetadataTableInitialized = false;
        this.metadataMetaClient = null;
        this.metadataFileSystemView = null;
        this.metadataTableConfig = null;
      }
    }
  }

  @Override
  public Option<HoodieRecord<HoodieMetadataPayload>> getRecordByKey(String key, String partitionName) {
    Map<String, HoodieRecord<HoodieMetadataPayload>> recordsByKeys = getRecordsByKeys(Collections.singletonList(key), partitionName);
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
  public HoodieData<HoodieRecord<HoodieMetadataPayload>> getRecordsByKeyPrefixes(List<String> keyPrefixes,
                                                                                 String partitionName,
                                                                                 boolean shouldLoadInMemory) {
    // Sort the prefixes so that keys are looked up in order
    List<String> sortedKeyPrefixes = new ArrayList<>(keyPrefixes);
    Collections.sort(sortedKeyPrefixes);

    // NOTE: Since we partition records to a particular file-group by full key, we will have
    //       to scan all file-groups for all key-prefixes as each of these might contain some
    //       records matching the key-prefix
    List<FileSlice> partitionFileSlices = partitionFileSliceMap.computeIfAbsent(partitionName,
        k -> HoodieTableMetadataUtil.getPartitionLatestMergedFileSlices(metadataMetaClient, metadataFileSystemView, partitionName));
    ValidationUtils.checkState(partitionFileSlices.size() > 0, "Number of file slices for partition " + partitionName + " should be > 0");

    return (shouldLoadInMemory ? HoodieListData.lazy(partitionFileSlices) :
        engineContext.parallelize(partitionFileSlices))
        .flatMap(
            (SerializableFunction<FileSlice, Iterator<HoodieRecord<HoodieMetadataPayload>>>) fileSlice -> {
              // NOTE: Since this will be executed by executors, we can't access previously cached
              //       readers, and therefore have to always open new ones
              Pair<HoodieSeekingFileReader<?>, HoodieMetadataLogRecordReader> readers =
                  openReaders(partitionName, fileSlice);
              try {
                List<Long> timings = new ArrayList<>();

                HoodieSeekingFileReader<?> baseFileReader = readers.getKey();
                HoodieMetadataLogRecordReader logRecordScanner = readers.getRight();

                if (baseFileReader == null && logRecordScanner == null) {
                  // TODO: what do we do if both does not exist? should we throw an exception and let caller do the fallback ?
                  return Collections.emptyIterator();
                }

                boolean fullKeys = false;

                Map<String, HoodieRecord<HoodieMetadataPayload>> logRecords =
                    readLogRecords(logRecordScanner, sortedKeyPrefixes, fullKeys, timings);

                Map<String, HoodieRecord<HoodieMetadataPayload>> mergedRecords =
                    readFromBaseAndMergeWithLogRecords(baseFileReader, sortedKeyPrefixes, fullKeys, logRecords, timings, partitionName);

                LOG.debug(String.format("Metadata read for %s keys took [baseFileRead, logMerge] %s ms",
                    sortedKeyPrefixes.size(), timings));

                return mergedRecords.values().iterator();
              } catch (IOException ioe) {
                throw new HoodieIOException("Error merging records from metadata table for  " + sortedKeyPrefixes.size() + " key : ", ioe);
              } finally {
                closeReader(readers);
              }
            });
  }

  @Override
  protected Map<String, HoodieRecord<HoodieMetadataPayload>> getRecordsByKeys(List<String> keys, String partitionName) {
    if (keys.isEmpty()) {
      return Collections.emptyMap();
    }

    Map<String, HoodieRecord<HoodieMetadataPayload>> result;

    // Load the file slices for the partition. Each file slice is a shard which saves a portion of the keys.
    List<FileSlice> partitionFileSlices = partitionFileSliceMap.computeIfAbsent(partitionName,
        k -> HoodieTableMetadataUtil.getPartitionLatestMergedFileSlices(metadataMetaClient, metadataFileSystemView, partitionName));
    final int numFileSlices = partitionFileSlices.size();
    ValidationUtils.checkState(numFileSlices > 0, "Number of file slices for partition " + partitionName + " should be > 0");

    // Lookup keys from each file slice
    if (numFileSlices == 1) {
      // Optimization for a single slice for smaller metadata table partitions
      result = lookupKeysFromFileSlice(partitionName, keys, partitionFileSlices.get(0));
    } else {
      // Parallel lookup for large sized partitions with many file slices
      // Partition the keys by the file slice which contains it
      ArrayList<ArrayList<String>> partitionedKeys = new ArrayList<>(numFileSlices);
      for (int i = 0; i < numFileSlices; ++i) {
        partitionedKeys.add(new ArrayList<>());
      }
      keys.forEach(key -> {
        int shardIndex = HoodieTableMetadataUtil.mapRecordKeyToFileGroupIndex(key, numFileSlices);
        partitionedKeys.get(shardIndex).add(key);
      });

      result = new HashMap<>(keys.size());
      getEngineContext().setJobStatus(this.getClass().getSimpleName(), "Reading keys from metadata table partition " + partitionName);
      getEngineContext().map(partitionedKeys, keysList -> {
        if (keysList.isEmpty()) {
          return Collections.<String, HoodieRecord<HoodieMetadataPayload>>emptyMap();
        }
        int shardIndex = HoodieTableMetadataUtil.mapRecordKeyToFileGroupIndex(keysList.get(0), numFileSlices);
        return lookupKeysFromFileSlice(partitionName, keysList, partitionFileSlices.get(shardIndex));
      }, partitionedKeys.size()).forEach(result::putAll);
    }

    return result;
  }

  /**
   * Lookup list of keys from a single file slice.
   *
   * @param partitionName Name of the partition
   * @param keys          The list of keys to lookup
   * @param fileSlice     The file slice to read
   * @return A {@code Map} of key name to {@code HoodieRecord} for the keys which were found in the file slice
   */
  private Map<String, HoodieRecord<HoodieMetadataPayload>> lookupKeysFromFileSlice(String partitionName, List<String> keys, FileSlice fileSlice) {
    Pair<HoodieSeekingFileReader<?>, HoodieMetadataLogRecordReader> readers = getOrCreateReaders(partitionName, fileSlice);
    try {
      List<Long> timings = new ArrayList<>(1);
      HoodieSeekingFileReader<?> baseFileReader = readers.getKey();
      HoodieMetadataLogRecordReader logRecordScanner = readers.getRight();
      if (baseFileReader == null && logRecordScanner == null) {
        return Collections.emptyMap();
      }

      // Sort it here once so that we don't need to sort individually for base file and for each individual log files.
      List<String> sortedKeys = new ArrayList<>(keys);
      Collections.sort(sortedKeys);
      boolean fullKeys = true;
      Map<String, HoodieRecord<HoodieMetadataPayload>> logRecords = readLogRecords(logRecordScanner, sortedKeys, fullKeys, timings);
      return readFromBaseAndMergeWithLogRecords(baseFileReader, sortedKeys, fullKeys, logRecords, timings, partitionName);
    } catch (IOException ioe) {
      throw new HoodieIOException("Error merging records from metadata table for  " + keys.size() + " key : ", ioe);
    } finally {
      if (!reuse) {
        closeReader(readers);
      }
    }
  }

  private Map<String, HoodieRecord<HoodieMetadataPayload>> readLogRecords(HoodieMetadataLogRecordReader logRecordReader,
                                                                          List<String> sortedKeys,
                                                                          boolean fullKey,
                                                                          List<Long> timings) {
    HoodieTimer timer = HoodieTimer.start();

    if (logRecordReader == null) {
      timings.add(timer.endTimer());
      return Collections.emptyMap();
    }

    try {
      return fullKey ? logRecordReader.getRecordsByKeys(sortedKeys) : logRecordReader.getRecordsByKeyPrefixes(sortedKeys);
    } finally {
      timings.add(timer.endTimer());
    }
  }

  private Map<String, HoodieRecord<HoodieMetadataPayload>> readFromBaseAndMergeWithLogRecords(HoodieSeekingFileReader<?> reader,
                                                                                              List<String> sortedKeys,
                                                                                              boolean fullKeys,
                                                                                              Map<String, HoodieRecord<HoodieMetadataPayload>> logRecords,
                                                                                              List<Long> timings,
                                                                                              String partitionName) throws IOException {
    HoodieTimer timer = HoodieTimer.start();

    if (reader == null) {
      // No base file at all
      timings.add(timer.endTimer());
      return logRecords;
    }

    HoodieTimer readTimer = HoodieTimer.start();

    Map<String, HoodieRecord<HoodieMetadataPayload>> records =
        fetchBaseFileRecordsByKeys(reader, sortedKeys, fullKeys, partitionName);

    metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.BASEFILE_READ_STR, readTimer.endTimer()));

    // Iterate over all provided log-records, merging them into existing records
    logRecords.values().forEach(logRecord ->
        records.merge(
            logRecord.getRecordKey(),
            logRecord,
            (oldRecord, newRecord) -> {
              HoodieMetadataPayload mergedPayload = newRecord.getData().preCombine(oldRecord.getData());
              return mergedPayload.isDeleted() ? null : new HoodieAvroRecord<>(oldRecord.getKey(), mergedPayload);
            }
        ));

    timings.add(timer.endTimer());
    return records;
  }

  @SuppressWarnings("unchecked")
  private Map<String, HoodieRecord<HoodieMetadataPayload>> fetchBaseFileRecordsByKeys(HoodieSeekingFileReader reader,
                                                                                      List<String> sortedKeys,
                                                                                      boolean fullKeys,
                                                                                      String partitionName) throws IOException {
    ClosableIterator<HoodieRecord<?>> records = fullKeys
        ? reader.getRecordsByKeysIterator(sortedKeys)
        : reader.getRecordsByKeyPrefixIterator(sortedKeys);

    Map<String, HoodieRecord<HoodieMetadataPayload>> result = toStream(records)
        .map(record -> {
          GenericRecord data = (GenericRecord) record.getData();
          return Pair.of(
              (String) (data).get(HoodieMetadataPayload.KEY_FIELD_NAME),
              composeRecord(data, partitionName));
        })
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    records.close();
    return result;
  }

  private HoodieRecord<HoodieMetadataPayload> composeRecord(GenericRecord avroRecord, String partitionName) {
    if (metadataTableConfig.populateMetaFields()) {
      return SpillableMapUtils.convertToHoodieRecordPayload(avroRecord,
          metadataTableConfig.getPayloadClass(), metadataTableConfig.getPreCombineField(), false);
    }
    return SpillableMapUtils.convertToHoodieRecordPayload(avroRecord,
        metadataTableConfig.getPayloadClass(), metadataTableConfig.getPreCombineField(),
        Pair.of(metadataTableConfig.getRecordKeyFieldProp(), metadataTableConfig.getPartitionFieldProp()),
        false, Option.of(partitionName), Option.empty());
  }

  /**
   * Create a file reader and the record scanner for a given partition and file slice
   * if readers are not already available.
   *
   * @param partitionName - Partition name
   * @param slice         - The file slice to open readers for
   * @return File reader and the record scanner pair for the requested file slice
   */
  private Pair<HoodieSeekingFileReader<?>, HoodieMetadataLogRecordReader> getOrCreateReaders(String partitionName, FileSlice slice) {
    if (reuse) {
      Pair<String, String> key = Pair.of(partitionName, slice.getFileId());
      return partitionReaders.get().computeIfAbsent(key, ignored -> openReaders(partitionName, slice));
    } else {
      return openReaders(partitionName, slice);
    }
  }

  private Pair<HoodieSeekingFileReader<?>, HoodieMetadataLogRecordReader> openReaders(String partitionName, FileSlice slice) {
    try {
      HoodieTimer timer = HoodieTimer.start();
      // Open base file reader
      Pair<HoodieSeekingFileReader<?>, Long> baseFileReaderOpenTimePair = getBaseFileReader(slice, timer);
      HoodieSeekingFileReader<?> baseFileReader = baseFileReaderOpenTimePair.getKey();
      final long baseFileOpenMs = baseFileReaderOpenTimePair.getValue();

      // Open the log record scanner using the log files from the latest file slice
      List<HoodieLogFile> logFiles = slice.getLogFiles().collect(Collectors.toList());
      Pair<HoodieMetadataLogRecordReader, Long> logRecordScannerOpenTimePair =
          getLogRecordScanner(logFiles, partitionName, Option.empty());
      HoodieMetadataLogRecordReader logRecordScanner = logRecordScannerOpenTimePair.getKey();
      final long logScannerOpenMs = logRecordScannerOpenTimePair.getValue();

      metrics.ifPresent(metrics -> metrics.updateMetrics(HoodieMetadataMetrics.SCAN_STR,
          baseFileOpenMs + logScannerOpenMs));
      return Pair.of(baseFileReader, logRecordScanner);
    } catch (IOException e) {
      throw new HoodieIOException("Error opening readers for metadata table partition " + partitionName, e);
    }
  }

  private Pair<HoodieSeekingFileReader<?>, Long> getBaseFileReader(FileSlice slice, HoodieTimer timer) throws IOException {
    HoodieSeekingFileReader<?> baseFileReader;
    long baseFileOpenMs;
    // If the base file is present then create a reader
    Option<HoodieBaseFile> basefile = slice.getBaseFile();
    if (basefile.isPresent()) {
      String baseFilePath = basefile.get().getPath();
      baseFileReader = (HoodieSeekingFileReader<?>) HoodieFileReaderFactory.getReaderFactory(HoodieRecordType.AVRO)
          .getFileReader(getHadoopConf(), new Path(baseFilePath));
      baseFileOpenMs = timer.endTimer();
      LOG.info(String.format("Opened metadata base file from %s at instant %s in %d ms", baseFilePath,
          basefile.get().getCommitTime(), baseFileOpenMs));
    } else {
      baseFileReader = null;
      baseFileOpenMs = 0L;
      timer.endTimer();
    }
    return Pair.of(baseFileReader, baseFileOpenMs);
  }

  public Pair<HoodieMetadataLogRecordReader, Long> getLogRecordScanner(List<HoodieLogFile> logFiles,
                                                                       String partitionName,
                                                                       Option<Boolean> allowFullScanOverride) {
    HoodieTimer timer = HoodieTimer.start();
    List<String> sortedLogFilePaths = logFiles.stream()
        .sorted(HoodieLogFile.getLogFileComparator())
        .map(o -> o.getPath().toString())
        .collect(Collectors.toList());

    // Only those log files which have a corresponding completed instant on the dataset should be read
    // This is because the metadata table is updated before the dataset instants are committed.
    Set<String> validInstantTimestamps = HoodieTableMetadataUtil
        .getValidInstantTimestamps(dataMetaClient, metadataMetaClient);

    Option<HoodieInstant> latestMetadataInstant = metadataMetaClient.getActiveTimeline().filterCompletedInstants().lastInstant();
    String latestMetadataInstantTime = latestMetadataInstant.map(HoodieInstant::getTimestamp).orElse(SOLO_COMMIT_TIMESTAMP);

    boolean allowFullScan = allowFullScanOverride.orElseGet(() -> isFullScanAllowedForPartition(partitionName));

    // Load the schema
    Schema schema = HoodieAvroUtils.addMetadataFields(HoodieMetadataRecord.getClassSchema());
    HoodieCommonConfig commonConfig = HoodieCommonConfig.newBuilder().fromProperties(metadataConfig.getProps()).build();
    HoodieMetadataLogRecordReader logRecordScanner = HoodieMetadataLogRecordReader.newBuilder()
        .withFileSystem(metadataMetaClient.getFs())
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
        .withEnableOptimizedLogBlocksScan(metadataConfig.doEnableOptimizedLogBlocksScan())
        .build();

    Long logScannerOpenMs = timer.endTimer();
    LOG.info(String.format("Opened %d metadata log files (dataset instant=%s, metadata instant=%s) in %d ms",
        sortedLogFilePaths.size(), getLatestDataInstantTime(), latestMetadataInstantTime, logScannerOpenMs));
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
    return metadataFileSystemView;
  }

  public Map<String, String> stats() {
    Set<String> allMetadataPartitionPaths = Arrays.stream(MetadataPartitionType.values()).map(MetadataPartitionType::getPartitionPath).collect(Collectors.toSet());
    return metrics.map(m -> m.getStats(true, metadataMetaClient, this, allMetadataPartitionPaths)).orElse(new HashMap<>());
  }

  @Override
  public Option<String> getSyncedInstantTime() {
    if (metadataMetaClient != null) {
      Option<HoodieInstant> latestInstant = metadataMetaClient.getActiveTimeline().getDeltaCommitTimeline().filterCompletedInstants().lastInstant();
      if (latestInstant.isPresent()) {
        return Option.of(latestInstant.get().getTimestamp());
      }
    }
    return Option.empty();
  }

  @Override
  public Option<String> getLatestCompactionTime() {
    if (metadataMetaClient != null) {
      Option<HoodieInstant> latestCompaction = metadataMetaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants().lastInstant();
      if (latestCompaction.isPresent()) {
        return Option.of(latestCompaction.get().getTimestamp());
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
      metadataFileSystemView.close();
      metadataFileSystemView = getFileSystemView(metadataMetaClient);
    }
    // the cached reader has max instant time restriction, they should be cleared
    // because the metadata timeline may have changed.
    closePartitionReaders();
    partitionFileSliceMap.clear();
  }

  @Override
  public int getNumFileGroupsForPartition(MetadataPartitionType partition) {
    partitionFileSliceMap.computeIfAbsent(partition.getPartitionPath(),
        k -> HoodieTableMetadataUtil.getPartitionLatestMergedFileSlices(metadataMetaClient,
            metadataFileSystemView, partition.getPartitionPath()));
    return partitionFileSliceMap.get(partition.getPartitionPath()).size();
  }
}
