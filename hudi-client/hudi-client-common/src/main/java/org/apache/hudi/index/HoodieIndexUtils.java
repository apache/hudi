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

package org.apache.hudi.index;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieAvroRecordMerger;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.log.HoodieFileSliceCompactedReader;
import org.apache.hudi.common.table.log.HoodieFileSliceReader;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.index.bloom.HoodieGlobalBloomIndex;
import org.apache.hudi.index.simple.HoodieGlobalSimpleIndex;
import org.apache.hudi.io.IOUtils;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import static java.util.stream.Collectors.toList;
import static org.apache.hudi.common.table.log.HoodieFileSliceReader.getFileSliceReader;

/**
 * Hoodie Index Utilities.
 */
public class HoodieIndexUtils {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieIndexUtils.class);

  /**
   * Fetches Pair of partition path and {@link HoodieBaseFile}s for interested partitions.
   *
   * @param partition   Partition of interest
   * @param hoodieTable Instance of {@link HoodieTable} of interest
   * @return the list of {@link HoodieBaseFile}
   */
  public static List<HoodieBaseFile> getLatestBaseFilesForPartition(String partition,
                                                                    HoodieTable hoodieTable) {
    Option<HoodieInstant> latestCommitTime = hoodieTable.getMetaClient().getCommitsTimeline()
        .filterCompletedInstants().lastInstant();
    if (latestCommitTime.isPresent()) {
      return hoodieTable.getBaseFileOnlyView()
          .getLatestBaseFilesBeforeOrOn(partition, latestCommitTime.get().getTimestamp())
          .collect(toList());
    }
    return Collections.emptyList();
  }

  /**
   * Fetches Pair of partition path and {@link FileSlice}s for interested partitions.
   *
   * @param partition   Partition of interest
   * @param hoodieTable Instance of {@link HoodieTable} of interest
   * @return the list of {@link FileSlice}
   */
  public static List<FileSlice> getLatestFileSlicesForPartition(
          final String partition,
          final HoodieTable hoodieTable) {
    Option<HoodieInstant> latestCommitTime = hoodieTable.getMetaClient().getCommitsTimeline()
            .filterCompletedInstants().lastInstant();
    if (latestCommitTime.isPresent()) {
      return hoodieTable.getHoodieView()
              .getLatestFileSlicesBeforeOrOn(partition, latestCommitTime.get().getTimestamp(), true)
              .collect(toList());
    }
    return Collections.emptyList();
  }

  public static Option<FileSlice> getLatestFileSliceForPartitionAndFileId(String partition, String fileId, HoodieTable hoodieTable) {
    Option<HoodieInstant> latestCommitTime = hoodieTable.getMetaClient().getCommitsTimeline()
        .filterCompletedInstants().lastInstant();
    if (latestCommitTime.isPresent()) {
      return hoodieTable.getHoodieView().getLatestFileSlice(partition, fileId);
    }
    return Option.empty();
  }

  /**
   * Fetches Pair of partition path and {@link HoodieBaseFile}s for interested partitions.
   *
   * @param partitions  list of partitions of interest
   * @param context     instance of {@link HoodieEngineContext} to use
   * @param hoodieTable instance of {@link HoodieTable} of interest
   * @return the list of Pairs of partition path and fileId
   */
  public static List<Pair<String, HoodieBaseFile>> getLatestBaseFilesForAllPartitions(final List<String> partitions,
                                                                                      final HoodieEngineContext context,
                                                                                      final HoodieTable hoodieTable) {
    context.setJobStatus(HoodieIndexUtils.class.getSimpleName(), "Load latest base files from all partitions: " + hoodieTable.getConfig().getTableName());
    return context.flatMap(partitions, partitionPath -> {
      List<Pair<String, HoodieBaseFile>> filteredFiles =
          getLatestBaseFilesForPartition(partitionPath, hoodieTable).stream()
              .map(baseFile -> Pair.of(partitionPath, baseFile))
              .collect(toList());

      return filteredFiles.stream();
    }, Math.max(partitions.size(), 1));
  }

  /**
   * Get tagged record for the passed in {@link HoodieRecord}.
   *
   * @param inputRecord instance of {@link HoodieRecord} for which tagging is requested
   * @param location    {@link HoodieRecordLocation} for the passed in {@link HoodieRecord}
   * @return the tagged {@link HoodieRecord}
   */
  public static <R> HoodieRecord<R> getTaggedRecord(HoodieRecord<R> inputRecord, Option<HoodieRecordLocation> location) {
    HoodieRecord<R> record = inputRecord;
    if (location.isPresent()) {
      // When you have a record in multiple files in the same partition, then <row key, record> collection
      // will have 2 entries with the same exact in memory copy of the HoodieRecord and the 2
      // separate filenames that the record is found in. This will result in setting
      // currentLocation 2 times and it will fail the second time. So creating a new in memory
      // copy of the hoodie record.
      record = inputRecord.newInstance();
      record.unseal();
      record.setCurrentLocation(location.get());
      record.seal();
    }
    return record;
  }

  /**
   * Given a list of row keys and one file, return only row keys existing in that file.
   *
   * @param filePath            - File to filter keys from
   * @param candidateRecordKeys - Candidate keys to filter
   * @return List of candidate keys that are available in the file
   */
  public static List<String> filterKeysFromFile(Path filePath, List<String> candidateRecordKeys,
                                                Configuration configuration) throws HoodieIndexException {
    ValidationUtils.checkArgument(FSUtils.isBaseFile(filePath));
    List<String> foundRecordKeys = new ArrayList<>();
    try (HoodieFileReader fileReader = HoodieFileReaderFactory.getReaderFactory(HoodieRecordType.AVRO)
        .getFileReader(configuration, filePath)) {
      // Load all rowKeys from the file, to double-confirm
      if (!candidateRecordKeys.isEmpty()) {
        HoodieTimer timer = HoodieTimer.start();
        Set<String> fileRowKeys = fileReader.filterRowKeys(new TreeSet<>(candidateRecordKeys));
        foundRecordKeys.addAll(fileRowKeys);
        LOG.info(String.format("Checked keys against file %s, in %d ms. #candidates (%d) #found (%d)", filePath,
            timer.endTimer(), candidateRecordKeys.size(), foundRecordKeys.size()));
        if (LOG.isDebugEnabled()) {
          LOG.debug("Keys matching for file " + filePath + " => " + foundRecordKeys);
        }
      }
    } catch (Exception e) {
      throw new HoodieIndexException("Error checking candidate keys against file.", e);
    }
    return foundRecordKeys;
  }

  public static boolean checkIfValidCommit(HoodieTimeline commitTimeline, String commitTs) {
    // Check if the last commit ts for this row is 1) present in the timeline or
    // 2) is less than the first commit ts in the timeline
    return !commitTimeline.empty() && commitTimeline.containsOrBeforeTimelineStarts(commitTs);
  }

  /**
   * Used by {@link HoodieGlobalSimpleIndex} and {@link HoodieGlobalBloomIndex}
   * exclusively to dedup the tagged records due to partition updates.
   *
   * @param taggedHoodieRecords records tagged by the index by making incoming records left-outer-join existing records.
   * @param dedupParallelism    parallelism for dedup.
   * @see HoodieGlobalSimpleIndex
   * @see HoodieGlobalBloomIndex
   * @see HoodieIndexConfig#GLOBAL_INDEX_DEDUP_PARALLELISM
   */
  public static <R> HoodieData<HoodieRecord<R>> dedupForPartitionUpdates(HoodieData<Pair<HoodieRecord<R>, Boolean>> taggedHoodieRecords, int dedupParallelism) {
    /*
     * If a record is updated from p1 to p2 and then to p3, 2 existing records
     * will be tagged for this record to insert to p3. So we dedup them here. (Set A)
     */
    HoodiePairData<String, HoodieRecord<R>> deduped = taggedHoodieRecords.filter(Pair::getRight)
        .map(Pair::getLeft)
        .distinctWithKey(HoodieRecord::getKey, dedupParallelism)
        .mapToPair(r -> Pair.of(r.getRecordKey(), r));

    /*
     * This includes
     *  - tagged existing records whose partition paths are not to be updated (Set B)
     *  - completely new records (Set C)
     */
    HoodieData<HoodieRecord<R>> undeduped = taggedHoodieRecords.filter(p -> !p.getRight()).map(Pair::getLeft);

    /*
     * There can be intersection between Set A and Set B mentioned above.
     *
     * Example: record X is updated from p1 to p2 and then back to p1.
     * Set A will contain an insert to p1 and Set B will contain an update to p1.
     *
     * As the insert to p1 is tagged in the context of partition update (p2 to p1), the record's
     * location will be empty (Index sees it as a new record to p1) and it will result in creating
     * a new file group in p1. We need to do "A left-anti join B" to drop the insert from Set A
     * and keep the update in Set B.
     */
    return deduped.leftOuterJoin(undeduped
            .filter(r -> !(r.getData() instanceof EmptyHoodieRecordPayload))
            .mapToPair(r -> Pair.of(r.getRecordKey(), r)))
        .values().filter(p -> !p.getRight().isPresent()).map(Pair::getLeft)
        .union(undeduped);
  }

  public static <R> HoodieData<HoodieRecord<R>> getTaggedRecordsFromPartitionLocations(HoodieData<Pair<String, HoodieRecordLocation>> partitionLocations, HoodieWriteConfig config, HoodieTable hoodieTable, Option<String> instantTime) {
    return partitionLocations.flatMap(p -> {
      Option<FileSlice> fileSliceOpt = getLatestFileSliceForPartitionAndFileId(p.getLeft(), p.getRight().getFileId(), hoodieTable);
      if (fileSliceOpt.isPresent() && instantTime.isPresent()) {
        return HoodieIndexUtils.<R>getRecordsFromFileSlice(fileSliceOpt.get(), config, hoodieTable, instantTime.get()).iterator();
      } else {
        return Collections.emptyIterator();
      }
    });
  }

  public static <R> List<HoodieRecord<R>> getRecordsFromFileSlice(FileSlice fileSlice, HoodieWriteConfig config, HoodieTable hoodieTable, String instantTime) {
    long maxMemoryPerCompaction = IOUtils.getMaxMemoryPerCompaction(hoodieTable.getTaskContextSupplier(), config);
    List<String> logFilePaths = fileSlice.getLogFiles().map(l -> l.getPath().toString()).collect(toList());
    Schema readerSchema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(config.getSchema()));
    HoodieMergedLogRecordScanner scanner = HoodieMergedLogRecordScanner.newBuilder()
        .withFileSystem(hoodieTable.getMetaClient().getFs())
        .withBasePath(hoodieTable.getMetaClient().getBasePath())
        .withLogFilePaths(logFilePaths)
        .withReaderSchema(readerSchema)
        .withLatestInstantTime(instantTime)
        .withMaxMemorySizeInBytes(maxMemoryPerCompaction)
        .withReadBlocksLazily(config.getCompactionLazyBlockReadEnabled())
        .withReverseReader(config.getCompactionReverseLogReadEnabled())
        .withBufferSize(config.getMaxDFSStreamBufferSize())
        .withSpillableMapBasePath(config.getSpillableMapBasePath())
        .withPartition(fileSlice.getPartitionPath())
        .withOptimizedLogBlocksScan(config.enableOptimizedLogBlocksScan())
        .withDiskMapType(config.getCommonConfig().getSpillableDiskMapType())
        .withBitCaskDiskMapCompressionEnabled(config.getCommonConfig().isBitCaskDiskMapCompressionEnabled())
        .withRecordMerger(config.getRecordMerger())
        .build();

    HoodieRecordLocation currentLocation = new HoodieRecordLocation(instantTime, fileSlice.getFileId());
    Option<HoodieFileReader> baseFileReader = Option.empty();
    List<HoodieRecord<R>> records = new ArrayList<>();
    try {
      if (fileSlice.getBaseFile().isPresent()) {
        HoodieBaseFile baseFile = fileSlice.getBaseFile().get();
        baseFileReader = Option.of(HoodieFileReaderFactory
            .getReaderFactory(config.getRecordMerger().getRecordType()).getFileReader(hoodieTable.getHadoopConf(), baseFile.getHadoopPath()));
      }
      new HoodieFileSliceCompactedReader<R>(baseFileReader, scanner, config).getCompactedRecords().forEach(r -> {
        r.unseal();
        r.setCurrentLocation(currentLocation);
        r.seal();
        records.add(r);
      });
    } catch (IOException e) {
      throw new HoodieIndexException("Error reading input data for " + fileSlice.getBaseFile()
          + " and " + logFilePaths, e);
    } finally {
      if (scanner != null) {
        scanner.close();
      }
      if (baseFileReader.isPresent()) {
        baseFileReader.get().close();
      }
    }
    return records;
  }

  public static <R> HoodieData<HoodieRecord<R>> mergeForPartitionUpdates(
      HoodieData<Pair<HoodieRecord<R>, Option<Pair<String, HoodieRecordLocation>>>> taggedHoodieRecords, HoodieWriteConfig config, HoodieTable hoodieTable) {
    // completely new records
    HoodieData<HoodieRecord<R>> newRecords = taggedHoodieRecords.filter(p -> !p.getRight().isPresent()).map(Pair::getLeft);
    // the records tagged to existing base files
    HoodieData<HoodieRecord<R>> updatingRecords = taggedHoodieRecords.filter(p -> p.getRight().isPresent()).map(Pair::getLeft).distinctWithKey(HoodieRecord::getRecordKey, config.getGlobalIndexDedupParallelism());
    // the tagging partitions and locations
    HoodieData<Pair<String, HoodieRecordLocation>> partitionLocations = taggedHoodieRecords
        .filter(p -> p.getRight().isPresent())
        .map(p -> p.getRight().get())
        .distinct(config.getGlobalIndexDedupParallelism());
    // merged existing records with current locations being set
    HoodieData<HoodieRecord<R>> existingRecords = getTaggedRecordsFromPartitionLocations(partitionLocations, config, hoodieTable,
        hoodieTable.getMetaClient().getCommitsTimeline()
            .filterCompletedInstants().lastInstant().map(HoodieInstant::getTimestamp));

    TypedProperties updatedProps = HoodieAvroRecordMerger.Config.withLegacyOperatingModePreCombining(config.getProps());
    HoodieData<HoodieRecord<R>> taggedUpdatingRecords = updatingRecords.mapToPair(r -> Pair.of(r.getRecordKey(), r)).leftOuterJoin(existingRecords.mapToPair(r -> Pair.of(r.getRecordKey(), r)))
        .values().flatMap(entry -> {
          HoodieRecord<R> incoming = entry.getLeft();
          Option<HoodieRecord<R>> existingOpt = entry.getRight();
          if (!existingOpt.isPresent()) {
            // existing record not found (e.g., due to delete log not merged to base file): tag as a new record
            return Collections.singletonList(getTaggedRecord(incoming, Option.empty())).iterator();
          }
          HoodieRecord<R> existing = existingOpt.get();
          Schema writeSchema = new Schema.Parser().parse(config.getWriteSchema());
          Schema writeSchemaWithMetaFields = HoodieAvroUtils.addMetadataFields(writeSchema, config.allowOperationMetadataField());
          Schema existingSchema = config.populateMetaFields() ? writeSchemaWithMetaFields : writeSchema;
          Option<Pair<HoodieRecord, Schema>> mergeResult = config.getRecordMerger().merge(existing, existingSchema, incoming, writeSchema, updatedProps);
          if (!mergeResult.isPresent()) {
            // merge resulted in delete: force tag the incoming to the old partition
            return Collections.singletonList(getTaggedRecord(incoming, Option.of(existing.getCurrentLocation()))).iterator();
          }
          HoodieRecord<R> merged = (HoodieRecord<R>) mergeResult.get().getLeft();
          if (Objects.equals(merged.getPartitionPath(), existing.getPartitionPath())) {
            // merged record has the same partition: route the incoming record to the current location as an update
            return Collections.singletonList(getTaggedRecord(incoming, Option.of(existing.getCurrentLocation()))).iterator();
          } else {
            // merged record has a different partition: issue a delete to the old partition and insert the merged record to the new partition
            HoodieRecord<R> deleteRecord = new HoodieAvroRecord(existing.getKey(), new EmptyHoodieRecordPayload());
            deleteRecord.setCurrentLocation(existing.getCurrentLocation());
            deleteRecord.seal();
            return Arrays.asList(deleteRecord, getTaggedRecord(merged, Option.empty())).iterator();
          }
        });
    return taggedUpdatingRecords.union(newRecords);
  }
}
