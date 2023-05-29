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
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.MetadataValues;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.io.HoodieMergedReadHandle;
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
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import static java.util.stream.Collectors.toList;
import static org.apache.hudi.table.action.commit.HoodieDeleteHelper.createDeleteRecord;

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

  public static HoodieIndex createUserDefinedIndex(HoodieWriteConfig config) {
    Object instance = ReflectionUtils.loadClass(config.getIndexClass(), config);
    if (!(instance instanceof HoodieIndex)) {
      throw new HoodieIndexException(config.getIndexClass() + " is not a subclass of HoodieIndex");
    }
    return (HoodieIndex) instance;
  }

  /**
   * Read existing records based on the given partition path and {@link HoodieRecordLocation} info.
   * <p>
   * This will perform merged read for MOR table, in case a FileGroup contains log files.
   *
   * @return {@link HoodieRecord}s that have the current location being set.
   */
  private static <R> HoodieData<HoodieRecord<R>> getExistingRecords(
      HoodieData<Pair<String, HoodieRecordLocation>> partitionLocations, HoodieWriteConfig config, HoodieTable hoodieTable) {
    final Option<String> instantTime = hoodieTable
        .getMetaClient()
        .getCommitsTimeline()
        .filterCompletedInstants()
        .lastInstant()
        .map(HoodieInstant::getTimestamp);
    return partitionLocations.flatMap(p -> {
      String partitionPath = p.getLeft();
      String fileId = p.getRight().getFileId();
      return new HoodieMergedReadHandle(config, instantTime, hoodieTable, Pair.of(partitionPath, fileId))
          .getMergedRecords().iterator();
    });
  }

  /**
   * Merge the incoming record with the matching existing record loaded via {@link HoodieMergedReadHandle}. The existing record is the latest version in the table.
   */
  private static <R> Option<HoodieRecord<R>> mergeIncomingWithExistingRecord(HoodieRecord<R> incoming, HoodieRecord<R> existing, Schema writeSchema, HoodieWriteConfig config) throws IOException {
    Schema existingSchema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(config.getSchema()), config.allowOperationMetadataField());
    Schema writeSchemaWithMetaFields = HoodieAvroUtils.addMetadataFields(writeSchema, config.allowOperationMetadataField());
    // prepend the hoodie meta fields as the incoming record does not have them
    HoodieRecord incomingPrepended = incoming
        .prependMetaFields(writeSchema, writeSchemaWithMetaFields, new MetadataValues().setRecordKey(incoming.getRecordKey()).setPartitionPath(incoming.getPartitionPath()), config.getProps());
    // after prepend the meta fields, convert the record back to the original payload
    HoodieRecord incomingWithMetaFields = incomingPrepended
        .wrapIntoHoodieRecordPayloadWithParams(writeSchema, config.getProps(), Option.empty(), config.allowOperationMetadataField(), Option.empty(), false, Option.empty());
    Option<Pair<HoodieRecord, Schema>> mergeResult = config.getRecordMerger()
        .merge(existing, existingSchema, incomingWithMetaFields, writeSchemaWithMetaFields, config.getProps());
    if (mergeResult.isPresent()) {
      // the merged record needs to be converted back to the original payload
      HoodieRecord<R> merged = mergeResult.get().getLeft().wrapIntoHoodieRecordPayloadWithParams(
          writeSchemaWithMetaFields, config.getProps(), Option.empty(),
          config.allowOperationMetadataField(), Option.empty(), false, Option.of(writeSchema));
      return Option.of(merged);
    } else {
      return Option.empty();
    }
  }

  /**
   * Merge tagged incoming records with existing records in case of partition path updated.
   */
  public static <R> HoodieData<HoodieRecord<R>> mergeForPartitionUpdates(
      HoodieData<Pair<HoodieRecord<R>, Option<Pair<String, HoodieRecordLocation>>>> taggedHoodieRecords, HoodieWriteConfig config, HoodieTable hoodieTable) {
    // completely new records
    HoodieData<HoodieRecord<R>> newRecords = taggedHoodieRecords.filter(p -> !p.getRight().isPresent()).map(Pair::getLeft);
    // the records tagged to existing base files
    HoodieData<HoodieRecord<R>> updatingRecords = taggedHoodieRecords.filter(p -> p.getRight().isPresent()).map(Pair::getLeft)
        .distinctWithKey(HoodieRecord::getRecordKey, config.getGlobalIndexReconcileParallelism());
    // the tagging partitions and locations
    HoodieData<Pair<String, HoodieRecordLocation>> partitionLocations = taggedHoodieRecords
        .filter(p -> p.getRight().isPresent())
        .map(p -> p.getRight().get())
        .distinct(config.getGlobalIndexReconcileParallelism());
    // merged existing records with current locations being set
    HoodieData<HoodieRecord<R>> existingRecords = getExistingRecords(partitionLocations, config, hoodieTable);

    HoodieData<HoodieRecord<R>> taggedUpdatingRecords = updatingRecords.mapToPair(r -> Pair.of(r.getRecordKey(), r))
        .leftOuterJoin(existingRecords.mapToPair(r -> Pair.of(r.getRecordKey(), r)))
        .values().flatMap(entry -> {
          HoodieRecord<R> incoming = entry.getLeft();
          Option<HoodieRecord<R>> existingOpt = entry.getRight();
          if (!existingOpt.isPresent()) {
            // existing record not found (e.g., due to delete log not merged to base file): tag as a new record
            return Collections.singletonList(getTaggedRecord(incoming, Option.empty())).iterator();
          }
          HoodieRecord<R> existing = existingOpt.get();
          Schema writeSchema = new Schema.Parser().parse(config.getWriteSchema());
          if (incoming.isDelete(writeSchema, config.getProps())) {
            // incoming is a delete: force tag the incoming to the old partition
            return Collections.singletonList(getTaggedRecord(incoming.newInstance(existing.getKey()), Option.of(existing.getCurrentLocation()))).iterator();
          }

          Option<HoodieRecord<R>> mergedOpt = mergeIncomingWithExistingRecord(incoming, existing, writeSchema, config);
          if (!mergedOpt.isPresent()) {
            // merge resulted in delete: force tag the incoming to the old partition
            return Collections.singletonList(getTaggedRecord(incoming.newInstance(existing.getKey()), Option.of(existing.getCurrentLocation()))).iterator();
          }
          HoodieRecord<R> merged = mergedOpt.get();
          if (Objects.equals(merged.getPartitionPath(), existing.getPartitionPath())) {
            // merged record has the same partition: route the merged result to the current location as an update
            return Collections.singletonList(getTaggedRecord(merged, Option.of(existing.getCurrentLocation()))).iterator();
          } else {
            // merged record has a different partition: issue a delete to the old partition and insert the merged record to the new partition
            HoodieRecord<R> deleteRecord = createDeleteRecord(config, existing.getKey());
            deleteRecord.setCurrentLocation(existing.getCurrentLocation());
            deleteRecord.seal();
            return Arrays.asList(deleteRecord, getTaggedRecord(merged, Option.empty())).iterator();
          }
        });
    return taggedUpdatingRecords.union(newRecords);
  }
}
