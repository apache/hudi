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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.io.storage.HoodieAvroFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static java.util.stream.Collectors.toList;

/**
 * Hoodie Index Utilities.
 */
public class HoodieIndexUtils {

  private static final Logger LOG = LogManager.getLogger(HoodieIndexUtils.class);

  /**
   * Fetches Pair of partition path and {@link HoodieBaseFile}s for interested partitions.
   *
   * @param partition   Partition of interest
   * @param hoodieTable Instance of {@link HoodieTable} of interest
   * @return the list of {@link HoodieBaseFile}
   */
  public static List<HoodieBaseFile> getLatestBaseFilesForPartition(
      final String partition,
      final HoodieTable hoodieTable) {
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
  public static HoodieRecord getTaggedRecord(HoodieRecord inputRecord, Option<HoodieRecordLocation> location) {
    HoodieRecord<?> record = inputRecord;
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
    try {
      // Load all rowKeys from the file, to double-confirm
      if (!candidateRecordKeys.isEmpty()) {
        HoodieTimer timer = new HoodieTimer().startTimer();
        HoodieAvroFileReader fileReader = HoodieFileReaderFactory.getFileReader(configuration, filePath);
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
}
