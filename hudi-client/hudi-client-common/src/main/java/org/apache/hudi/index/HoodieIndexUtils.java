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

import org.apache.hudi.client.common.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.HoodieIndex.IndexType;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * Hoodie Index Utilities.
 */
public class HoodieIndexUtils {
  private static final Logger LOG = LogManager.getLogger(HoodieIndexUtils.class);

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
    context.setJobStatus(HoodieIndexUtils.class.getSimpleName(), "Load latest base files from all partitions");
    return context.flatMap(partitions, partitionPath -> {
      Option<HoodieInstant> latestCommitTime = hoodieTable.getMetaClient().getCommitsTimeline()
          .filterCompletedInstants().lastInstant();
      List<Pair<String, HoodieBaseFile>> filteredFiles = new ArrayList<>();
      if (latestCommitTime.isPresent()) {
        filteredFiles = hoodieTable.getBaseFileOnlyView()
            .getLatestBaseFilesBeforeOrOn(partitionPath, latestCommitTime.get().getTimestamp())
            .map(f -> Pair.of(partitionPath, f))
            .collect(toList());
      }
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
    HoodieRecord record = inputRecord;
    if (location.isPresent()) {
      // When you have a record in multiple files in the same partition, then <row key, record> collection
      // will have 2 entries with the same exact in memory copy of the HoodieRecord and the 2
      // separate filenames that the record is found in. This will result in setting
      // currentLocation 2 times and it will fail the second time. So creating a new in memory
      // copy of the hoodie record.
      record = new HoodieRecord<>(inputRecord);
      record.unseal();
      record.setCurrentLocation(location.get());
      record.seal();
    }
    return record;
  }

  /**
   * Check compatible between new writeIndexType and indexType already in hoodie.properties.
   * @param writeIndexType new indexType
   * @param persistIndexType indexType already in hoodie.properties
   */
  public static void checkIndexTypeCompatible(IndexType writeIndexType, IndexType persistIndexType) {
    boolean isTypeCompatible = false;
    switch (persistIndexType) {
      case GLOBAL_BLOOM:
        if (writeIndexType.equals(IndexType.GLOBAL_BLOOM)
            || writeIndexType.equals(IndexType.BLOOM)
            || writeIndexType.equals(IndexType.SIMPLE)
            || writeIndexType.equals(IndexType.GLOBAL_SIMPLE)) {
          isTypeCompatible = true;
        }
        break;
      case GLOBAL_SIMPLE:
        if (writeIndexType.equals(IndexType.GLOBAL_SIMPLE)
            || writeIndexType.equals(IndexType.GLOBAL_BLOOM)) {
          isTypeCompatible = true;
        }
        break;
      case SIMPLE:
        if (writeIndexType.equals(IndexType.SIMPLE)) {
          isTypeCompatible = true;
        }
        break;
      case BLOOM:
        if (writeIndexType.equals(IndexType.BLOOM)) {
          isTypeCompatible = true;
        }
        break;
      case INMEMORY:
        if (writeIndexType.equals(IndexType.INMEMORY)) {
          isTypeCompatible = true;
        }
        LOG.error("PersistIndexType INMEMORY can not be used in production");
        break;
      case HBASE:
        if (writeIndexType.equals(IndexType.HBASE)) {
          isTypeCompatible = true;
        }
        break;
      case CUSTOM:
        if (writeIndexType.equals(IndexType.CUSTOM)) {
          isTypeCompatible = true;
        }
        break;
      default:
        throw new HoodieIndexException("Index type" + persistIndexType + " unspecified in  properties file");
    }
    if (!isTypeCompatible) {
      throw new HoodieException("The new write indextype " + writeIndexType
            + " is not compatible with persistIndexType " + persistIndexType);
    }
  }
}
