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

package org.apache.hudi.index;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.metadata.HoodieMetadataCommonUtils;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.table.HoodieTable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.execution.PartitionIdPassthrough;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * Hoodie Index implementation backed by the record index present in the Metadata Table.
 */
public class SparkMetadataTableRecordIndex extends HoodieIndex<Object, Object> {

  private static final Logger LOG = LogManager.getLogger(SparkMetadataTableRecordIndex.class);

  public SparkMetadataTableRecordIndex(HoodieWriteConfig config) {
    super(config);
  }

  @Override
  public <R> HoodieData<HoodieRecord<R>> tagLocation(HoodieData<HoodieRecord<R>> records, HoodieEngineContext context,
                                                     HoodieTable hoodieTable) {
    final int numFileGroups;
    try {
      HoodieTableMetaClient metadataTableMetaClient = HoodieMetadataCommonUtils.getMetadataTableMetaClient(hoodieTable.getMetaClient());
      numFileGroups = HoodieMetadataCommonUtils.getPartitionLatestMergedFileSlices(metadataTableMetaClient,
          HoodieMetadataCommonUtils.getFileSystemView(metadataTableMetaClient), MetadataPartitionType.RECORD_INDEX.getPartitionPath()).size();
    } catch (TableNotFoundException e) {
      // implies that metadata table has not been initialized yet (probably the first write on a new table)
      return records;
    }
    Random random = new Random(0xDEEAD);
    int totalParallelism = Math.max(numFileGroups, config.getUpsertShuffleParallelism());
    JavaRDD<HoodieRecord<R>> y2 = HoodieJavaRDD.getJavaRDD(records)
        .keyBy(r -> HoodieMetadataCommonUtils.mapRecordKeyToSparkPartition(r.getRecordKey(), numFileGroups, config.getUpsertShuffleParallelism(), random))
        .partitionBy(new PartitionIdPassthrough(totalParallelism))
        .map(t -> t._2);

    return HoodieJavaRDD.of(y2.mapPartitions(new LocationTagFunction(hoodieTable, Option.empty())));
  }

  @Override
  public HoodieData<WriteStatus> updateLocation(HoodieData<WriteStatus> writeStatuses, HoodieEngineContext context,
                                                HoodieTable hoodieTable) {
    // This is a no-op as metadata record index updates are automatically maintained within the metadata table.
    return writeStatuses;
  }

  @Override
  public boolean rollbackCommit(String instantTime) {
    return true;
  }

  @Override
  public boolean isGlobal() {
    return true;
  }

  @Override
  public boolean canIndexLogFiles() {
    return false;
  }

  @Override
  public boolean isImplicitWithStorage() {
    return false;
  }

  /**
   * Function that tags each HoodieRecord with an existing location, if known.
   */
  class LocationTagFunction<R> implements FlatMapFunction<Iterator<HoodieRecord<R>>, HoodieRecord<R>> {
    private HoodieTable hoodieTable;
    private Option<Registry> registry;

    public LocationTagFunction(HoodieTable hoodieTable, Option<Registry> registry) {
      this.hoodieTable = hoodieTable;
      this.registry = registry;
    }

    @Override
    public Iterator<HoodieRecord<R>> call(Iterator<HoodieRecord<R>> hoodieRecordIterator) {
      HoodieTimer timer = new HoodieTimer().startTimer();

      List<HoodieRecord<R>> taggedRecords = new ArrayList<>();
      Map<String, Integer> keyToIndexMap = new HashMap<>();
      while (hoodieRecordIterator.hasNext()) {
        HoodieRecord rec = hoodieRecordIterator.next();
        keyToIndexMap.put(rec.getRecordKey(), taggedRecords.size());
        taggedRecords.add(rec);
      }

      List<String> recordKeys = keyToIndexMap.keySet().stream().sorted().collect(Collectors.toList());
      try {
        Map<String, HoodieRecordGlobalLocation> recordIndexInfo = hoodieTable.getMetadataReader().tagLocationForRecordKeys(recordKeys);

        for (Entry<String, HoodieRecordGlobalLocation> e : recordIndexInfo.entrySet()) {
          // TODO: fix me. handle new inserts (no record location).
          // TODO: do we really need to check valid commit. since metadata read would have already resolved the inflight commits.
          if (e.getValue().getInstantTime() != null && checkIfValidCommit(metaClient, e.getValue().getInstantTime())) {
            HoodieRecord rec = taggedRecords.get(keyToIndexMap.get(e.getKey()));
            rec.unseal();
            rec.setCurrentLocation(e.getValue());
            rec.seal();
          }
        }

        // registry.ifPresent(r -> r.add(TAG_LOC_DURATION, timer.endTimer()));
        // registry.ifPresent(r -> r.add(TAG_LOC_RECORD_COUNT, recordKeys.size()));
        // registry.ifPresent(r -> r.add(TAG_LOC_HITS, recordIndexInfo.size()));
      } catch (UnsupportedOperationException e) {
        // This means that record index is not created yet
        LOG.error("UnsupportedOperationException thrown while reading records from record index in metadata table", e);
        throw new HoodieException("Unsupported operation exception thrown while reading from record index in Metadata table ", e);
      }
      return taggedRecords.iterator();
    }
  }

}