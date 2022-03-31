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

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.table.HoodieTable;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.execution.PartitionIdPassthrough;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

/**
 * Hoodie Index implementation backed by the record index present in the Metadata Table.
 */
public class SparkMetadataTableRecordIndex<T extends HoodieRecordPayload>
    extends HoodieIndex<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> {

  public SparkMetadataTableRecordIndex(HoodieWriteConfig config) {
    super(config);
  }

  @Override
  public HoodieData<HoodieRecord<T>> tagLocation(HoodieData<HoodieRecord<T>> records, HoodieEngineContext context,
      HoodieTable hoodieTable) {
    final int numFileGroups;
    try {
      HoodieTableMetaClient metaClient = HoodieTableMetadataUtil.getMetadataTableMetaClient(hoodieTable.getMetaClient());
      numFileGroups = HoodieTableMetadataUtil.getPartitionLatestMergedFileSlices(metaClient,
          MetadataPartitionType.RECORD_INDEX.partitionPath()).size();
    } catch (TableNotFoundException e) {
      // implies that metadata table has not been initialized yet (probably the first write on a new table)
      return records;
    }

    JavaRDD<HoodieRecord<T>> y = HoodieJavaRDD.getJavaRDD(records).keyBy(r -> HoodieTableMetadataUtil.mapRecordKeyToFileGroupIndex(r.getRecordKey(), numFileGroups))
        .partitionBy(new PartitionIdPassthrough(numFileGroups))
        .map(t -> t._2);
    ValidationUtils.checkState(y.getNumPartitions() <= numFileGroups);

    registry.ifPresent(r -> r.add(TAG_LOC_NUM_PARTITIONS, records.getNumPartitions()));
    return HoodieJavaRDD.of(y.mapPartitions(new LocationTagFunction(hoodieTable, registry)));
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
  class LocationTagFunction implements FlatMapFunction<Iterator<HoodieRecord<T>>, HoodieRecord<T>> {
    private HoodieTable hoodieTable;
    private Option<Registry> registry;

    public LocationTagFunction(HoodieTable hoodieTable, Option<Registry> registry) {
      this.hoodieTable = hoodieTable;
      this.registry = registry;
    }

    @Override
    public Iterator<HoodieRecord<T>> call(Iterator<HoodieRecord<T>> hoodieRecordIterator) {
      HoodieTimer timer = new HoodieTimer().startTimer();

      List<HoodieRecord<T>> taggedRecords = new ArrayList<>();
      Map<String, Integer> keyToIndexMap = new HashMap<>();
      while (hoodieRecordIterator.hasNext()) {
        HoodieRecord rec = hoodieRecordIterator.next();
        keyToIndexMap.put(rec.getRecordKey(), taggedRecords.size());
        taggedRecords.add(rec);
      }

      List<String> recordKeys = keyToIndexMap.keySet().stream().sorted().collect(Collectors.toList());
      try {
        Map<String, HoodieRecordGlobalLocation> recordIndexInfo = hoodieTable.getMetadataReader().readRecordIndex(recordKeys);
        HoodieTableMetaClient metaClient = hoodieTable.getMetaClient();

        for (Entry<String, HoodieRecordGlobalLocation> e : recordIndexInfo.entrySet()) {
          if (checkIfValidCommit(metaClient, e.getValue().getInstantTime())) {
            HoodieRecord rec = taggedRecords.get(keyToIndexMap.get(e.getKey()));
            rec.unseal();
            rec.setCurrentLocation(e.getValue());
            rec.seal();
          }
        }

        registry.ifPresent(r -> r.add(TAG_LOC_DURATION, timer.endTimer()));
        registry.ifPresent(r -> r.add(TAG_LOC_RECORD_COUNT, recordKeys.size()));
        registry.ifPresent(r -> r.add(TAG_LOC_HITS, recordIndexInfo.size()));
      } catch (UnsupportedOperationException e) {
        // This means that record index is not created yet
      }

      return taggedRecords.iterator();
    }
  }
}
