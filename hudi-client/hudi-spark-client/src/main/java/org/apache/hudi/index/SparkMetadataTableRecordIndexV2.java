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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.metadata.HoodieMetadataCommonUtils;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.table.HoodieTable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.execution.PartitionIdPassthrough;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import scala.Tuple2;

/**
 * Hoodie Index implementation backed by the record index present in the Metadata Table.
 */
public class SparkMetadataTableRecordIndexV2 extends HoodieIndex<Object, Object> {

  private static final Logger LOG = LogManager.getLogger(SparkMetadataTableRecordIndex.class);

  public SparkMetadataTableRecordIndexV2(HoodieWriteConfig config) {
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

    // repartition based on record level partition file groups in metadata table.
    JavaPairRDD<String, HoodieRecord<R>> y = HoodieJavaRDD.getJavaRDD(records)
        .keyBy(r -> HoodieMetadataCommonUtils.mapRecordKeyToFileGroupIndex(r.getRecordKey(), numFileGroups))
        .partitionBy(new PartitionIdPassthrough(numFileGroups))
        .mapToPair(t -> new Tuple2<>(t._2.getRecordKey(), t._2));
    ValidationUtils.checkState(y.getNumPartitions() <= numFileGroups);
    // get record keys to be looked up in metadata table's RLI
    JavaRDD<String> keys = y.map(rec -> rec._1);
    // look up in RLI
    JavaPairRDD<String, Option<HoodieRecordGlobalLocation>> recordKeysWithLocation = keys.mapPartitions(new LocationTagFunction(hoodieTable, Option.empty())).mapToPair(
        (PairFunction<Tuple2<String, Option<HoodieRecordGlobalLocation>>, String, Option<HoodieRecordGlobalLocation>>) entry -> entry);

    // join to tag incoming records
    return HoodieJavaRDD.of(y.join(recordKeysWithLocation).values().map((Function<Tuple2<HoodieRecord<R>, Option<HoodieRecordGlobalLocation>>, HoodieRecord<R>>) hoodieRecordLocationPair -> {
      // for new inserts, record location may not be present
      if (hoodieRecordLocationPair._2.isPresent()) {
        hoodieRecordLocationPair._1.unseal();
        hoodieRecordLocationPair._1.setCurrentLocation(hoodieRecordLocationPair._2.get());
        hoodieRecordLocationPair._1.seal();
      }
      return hoodieRecordLocationPair._1;
    }));
    // TODO: should we repartition the return value based on original partitioning scheme. if not, its partitioned based on
    //  record level index partition file group count in metadata table. So further parallel processing might have lesser spark partitions.
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
  class LocationTagFunction implements FlatMapFunction<Iterator<String>, Tuple2<String, Option<HoodieRecordGlobalLocation>>> {
    private HoodieTable hoodieTable;
    private Option<Registry> registry;

    public LocationTagFunction(HoodieTable hoodieTable, Option<Registry> registry) {
      this.hoodieTable = hoodieTable;
      this.registry = registry;
    }

    @Override
    public Iterator<Tuple2<String, Option<HoodieRecordGlobalLocation>>> call(Iterator<String> recordKeyIterator) {
      List<String> allKeys = new ArrayList<>();
      recordKeyIterator.forEachRemaining(allKeys::add);
      List<Tuple2<String, Option<HoodieRecordGlobalLocation>>> recordKeyToRecordLocation = new ArrayList<>();
      try {
        Map<String, HoodieRecordGlobalLocation> recordIndexInfo = hoodieTable.getMetadataReader().tagLocationForRecordKeys(allKeys);
        recordIndexInfo.forEach((k, v) -> {
          if (v.getInstantTime() != null) {
            recordKeyToRecordLocation.add(new Tuple2(k, Option.of(v)));
          } else {
            recordKeyToRecordLocation.add(new Tuple2(k, Option.empty()));
          }
        });
      } catch (UnsupportedOperationException e) {
        // This means that record index is not created yet
        LOG.error("UnsupportedOperationException thrown while reading records from record index in metadata table", e);
        throw new HoodieException("Unsupported operation exception thrown while reading from record index in Metadata table ", e);
      }
      return recordKeyToRecordLocation.iterator();
    }
  }
}