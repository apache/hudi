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

package org.apache.hudi.index.simple;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.HoodieKeyLocationFetchHandle;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.table.HoodieTable;

import java.util.List;

import static org.apache.hudi.common.model.HoodieTableType.MERGE_ON_READ;
import static org.apache.hudi.index.HoodieIndexUtils.getLatestBaseFilesForAllPartitions;
import static org.apache.hudi.index.HoodieIndexUtils.tagGlobalLocationBackToRecords;

/**
 * A global simple index which reads interested fields(record key and partition path) from base files and
 * joins with incoming records to find the tagged location.
 */
public class HoodieGlobalSimpleIndex extends HoodieSimpleIndex {
  public HoodieGlobalSimpleIndex(HoodieWriteConfig config, Option<BaseKeyGenerator> keyGeneratorOpt) {
    super(config, keyGeneratorOpt);
  }

  @Override
  public <R> HoodieData<HoodieRecord<R>> tagLocation(
      HoodieData<HoodieRecord<R>> records, HoodieEngineContext context,
      HoodieTable hoodieTable) {
    return tagLocationInternal(records, context, hoodieTable);
  }

  /**
   * Tags records location for incoming records.
   *
   * @param inputRecords {@link HoodieData} of incoming records
   * @param context      instance of {@link HoodieEngineContext} to use
   * @param hoodieTable  instance of {@link HoodieTable} to use
   * @return {@link HoodieData} of records with record locations set
   */
  @Override
  protected <R> HoodieData<HoodieRecord<R>> tagLocationInternal(
      HoodieData<HoodieRecord<R>> inputRecords, HoodieEngineContext context,
      HoodieTable hoodieTable) {
    List<Pair<String, HoodieBaseFile>> latestBaseFiles = getAllBaseFilesInTable(context, hoodieTable);
    HoodiePairData<String, HoodieRecordGlobalLocation> allKeysAndLocations =
        fetchRecordGlobalLocations(context, hoodieTable, latestBaseFiles);
    boolean mayContainDuplicateLookup = hoodieTable.getMetaClient().getTableType() == MERGE_ON_READ;
    boolean shouldUpdatePartitionPath = config.getGlobalSimpleIndexUpdatePartitionPath() && hoodieTable.isPartitioned();
    return tagGlobalLocationBackToRecords(inputRecords, allKeysAndLocations,
        mayContainDuplicateLookup, shouldUpdatePartitionPath, config, hoodieTable);
  }

  private HoodiePairData<String, HoodieRecordGlobalLocation> fetchRecordGlobalLocations(
      HoodieEngineContext context, HoodieTable hoodieTable,
      List<Pair<String, HoodieBaseFile>> baseFiles) {
    int parallelism = getParallelism(config.getGlobalSimpleIndexParallelism(), baseFiles.size());

    return context.parallelize(baseFiles, parallelism)
        .flatMap(partitionPathBaseFile -> new HoodieKeyLocationFetchHandle(config, hoodieTable, partitionPathBaseFile, keyGeneratorOpt)
            .globalLocations())
        .mapToPair(e -> (Pair<String, HoodieRecordGlobalLocation>) e);
  }

  /**
   * Load all files for all partitions as <Partition, filename> pair data.
   */
  private List<Pair<String, HoodieBaseFile>> getAllBaseFilesInTable(
      final HoodieEngineContext context, final HoodieTable hoodieTable) {
    HoodieTableMetaClient metaClient = hoodieTable.getMetaClient();
    List<String> allPartitionPaths = FSUtils.getAllPartitionPaths(context, metaClient, config.getMetadataConfig());
    // Obtain the latest data files from all the partitions.
    return getLatestBaseFilesForAllPartitions(allPartitionPaths, context, hoodieTable);
  }

  @Override
  public boolean isGlobal() {
    return true;
  }
}
