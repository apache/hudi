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

package org.apache.hudi.index.dataframe;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieSparkKVRecord;
import org.apache.hudi.common.model.HoodieSparkIndexRecord;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaDataFrame;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.io.HoodieKeyLocationFetchHandle;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.table.HoodieTable;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.index.HoodieIndexUtils.getLatestBaseFilesForAllPartitions;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * A simple index which reads interested fields(record key and partition path) from base files and
 * joins with incoming records to find the tagged location.
 */
public class HoodieSimpleIndex
    extends HoodieIndex<Object, Object> {

  protected final Option<BaseKeyGenerator> keyGeneratorOpt;

  public HoodieSimpleIndex(HoodieWriteConfig config, Option<BaseKeyGenerator> keyGeneratorOpt) {
    super(config);
    this.keyGeneratorOpt = keyGeneratorOpt;
  }

  @Override
  public HoodieData<WriteStatus> updateLocation(
      HoodieData<WriteStatus> writeStatuses, HoodieEngineContext context,
      HoodieTable hoodieTable) {
    return writeStatuses;
  }

  @Override
  public boolean rollbackCommit(String commitTime) {
    return true;
  }

  @Override
  public boolean isGlobal() {
    return false;
  }

  @Override
  public boolean canIndexLogFiles() {
    return false;
  }

  @Override
  public boolean isImplicitWithStorage() {
    return true;
  }

  @Override
  public <R> HoodieData<HoodieRecord<R>> tagLocation(
      HoodieData<HoodieRecord<R>> records, HoodieEngineContext context,
      HoodieTable hoodieTable) {
    Dataset<HoodieRecord<R>> taggedRecords = tagLocationInternal(
            HoodieJavaDataFrame.getDataFrame(records), (HoodieSparkEngineContext) context, hoodieTable);
    return HoodieJavaDataFrame.of(taggedRecords);
  }

  /**
   * Tags records location for incoming records.
   *
   * @param inputRecords {@link HoodieData} of incoming records
   * @param context      instance of {@link HoodieEngineContext} to use
   * @param hoodieTable  instance of {@link HoodieTable} to use
   * @return {@link HoodieData} of records with record locations set
   */
  protected <R> Dataset<HoodieRecord<R>> tagLocationInternal(
      Dataset<HoodieRecord<R>> inputRecords, HoodieSparkEngineContext context,
      HoodieTable hoodieTable) {
    if (config.getSimpleIndexUseCaching()) {
      String cacheLevel = new HoodieConfig(config.getProps())
              .getString(HoodieIndexConfig.SIMPLE_INDEX_INPUT_STORAGE_LEVEL_VALUE);
      inputRecords.persist(StorageLevel.fromString(cacheLevel));
    }

    int inputParallelism = inputRecords.queryExecution().sparkPlan().outputPartitioning().numPartitions();
    int configuredSimpleIndexParallelism = config.getSimpleIndexParallelism();
    // NOTE: Target parallelism could be overridden by the config
    int targetParallelism =
        configuredSimpleIndexParallelism > 0 ? configuredSimpleIndexParallelism : inputParallelism;
    Dataset<HoodieSparkKVRecord> keyedInputRecords = inputRecords.map(
            (MapFunction<HoodieRecord<R>, HoodieSparkKVRecord>) this::castToPair,
            Encoders.bean(HoodieSparkKVRecord.class));
    Dataset<HoodieSparkIndexRecord> existingLocationsOnTable =
        fetchRecordLocationsForAffectedPartitions(inputRecords.as(Encoders.bean(HoodieRecord.class)), context, hoodieTable,
            targetParallelism);

    String joinColumns = "recordKey";
    Encoder<HoodieRecord<R>> encoder = inputRecords.encoder();

    Dataset<HoodieRecord<R>> taggedRecords = keyedInputRecords
            .joinWith(existingLocationsOnTable, new Column(joinColumns), "left")
            .map((MapFunction<Tuple2<HoodieSparkKVRecord, HoodieSparkIndexRecord>, HoodieRecord<R>>)
                    this::tagAsNewRecordIfNeeded, encoder);

    if (config.getSimpleIndexUseCaching()) {
      inputRecords.unpersist();
    }
    return taggedRecords;
  }

  public HoodieSparkKVRecord castToPair(HoodieRecord inputRecord) {
    return HoodieSparkKVRecord.apply(inputRecord.getRecordKey(), (HoodieSparkRecord) inputRecord);
  }

  public <R> HoodieRecord<R> tagAsNewRecordIfNeeded(Tuple2<HoodieSparkKVRecord, HoodieSparkIndexRecord> row) {
    HoodieSparkRecord record = row._1.record();
    if (Option.of(row._2).isPresent()) {
      // When you have a record in multiple files in the same partition, then <row key, record> collection
      // will have 2 entries with the same exact in memory copy of the HoodieRecord and the 2
      // separate filenames that the record is found in. This will result in setting
      // currentLocation 2 times and it will fail the second time. So creating a new in memory
      // copy of the hoodie record.
      HoodieSparkRecord newRecord = record.newInstance();
      newRecord.unseal();
      newRecord.setCurrentLocation(row._2.recordLocation());
      newRecord.seal();
      return (HoodieRecord<R>) newRecord;
    } else {
      return (HoodieRecord<R>) record;
    }
  }

  /**
   * Fetch record locations for passed in {@link HoodieKey}s.
   *
   * @param records  {@link HoodieData} of {@link HoodieKey}s for which locations are fetched
   * @param context     instance of {@link HoodieEngineContext} to use
   * @param hoodieTable instance of {@link HoodieTable} of interest
   * @param parallelism parallelism to use
   * @return {@link HoodiePairData} of {@link HoodieKey} and {@link HoodieRecordLocation}
   */
  protected Dataset<HoodieSparkIndexRecord> fetchRecordLocationsForAffectedPartitions(
      Dataset<HoodieRecord> records, HoodieSparkEngineContext context, HoodieTable hoodieTable,
      int parallelism) {
    List<String> affectedPartitionPathList = records.select(HoodieRecord.PARTITION_PATH_METADATA_FIELD)
            .distinct()
            .collectAsList()
            .stream()
            .map(record -> record.getString(0))
            .collect(Collectors.toList());
    List<Pair<String, HoodieBaseFile>> latestBaseFiles =
        getLatestBaseFilesForAllPartitions(affectedPartitionPathList, context, hoodieTable);
    return fetchRecordLocations(context, hoodieTable, parallelism, latestBaseFiles);
  }

  protected Dataset<HoodieSparkIndexRecord> fetchRecordLocations(
          HoodieSparkEngineContext context, HoodieTable hoodieTable, int parallelism,
          List<Pair<String, HoodieBaseFile>> baseFiles) {
    int fetchParallelism = Math.max(1, Math.min(baseFiles.size(), parallelism));
    SparkSession session = context.getSqlContext().sparkSession();
    StructType columns = new StructType();
    columns.add("recordKey", StringType, true);
    columns.add("fileId", StringType, true);
    columns.add("instantTime", StringType, true);

    List<HoodieSparkIndexRecord> records = baseFiles.stream().flatMap(partitionPathBaseFile -> {
      Stream<Pair<HoodieKey, HoodieRecordLocation>> locations;
      locations = new HoodieKeyLocationFetchHandle(config, hoodieTable, partitionPathBaseFile, keyGeneratorOpt).locations();
      return locations;
    }).map(pair -> HoodieSparkIndexRecord.apply(pair.getLeft().getRecordKey(), new HoodieRecordLocation(pair.getRight().getInstantTime(), pair.getRight().getFileId()))).collect(Collectors.toList());

    return session.createDataset(records, Encoders.bean(HoodieSparkIndexRecord.class));
  }
}
