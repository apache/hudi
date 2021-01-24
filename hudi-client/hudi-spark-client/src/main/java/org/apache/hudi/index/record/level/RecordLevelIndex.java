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

package org.apache.hudi.index.record.level;

import org.apache.hudi.avro.model.HoodieRecordLevelIndexRecord;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieMetricsConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.index.HoodieIndexUtils;
import org.apache.hudi.index.HoodieRecordLevelIndexPayload;
import org.apache.hudi.index.SparkHoodieIndex;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.util.Utils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import scala.Tuple2;

/**
 * Record level index for Hoodie dataset. This index is backed by hoodie's MOR table.
 * Base path of the RecordLevelIndex Table relative to the dataset is ".hoodie/index"
 * Index information is stored in a hoodie table(index table) with statically allocated partitions.
 * Tagging will look up the table and return the values
 * UpdateLocation will ingest indexing information for records into index table.
 *
 * Record Key: {<data_record_key>_<data_partition_path>}
 * Partition Path: index partition found by hash partitioning
 * Data contains: key, partition_path, instantTime and fileId. Check {@link HoodieRecordLevelIndexRecord} for more info.
 * Hashing is based on original HoodieKey.toString(). This hashing will assist in determining the partition in index table.
 * @param <T>
 */
public class RecordLevelIndex<T extends HoodieRecordPayload> extends SparkHoodieIndex<T> {

  // Table name suffix
  private static final String RECORD_LEVEL_INDEX_TABLE_NAME_SUFFIX = "index";

  // Base path of the RecordLevelIndex Table relative to the dataset (.hoodie/index)
  private static final String RECORD_LEVEL_INDEX_TABLE_REL_PATH = HoodieTableMetaClient.METAFOLDER_NAME + Path.SEPARATOR + RECORD_LEVEL_INDEX_TABLE_NAME_SUFFIX;

  private static final Logger LOG = LogManager.getLogger(RecordLevelIndex.class);

  private final HoodieWriteConfig datasetWriteConfig;
  private final HoodieWriteConfig recordLevelIndexWriteConfig;
  private final SerializableConfiguration hadoopConf;
  protected final transient HoodieEngineContext engineContext;
  private HoodieTableMetaClient datasetMetaClient;

  private String indexTableName;
  private int numIndexPartitions = 1;

  public RecordLevelIndex(SerializableConfiguration hadoopConf, HoodieWriteConfig writeConfig, HoodieEngineContext engineContext) {
    super(writeConfig);
    this.datasetWriteConfig = writeConfig;
    this.engineContext = engineContext;
    this.hadoopConf = hadoopConf;
    this.indexTableName = writeConfig.getTableName() + RECORD_LEVEL_INDEX_TABLE_NAME_SUFFIX;
    this.recordLevelIndexWriteConfig = createRecordLevelIndexWriteConfig(writeConfig);
    this.numIndexPartitions = writeConfig.getNumPartitionsForRecordLevelIndex();
    this.datasetMetaClient = new HoodieTableMetaClient(hadoopConf.get(), writeConfig.getBasePath());
  }

  @Override
  public boolean rollbackCommit(String instantTime) {
    try (SparkRDDWriteClient writeClient = new SparkRDDWriteClient(engineContext, recordLevelIndexWriteConfig, true)) {
      return writeClient.rollback(instantTime);
    }
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
    return false;
  }

  @Override
  public JavaRDD<WriteStatus> updateLocation(JavaRDD<WriteStatus> writeStatusRDD, HoodieEngineContext context,
      HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> hoodieTable, String instantTime) throws HoodieIndexException {

    // prepare records to be updated to index table

    // fetch records from written records in write status.
    // We need a pair of HoodieKey.toString() to IndexRecord. HoodieKey.toString to do hash partitioning
    JavaPairRDD<String, HoodieRecord> recordsToBeUpdated =
        writeStatusRDD.flatMapToPair((PairFlatMapFunction<WriteStatus, String, HoodieRecord>) writeStatus -> {
          List<Tuple2<String, HoodieRecord>> toReturn = new ArrayList<>();
          for (HoodieRecord record : writeStatus.getWrittenRecords()) {
            // we need to update only written records from WriteStatus
            if (!writeStatus.isErrored(record.getKey()) && record.getNewLocation().isPresent() && record.getCurrentLocation() == null) {
              HoodieRecordLocation recordLocation = (HoodieRecordLocation) record.getNewLocation().get();
              HoodieRecordLevelIndexRecord recordLevelIndexRecord =
                  new HoodieRecordLevelIndexRecord((record.getRecordKey() + "_" + record.getPartitionPath()), record.getPartitionPath(), recordLocation.getInstantTime(), recordLocation.getFileId());
              toReturn.add(new Tuple2(record.getKey().toString(), new HoodieRecord<>(record.getKey(), new HoodieRecordLevelIndexPayload(Option.of(recordLevelIndexRecord)))));
            }
          }
          return toReturn.iterator();
        });

    // repartition with numBuckets and sort within partitions
    JavaPairRDD<String, HoodieRecord> sortedRecordsRdd
        = recordsToBeUpdated.repartitionAndSortWithinPartitions(new HashPartitioner(numIndexPartitions));

    // Format of HoodieKey in index table ("<data_record_key>_<data_partition_path>", "<partitionPath in index table>"). so we fix hoodie key to have the right values.
    // we can't avoid this mapPartitionsWithIndex call, since we need the partition index in index table to be injected into HoodieKey for every record.
    JavaRDD<HoodieRecord> indexRecords =
        sortedRecordsRdd.values().mapPartitionsWithIndex(new Function2<Integer, Iterator<HoodieRecord>, Iterator<HoodieRecord>>() {
          @Override
          public Iterator<HoodieRecord> call(Integer partitionIndex, Iterator<HoodieRecord> hoodieRecordIterator) throws Exception {
            String partitionPath = Integer.toString(partitionIndex);
            List<HoodieRecord> toReturn = new ArrayList<>();
            while (hoodieRecordIterator.hasNext()) {
              HoodieRecord incomingRecord = hoodieRecordIterator.next();
              toReturn.add(new HoodieRecord(new HoodieKey((incomingRecord.getRecordKey() + "_" + incomingRecord.getPartitionPath()), partitionPath),
                  incomingRecord.getData()));
            }
            return toReturn.iterator();
          }
        }, true);

    // write to index table
    try (SparkRDDWriteClient writeClient = new SparkRDDWriteClient(engineContext, recordLevelIndexWriteConfig, true)) {
      // start commit with same instant as data table
      writeClient.startCommitWithTime(instantTime);
      JavaRDD<WriteStatus> indexUpdateWriteStatuses = writeClient.upsert(indexRecords, instantTime);
      List<WriteStatus> result = indexUpdateWriteStatuses.collect();
      // TODO: what do we need to do w/ the result? parse and throw error if failed
      // return original WriteStatus from data table)
      return writeStatusRDD;
    }
  }

  @Override
  public JavaRDD<WriteStatus> updateLocation(JavaRDD<WriteStatus> writeStatusRDD, HoodieEngineContext context,
      HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> hoodieTable) throws HoodieIndexException {
    return updateLocation(writeStatusRDD, context, hoodieTable, null);
  }

  @Override
  public JavaRDD<HoodieRecord<T>> tagLocation(JavaRDD<HoodieRecord<T>> records, HoodieEngineContext context,
      HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> hoodieTable) throws HoodieIndexException {
    datasetMetaClient = HoodieTableMetaClient.reload(datasetMetaClient);
    // in the end, we need to join w/ incoming record to tag them, hence creating a map here.
    JavaPairRDD<HoodieKey, HoodieRecord<T>> incomingRecords = records.mapToPair(entry -> new Tuple2<>(entry.getKey(), entry));
    try {
      // repartition with HoodieKey based partitioning and sort within each partition.
      JavaPairRDD<HoodieKey, HoodieRecord<T>> partitionedSortedRDD = incomingRecords.repartitionAndSortWithinPartitions(new HoodieKeyHashPartitioner(numIndexPartitions), new HoodieKeyComparator());

      JavaRDD<Tuple2<HoodieKey, Option<Tuple2<String, HoodieRecordLocation>>>> keyToLocationRdd = !datasetWriteConfig.enableSeekForRecordLevelIndex()
          ? // if seek is not enabled, use HoodieRecordLevelIndexScanner which reads all records and looks up all keys together
          partitionedSortedRDD.mapPartitionsWithIndex(
              (Function2<Integer, Iterator<Tuple2<HoodieKey, HoodieRecord<T>>>, Iterator<Tuple2<HoodieKey, Option<Tuple2<String, HoodieRecordLocation>>>>>) (partitionIndex, recordItr) -> {
                List<HoodieKey> keysToLookup = new ArrayList<>();
                while (recordItr.hasNext()) {
                  keysToLookup.add(recordItr.next()._1);
                }
                return new HoodieRecordLevelIndexScanner(datasetWriteConfig, recordLevelIndexWriteConfig, datasetMetaClient, indexTableName,
                    hadoopConf, keysToLookup, partitionIndex).getRecordLocations().iterator();
              }, true)
          // if seek is enabled, use HoodieRecordLevelIndexLookupIterator which does per key lookup
          : partitionedSortedRDD.mapPartitionsWithIndex(
              new HoodieRecordLevelIndexLookupFunction(datasetWriteConfig, recordLevelIndexWriteConfig, datasetMetaClient, hadoopConf), true);

      // conver to JavaPairRDD to join w/ incoming records
      JavaPairRDD<HoodieKey, Option<Tuple2<String, HoodieRecordLocation>>> keyToLocationPairRdd = keyToLocationRdd.mapToPair(entry -> new Tuple2<>(entry._1, entry._2));

      // join with incoming records to tag them.
      return incomingRecords.join(keyToLocationPairRdd).values()
          .map(v1 -> HoodieIndexUtils.getTaggedRecord(v1._1,
              v1._2.isPresent() ? Option.ofNullable(v1._2.get()._2) : Option.empty()));
    } catch (Exception e) {
      throw new HoodieIndexException("Exception thrown while index lookup with RecordLevel Index ", e);
    }
  }

  /**
   * Create a {@code HoodieWriteConfig} to use for the Index Table. // TODO: copied from Metadata code w/ minimal changes required for index. Need to revisit every config set.
   *
   * @param writeConfig {@code HoodieWriteConfig} of the main dataset writer
   */
  private HoodieWriteConfig createRecordLevelIndexWriteConfig(HoodieWriteConfig writeConfig) {
    int parallelism = writeConfig.getMetadataInsertParallelism();

    // Create the write config for the record level index table by borrowing options from the main write config.
    HoodieWriteConfig.Builder builder = HoodieWriteConfig.newBuilder()
        .withTimelineLayoutVersion(TimelineLayoutVersion.CURR_VERSION)
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder()
            .withConsistencyCheckEnabled(writeConfig.getConsistencyGuardConfig().isConsistencyCheckEnabled())
            .withInitialConsistencyCheckIntervalMs(writeConfig.getConsistencyGuardConfig().getInitialConsistencyCheckIntervalMs())
            .withMaxConsistencyCheckIntervalMs(writeConfig.getConsistencyGuardConfig().getMaxConsistencyCheckIntervalMs())
            .withMaxConsistencyChecks(writeConfig.getConsistencyGuardConfig().getMaxConsistencyChecks())
            .build())
        .withAutoCommit(true)
        .withAvroSchemaValidate(true)
        .withEmbeddedTimelineServerEnabled(false)
        .withPath(writeConfig.getBasePath() + Path.SEPARATOR + RECORD_LEVEL_INDEX_TABLE_REL_PATH)
        .withSchema(HoodieRecordLevelIndexRecord.getClassSchema().toString())
        .forTable(indexTableName)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withAsyncClean(writeConfig.isMetadataAsyncClean())
            // we will trigger cleaning manually, to control the instant times
            .withAutoClean(false)
            .withCleanerParallelism(parallelism)
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS)
            .retainCommits(writeConfig.getMetadataCleanerCommitsRetained())
            .archiveCommitsWith(writeConfig.getMetadataMinCommitsToKeep(), writeConfig.getMetadataMaxCommitsToKeep())
            // we will trigger compaction manually, to control the instant times
            .withInlineCompaction(false)
            .withMaxNumDeltaCommitsBeforeCompaction(writeConfig.getMetadataCompactDeltaCommitMax()).build())
        .withParallelism(parallelism, parallelism)
        .withDeleteParallelism(parallelism)
        .withRollbackParallelism(parallelism)
        .withFinalizeWriteParallelism(parallelism);

    if (writeConfig.isMetricsOn()) {
      HoodieMetricsConfig.Builder metricsConfig = HoodieMetricsConfig.newBuilder()
          .withReporterType(writeConfig.getMetricsReporterType().toString())
          .withExecutorMetrics(writeConfig.isExecutorMetricsEnabled())
          .on(true);
      switch (writeConfig.getMetricsReporterType()) {
        case GRAPHITE:
          metricsConfig.onGraphitePort(writeConfig.getGraphiteServerPort())
              .toGraphiteHost(writeConfig.getGraphiteServerHost())
              .usePrefix(writeConfig.getGraphiteMetricPrefix());
          break;
        case JMX:
          metricsConfig.onJmxPort(writeConfig.getJmxPort())
              .toJmxHost(writeConfig.getJmxHost());
          break;
        case DATADOG:
          // TODO:
          break;
        case CONSOLE:
        case INMEMORY:
          break;
        default:
          throw new HoodieMetadataException("Unsupported Metrics Reporter type " + writeConfig.getMetricsReporterType());
      }

      builder.withMetricsConfig(metricsConfig.build());
    }

    return builder.build();
  }

  /**
   * Hoodie key based hash Partitioner.
   */
  class HoodieKeyHashPartitioner extends Partitioner {

    int numPartitions;

    public HoodieKeyHashPartitioner(Integer partitions) {
      this.numPartitions = partitions;
    }

    public boolean equals(HoodieKeyHashPartitioner other) {
      return this.numPartitions == other.numPartitions;
    }

    public int hashCode() {
      return numPartitions;
    }

    @Override
    public int numPartitions() {
      return numPartitions;
    }

    @Override
    public int getPartition(Object key) {
      if (key == null) {
        return 0;
      } else if (key instanceof HoodieKey) {
        return Utils.nonNegativeMod(key.toString().hashCode(), numPartitions);
      }
      return 0;
    }
  }

}
