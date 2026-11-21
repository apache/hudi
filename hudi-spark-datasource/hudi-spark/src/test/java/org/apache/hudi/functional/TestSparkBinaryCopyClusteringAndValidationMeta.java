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

package org.apache.hudi.functional;

import org.apache.hudi.avro.HoodieAvroWriteSupport;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieSliceInfo;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.clustering.run.strategy.SparkBinaryCopyClusteringExecutionStrategy;
import org.apache.hudi.client.clustering.run.strategy.SparkStreamCopyClusteringExecutionStrategy;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.avro.HoodieBloomFilterWriteSupport;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieParquetConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.ClusteringGroupInfo;
import org.apache.hudi.common.model.ClusteringOperation;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.MetaFieldsMode;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.storage.hadoop.HoodieAvroParquetWriter;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.util.TaskFailureListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.hudi.common.bloom.BloomFilterTypeCode.DYNAMIC_V0;
import static org.apache.hudi.common.bloom.BloomFilterTypeCode.SIMPLE;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.AVRO_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.HOODIE_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_NESTED_EXAMPLE_SCHEMA;
import static org.apache.hudi.config.HoodieClusteringConfig.PLAN_STRATEGY_SORT_COLUMNS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("functional")
public class TestSparkBinaryCopyClusteringAndValidationMeta extends HoodieClientTestBase {

  private static final String PARTITION_PATH = "2015/03/16";

  private FileSystem fs;

  @BeforeEach
  public void setUp() throws IOException {
    initPath();
    initSparkContexts();
    metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);
    this.fs = (FileSystem) metaClient.getStorage().getFileSystem();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  @Test
  public void testAndValidateClusteringOutputFiles() throws IOException {
    String partitionPath = "2015/03/16";
    Properties properties = new Properties();
    properties.setProperty("hoodie.parquet.small.file.limit", "-1");
    HoodieWriteConfig.Builder cfgBuilder = new HoodieWriteConfig.Builder()
        .withPath(basePath)
        .withSchema(TRIP_NESTED_EXAMPLE_SCHEMA)
        .withEmbeddedTimelineServerEnabled(false)
        .withClusteringConfig(
            HoodieClusteringConfig
                .newBuilder()
                .withInlineClustering(true)
                .withAsyncClustering(false)
                .withInlineClusteringNumCommits(2)
                .withClusteringExecutionStrategyClass(SparkBinaryCopyClusteringExecutionStrategy.class.getName())
                .build()).withProps(properties);
    SparkRDDWriteClient client = getHoodieWriteClient(cfgBuilder.build());
    String newCommitTime1 = client.startCommit("commit");
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(HoodieTestDataGenerator.TRIP_NESTED_EXAMPLE_SCHEMA,
        0xDEED, new String[] {partitionPath}, new HashMap<>());
    List<HoodieRecord> allRecord = new ArrayList<>();

    List<HoodieRecord> hoodieRecords1 = dataGen.generateInsertsNestedExample(newCommitTime1, 30);
    allRecord.addAll(hoodieRecords1);
    JavaRDD<HoodieRecord> insertRecordsRDD1 = jsc.parallelize(hoodieRecords1, 1);
    JavaRDD<WriteStatus> statuses1 = client.insert(insertRecordsRDD1, newCommitTime1);
    client.commit(newCommitTime1, statuses1);
    List<WriteStatus> statusList1 = statuses1.collect();
    // Trigger clustering
    String newCommitTime2 = client.startCommit("commit");
    List<HoodieRecord> hoodieRecords2 = dataGen.generateInsertsNestedExample(newCommitTime2, 30);
    allRecord.addAll(hoodieRecords2);
    JavaRDD<HoodieRecord> insertRecordsRDD2 = jsc.parallelize(hoodieRecords2, 1);
    JavaRDD<WriteStatus> statuses2 = client.insert(insertRecordsRDD2, newCommitTime2);
    client.commit(newCommitTime2, statuses2);
    List<WriteStatus> statusList2 = statuses2.collect();

    Set<String> commitTimeSet = new HashSet<>();
    commitTimeSet.add(newCommitTime1);
    commitTimeSet.add(newCommitTime2);
    checkClusteredFilesAgainstReplaceCommit(partitionPath, allRecord, commitTimeSet);
  }

  /**
   * Finds all parquet files created as part of clustering, verifies they match what is found in the replace commit
   * metadata and that their footers describe the given records. Returns the clustered files, relative to the base path.
   */
  private List<String> checkClusteredFilesAgainstReplaceCommit(String partitionPath, List<HoodieRecord> allRecord,
                                                               Set<String> commitTimeSet) throws IOException {
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieInstant replaceCommitInstant = metaClient.getActiveTimeline()
        .getCompletedReplaceTimeline().firstInstant().get();
    HoodieReplaceCommitMetadata replaceCommitMetadata =
        metaClient.getActiveTimeline().readReplaceCommitMetadata(replaceCommitInstant);

    List<String> filesFromReplaceCommit = new ArrayList<>();
    replaceCommitMetadata.getPartitionToWriteStats()
        .forEach((k, v) -> v.forEach(entry -> filesFromReplaceCommit.add(entry.getPath())));

    FileStatus[] fileStatuses = fs.listStatus(new Path(basePath + "/" + partitionPath));
    List<String> clusteredFiles = new ArrayList<>();
    String clusteredFileName = "";
    for (FileStatus status : fileStatuses) {
      if (status.getPath().getName().contains(replaceCommitInstant.requestedTime())) {
        clusteredFiles.add(partitionPath + "/" + status.getPath().getName());
        clusteredFileName = status.getPath().getName();
      }
    }
    assertEquals(clusteredFiles, filesFromReplaceCommit);
    checkFileFooter(clusteredFiles, allRecord, commitTimeSet, clusteredFileName);
    return clusteredFiles;
  }

  private void checkFileFooter(List<String> clusteredFiles, List<HoodieRecord> allRecord,
                               Set<String> commitTimeSet, String clusteredFileName) {
    String partitionPath = "2015/03/16";
    assertTrue(clusteredFiles.size() == 1);
    List<String> recordKeys = allRecord.stream().map(record -> record.getRecordKey()).collect(Collectors.toList());

    // 1、Check max and min
    String minKey = recordKeys.stream().min(Comparator.naturalOrder()).get();
    String maxKey = recordKeys.stream().max(Comparator.naturalOrder()).get();

    FileMetaData parquetMetadata = ParquetUtils.readMetadata(metaClient.getStorage(),
        new StoragePath(basePath + "/" + clusteredFiles.get(0))).getFileMetaData();

    Map<String, String> keyValueMetaData = parquetMetadata.getKeyValueMetaData();
    assertEquals(keyValueMetaData.get(HoodieBloomFilterWriteSupport.HOODIE_MIN_RECORD_KEY_FOOTER), minKey);
    assertEquals(keyValueMetaData.get(HoodieBloomFilterWriteSupport.HOODIE_MAX_RECORD_KEY_FOOTER), maxKey);

    // 2、Check bloom value
    assertEquals(keyValueMetaData.get(HoodieBloomFilterWriteSupport.HOODIE_BLOOM_FILTER_TYPE_CODE),
        BloomFilterTypeCode.DYNAMIC_V0.name());

    // 3、Make sure Bloom Filter contains all the record keys
    BloomFilter bloomFilter = new ParquetUtils().readBloomFilterFromMetadata(metaClient.getStorage(),
        new StoragePath(basePath + "/" + clusteredFiles.get(0)));
    recordKeys.forEach(recordKey -> {
      assertTrue(bloomFilter.mightContain(recordKey));
    });

    // 4、Check meta info in dataset
    Dataset<Row> rows = sqlContext.read().format("parquet")
        .load(basePath + "/" + clusteredFiles.get(0));
    rows.cache();
    assertTrue(rows.select("_hoodie_commit_time").collectAsList()
        .stream().allMatch(t -> commitTimeSet.contains(t.getAs(0))));
    assertTrue(rows.select("_hoodie_record_key").collectAsList()
        .stream().allMatch(t -> recordKeys.contains(t.getAs(0))));
    assertTrue(rows.select("_hoodie_partition_path").collectAsList()
        .stream().allMatch(t -> t.getAs(0).equals(partitionPath)));
    assertTrue(rows.select("_hoodie_file_name").collectAsList().stream()
        .allMatch(t -> t.getAs(0).equals(clusteredFileName)));
  }

  @Test
  public void testSupportBinaryStreamCopy() throws IOException {
    HoodieWriteConfig writeConfig = new HoodieWriteConfig.Builder()
        .withPath(basePath)
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .withEmbeddedTimelineServerEnabled(false)
        .build();
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    HoodieTable table = HoodieSparkTable.create(writeConfig, engineContext, metaClient);
    SparkBinaryCopyClusteringExecutionStrategy strategy = new SparkBinaryCopyClusteringExecutionStrategy(table, engineContext, writeConfig);

    MessageType legacySchema = new AvroSchemaConverter().convert(AVRO_SCHEMA);
    Configuration conf = new Configuration();
    conf.set("parquet.avro.write-old-list-structure", "false");
    MessageType standardSchema = new AvroSchemaConverter(conf).convert(AVRO_SCHEMA);

    BloomFilter simpleBloomFilter = BloomFilterFactory.createBloomFilter(1000, 0.0001, 10000, SIMPLE.name());
    BloomFilter dynamicBloomFilter = BloomFilterFactory.createBloomFilter(1000, 0.0001, 10000, DYNAMIC_V0.name());
    String file1 = makeTestFile("file-1.parquet", HOODIE_SCHEMA, legacySchema, simpleBloomFilter);
    String file2 = makeTestFile("file-2.parquet", HOODIE_SCHEMA, legacySchema, dynamicBloomFilter);
    String file3 = makeTestFile("file-3.parquet", HOODIE_SCHEMA, standardSchema, dynamicBloomFilter);
    String file4 = makeTestFile("file-4.parquet", HOODIE_SCHEMA, standardSchema, dynamicBloomFilter);

    // input file contains multiple bloom filter code type, should return false
    List<ClusteringGroupInfo> groups = makeClusteringGroup(file1, file2);
    Assertions.assertFalse(strategy.supportBinaryStreamCopy(groups, new HashMap<>()));

    // input file contains legacy schema, should return false
    groups = makeClusteringGroup(file2, file3);
    Assertions.assertFalse(strategy.supportBinaryStreamCopy(groups, new HashMap<>()));

    // otherwise should return true
    groups = makeClusteringGroup(file3, file4);
    Assertions.assertTrue(strategy.supportBinaryStreamCopy(groups, new HashMap<>()));

    metaClient = HoodieTestUtils.init(basePath, HoodieTableType.MERGE_ON_READ);
    table = HoodieSparkTable.create(writeConfig, engineContext, metaClient);
    strategy = new SparkBinaryCopyClusteringExecutionStrategy(table, engineContext, writeConfig);
    Assertions.assertFalse(strategy.supportBinaryStreamCopy(groups, new HashMap<>()));
  }

  @Test
  public void testSupportStreamCopy() throws IOException {
    HoodieWriteConfig writeConfig = new HoodieWriteConfig.Builder()
        .withPath(basePath)
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .withEmbeddedTimelineServerEnabled(false)
        .build();
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    HoodieTable table = HoodieSparkTable.create(writeConfig, engineContext, metaClient);
    SparkStreamCopyClusteringExecutionStrategy strategy =
        new SparkStreamCopyClusteringExecutionStrategy(table, engineContext, writeConfig);

    MessageType legacySchema = new AvroSchemaConverter().convert(AVRO_SCHEMA);
    Configuration conf = new Configuration();
    conf.set("parquet.avro.write-old-list-structure", "false");
    MessageType standardSchema = new AvroSchemaConverter(conf).convert(AVRO_SCHEMA);

    BloomFilter simpleBloomFilter = BloomFilterFactory.createBloomFilter(1000, 0.0001, 10000, SIMPLE.name());
    BloomFilter dynamicBloomFilter = BloomFilterFactory.createBloomFilter(1000, 0.0001, 10000, DYNAMIC_V0.name());
    // differs from the others in both bloom filter type code and list structure
    String file1 = makeTestFile("stream-copy-1.parquet", HOODIE_SCHEMA, legacySchema, simpleBloomFilter);
    String file2 = makeTestFile("stream-copy-2.parquet", HOODIE_SCHEMA, standardSchema, dynamicBloomFilter);
    String file3 = makeTestFile("stream-copy-3.parquet", HOODIE_SCHEMA, standardSchema, dynamicBloomFilter);
    List<ClusteringGroupInfo> incompatibleGroups = makeClusteringGroup(file1, file2);
    List<ClusteringGroupInfo> compatibleGroups = makeClusteringGroup(file2, file3);

    // schema evolution is disabled by default, so the files are never scanned and even a mismatch is accepted
    Assertions.assertTrue(strategy.supportBinaryStreamCopy(incompatibleGroups, new HashMap<>()));

    // sorting is not supported by binary copy
    Map<String, String> sortParams = new HashMap<>();
    sortParams.put(PLAN_STRATEGY_SORT_COLUMNS.key(), "rider");
    Assertions.assertFalse(strategy.supportBinaryStreamCopy(compatibleGroups, sortParams));

    // with schema evolution enabled the files are scanned for real
    HoodieWriteConfig evolutionConfig = new HoodieWriteConfig.Builder()
        .withPath(basePath)
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .withEmbeddedTimelineServerEnabled(false)
        .withClusteringConfig(HoodieClusteringConfig.newBuilder()
            .withFileStitchingBinaryCopySchemaEvolutionEnabled(true)
            .build())
        .build();
    SparkStreamCopyClusteringExecutionStrategy evolutionStrategy =
        new SparkStreamCopyClusteringExecutionStrategy(table, engineContext, evolutionConfig);
    Assertions.assertTrue(evolutionStrategy.supportBinaryStreamCopy(compatibleGroups, new HashMap<>()));
    Assertions.assertFalse(evolutionStrategy.supportBinaryStreamCopy(incompatibleGroups, new HashMap<>()));

    // only parquet base files are supported
    metaClient = HoodieTestUtils.init(basePath, HoodieFileFormat.ORC);
    HoodieTable orcTable = HoodieSparkTable.create(writeConfig, engineContext, metaClient);
    Assertions.assertFalse(new SparkStreamCopyClusteringExecutionStrategy(orcTable, engineContext, writeConfig)
        .supportBinaryStreamCopy(compatibleGroups, new HashMap<>()));

    // only COW tables are supported
    metaClient = HoodieTestUtils.init(basePath, HoodieTableType.MERGE_ON_READ);
    HoodieTable morTable = HoodieSparkTable.create(writeConfig, engineContext, metaClient);
    Assertions.assertFalse(new SparkStreamCopyClusteringExecutionStrategy(morTable, engineContext, writeConfig)
        .supportBinaryStreamCopy(compatibleGroups, new HashMap<>()));
  }

  @Test
  public void testStreamCopyClusteringEndToEnd() throws IOException {
    String partitionPath = "2015/03/16";
    Properties properties = new Properties();
    properties.setProperty("hoodie.parquet.small.file.limit", "-1");
    HoodieWriteConfig.Builder cfgBuilder = new HoodieWriteConfig.Builder()
        .withPath(basePath)
        .withSchema(TRIP_NESTED_EXAMPLE_SCHEMA)
        .withEmbeddedTimelineServerEnabled(false)
        .withClusteringConfig(
            HoodieClusteringConfig
                .newBuilder()
                .withInlineClustering(true)
                .withAsyncClustering(false)
                .withInlineClusteringNumCommits(2)
                // the execution strategy is picked from the write config rather than from the plan, so set both
                .withClusteringPlanStrategyClass(HoodieClusteringConfig.SPARK_STREAM_COPY_CLUSTERING_PLAN_STRATEGY)
                .withClusteringExecutionStrategyClass(HoodieClusteringConfig.SPARK_STREAM_COPY_CLUSTERING_EXECUTION_STRATEGY)
                .withFileStitchingBinaryCopySchemaEvolutionEnabled(false)
                .build()).withProps(properties);
    SparkRDDWriteClient client = getHoodieWriteClient(cfgBuilder.build());
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(HoodieTestDataGenerator.TRIP_NESTED_EXAMPLE_SCHEMA,
        0xDEED, new String[] {partitionPath}, new HashMap<>());

    List<HoodieRecord> allRecord = new ArrayList<>();
    Set<String> commitTimeSet = new HashSet<>();
    // the second commit triggers the inline clustering of both base files
    for (int i = 0; i < 2; i++) {
      String newCommitTime = client.startCommit("commit");
      commitTimeSet.add(newCommitTime);
      List<HoodieRecord> hoodieRecords = dataGen.generateInsertsNestedExample(newCommitTime, 30);
      allRecord.addAll(hoodieRecords);
      JavaRDD<WriteStatus> statuses = client.insert(jsc.parallelize(hoodieRecords, 1), newCommitTime);
      client.commit(newCommitTime, statuses);
    }

    List<String> clusteredFiles = checkClusteredFilesAgainstReplaceCommit(partitionPath, allRecord, commitTimeSet);

    // every input record has to survive the binary stream copy, exactly once
    List<String> clusteredKeys = sqlContext.read().format("parquet")
        .load(basePath + "/" + clusteredFiles.get(0))
        .select("_hoodie_record_key").collectAsList()
        .stream().map(row -> row.getString(0)).collect(Collectors.toList());
    assertEquals(allRecord.size(), clusteredKeys.size());
    assertEquals(allRecord.stream().map(HoodieRecord::getRecordKey).collect(Collectors.toSet()),
        new HashSet<>(clusteredKeys));
  }

  /**
   * Binary-copy clustering must not populate {@code _hoodie_file_name} on a table whose mode opts out.
   *
   * <p>{@code HoodieParquetBinaryCopyBase#initFileWriter} installs a mask rewriting that column to the
   * output file name. Unconditionally applied, it hands a {@code COMMIT_TIME_ONLY} table a file name it
   * never advertised -- the column reads as populated while the table config says it is not. The mask
   * is gated on the table's mode; this pins that gate for the one strategy that reaches it.
   */
  @Test
  public void testStreamCopyClusteringLeavesFileNameNullUnderCommitTimeOnly() throws IOException {
    String partitionPath = "2015/03/16";
    Properties properties = new Properties();
    properties.setProperty("hoodie.parquet.small.file.limit", "-1");
    properties.setProperty(HoodieTableConfig.META_FIELDS_MODE.key(), MetaFieldsMode.COMMIT_TIME_ONLY.name());
    // A selective mode leaves _hoodie_record_key unpopulated, so the write path derives the key
    // through the key generator instead of reading the column -- which means the key-generator
    // properties have to be present. The sibling ALL-mode tests never construct one.
    properties.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
    properties.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "partition_path");
    HoodieWriteConfig.Builder cfgBuilder = new HoodieWriteConfig.Builder()
        .withPath(basePath)
        .withSchema(TRIP_NESTED_EXAMPLE_SCHEMA)
        .withEmbeddedTimelineServerEnabled(false)
        .withMetaFieldsMode(MetaFieldsMode.COMMIT_TIME_ONLY)
        .withClusteringConfig(
            HoodieClusteringConfig
                .newBuilder()
                .withInlineClustering(true)
                .withAsyncClustering(false)
                .withInlineClusteringNumCommits(2)
                // the execution strategy is picked from the write config rather than from the plan, so set both
                .withClusteringPlanStrategyClass(HoodieClusteringConfig.SPARK_STREAM_COPY_CLUSTERING_PLAN_STRATEGY)
                .withClusteringExecutionStrategyClass(HoodieClusteringConfig.SPARK_STREAM_COPY_CLUSTERING_EXECUTION_STRATEGY)
                .withFileStitchingBinaryCopySchemaEvolutionEnabled(false)
                .build()).withProps(properties);

    // The table has to agree with the writer: meta-field population is a table property. Update the
    // existing hoodie.properties rather than re-initialising through newTableBuilder, which would drop
    // the key-generator properties HoodieTestUtils.init put there and fail keygen construction.
    Properties tableProps = new Properties();
    tableProps.putAll(metaClient.getTableConfig().getProps());
    tableProps.put(HoodieTableConfig.META_FIELDS_MODE.key(), MetaFieldsMode.COMMIT_TIME_ONLY.name());
    tableProps.put(HoodieTableConfig.POPULATE_META_FIELDS.key(), "false");
    HoodieTableConfig.update(metaClient.getStorage(), metaClient.getMetaPath(), tableProps);
    metaClient = HoodieTableMetaClient.reload(metaClient);

    SparkRDDWriteClient client = getHoodieWriteClient(cfgBuilder.build());
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(HoodieTestDataGenerator.TRIP_NESTED_EXAMPLE_SCHEMA,
        0xDEED, new String[] {partitionPath}, new HashMap<>());

    for (int i = 0; i < 2; i++) {
      String newCommitTime = client.startCommit("commit");
      List<HoodieRecord> hoodieRecords = dataGen.generateInsertsNestedExample(newCommitTime, 30);
      JavaRDD<WriteStatus> statuses = client.insert(jsc.parallelize(hoodieRecords, 1), newCommitTime);
      client.commit(newCommitTime, statuses);
    }

    HoodieTableMetaClient reloaded = HoodieTableMetaClient.reload(metaClient);
    assertEquals(1, reloaded.getActiveTimeline().getCompletedReplaceTimeline().countInstants(),
        "clustering must have run, otherwise this test proves nothing");

    Dataset<Row> clustered = sqlContext.read().format("parquet")
        .load(basePath + "/" + partitionPath + "/*.parquet");
    long total = clustered.count();
    assertTrue(total > 0, "expected the clustered table to still hold rows");
    assertEquals(0, clustered.filter(functions.col("_hoodie_file_name").isNotNull()).count(),
        "binary-copy clustering must not populate _hoodie_file_name on a COMMIT_TIME_ONLY table");
    assertEquals(total, clustered.filter(functions.col("_hoodie_commit_time").isNotNull()).count(),
        "the opted-in commit time must survive binary-copy clustering on every row");
  }

  /**
   * A copy attempt that fails on its second input leaves a readable file with the rows of the first input, and
   * Spark retries the task with a new file ID. The commit must delete the failed attempt's file through its
   * marker, so a read that lists the partition (no metadata table) and the cleaner see only the committed file.
   */
  @ParameterizedTest
  @EnumSource(MarkerType.class)
  public void testFailedCopyAttemptIsDeletedWhenTaskIsRetried(MarkerType markerType) throws IOException {
    // local[n, maxFailures]: the second number lets Spark retry the failed clustering task once
    cleanupSparkContexts();
    extraConf.put("spark.master", "local[2,2]");
    initSparkContexts();
    SparkRDDWriteClient client = getHoodieWriteClient(failingCopyConfig(markerType));
    List<HoodieRecord> allRecords = new ArrayList<>();
    String clusteringInstant = writeTwoFilesAndScheduleClustering(client, allRecords).getLeft();

    client.cluster(clusteringInstant, true);

    assertEquals(1, FailSecondInputOnceStrategy.FAILED_ATTEMPTS.get(), "the first copy attempt must fail");
    assertOnlyCommittedFileRemains(client, clusteringInstant, allRecords);
  }

  /**
   * The same failure without a task retry fails the clustering, and the failed attempt's file holds exactly the
   * first input's rows. Running the clustering again rolls back its inflight instant first, and the new run
   * commits under the same instant time, so the rollback must delete that file through its marker.
   */
  @ParameterizedTest
  @EnumSource(MarkerType.class)
  public void testFailedCopyAttemptIsRolledBackWhenClusteringIsRetried(MarkerType markerType) throws IOException {
    SparkRDDWriteClient client = getHoodieWriteClient(failingCopyConfig(markerType));
    List<HoodieRecord> allRecords = new ArrayList<>();
    Pair<String, List<String>> scheduled = writeTwoFilesAndScheduleClustering(client, allRecords);
    String clusteringInstant = scheduled.getLeft();
    List<String> inputFiles = scheduled.getRight();

    Exception failure = assertThrows(Exception.class, () -> client.cluster(clusteringInstant, true));
    assertTrue(hasCause(failure, "Failed to retrieve prefetched content for ", new StoragePath(inputFiles.get(1)).getName()),
        "the copy must fail on the second input");
    List<String> failedAttemptFiles = baseFilesInPartition(clusteringInstant);
    assertEquals(1, failedAttemptFiles.size());
    assertEquals(recordKeys(inputFiles.get(0)), recordKeys(basePath + "/" + failedAttemptFiles.get(0)));

    client.cluster(clusteringInstant, true);

    assertEquals(1, FailSecondInputOnceStrategy.FAILED_ATTEMPTS.get());
    assertOnlyCommittedFileRemains(client, clusteringInstant, allRecords);
  }

  private HoodieWriteConfig failingCopyConfig(MarkerType markerType) {
    Properties properties = new Properties();
    properties.setProperty("hoodie.parquet.small.file.limit", "-1");
    return new HoodieWriteConfig.Builder()
        .withPath(basePath)
        .withSchema(TRIP_NESTED_EXAMPLE_SCHEMA)
        // without the metadata table, the file system view and the cleaner list the partition
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .withMarkersType(markerType.name())
        // timeline-server-based markers fall back to direct markers without the embedded timeline server
        .withEmbeddedTimelineServerEnabled(markerType == MarkerType.TIMELINE_SERVER_BASED)
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withAutoClean(false)
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS)
            .retainFileVersions(1)
            .build())
        .withClusteringConfig(HoodieClusteringConfig.newBuilder()
            .withClusteringExecutionStrategyClass(FailSecondInputOnceStrategy.class.getName())
            .build())
        .withProps(properties)
        .build();
  }

  /**
   * Writes two base files to one partition, schedules their clustering into one file, and makes the copy fail on
   * the second one. Returns the clustering instant and the input files in the order the copy reads them.
   */
  private Pair<String, List<String>> writeTwoFilesAndScheduleClustering(SparkRDDWriteClient client, List<HoodieRecord> allRecords) {
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(TRIP_NESTED_EXAMPLE_SCHEMA,
        0xDEED, new String[] {PARTITION_PATH}, new HashMap<>());
    for (int i = 0; i < 2; i++) {
      String commitTime = client.startCommit("commit");
      List<HoodieRecord> records = dataGen.generateInsertsNestedExample(commitTime, 30);
      allRecords.addAll(records);
      client.commit(commitTime, client.insert(jsc.parallelize(records, 1), commitTime));
    }
    String clusteringInstant = (String) client.scheduleClustering(Option.empty()).get();
    HoodieClusteringPlan plan = ClusteringUtils.getPendingClusteringPlan(HoodieTableMetaClient.reload(metaClient), clusteringInstant);
    assertEquals(1, plan.getInputGroups().size());
    List<String> inputFiles = plan.getInputGroups().get(0).getSlices().stream()
        .map(HoodieSliceInfo::getDataFilePath).collect(Collectors.toList());
    assertEquals(2, inputFiles.size());
    FailSecondInputOnceStrategy.failOnce(inputFiles.get(1));
    return Pair.of(clusteringInstant, inputFiles);
  }

  private void assertOnlyCommittedFileRemains(SparkRDDWriteClient client, String clusteringInstant,
                                              List<HoodieRecord> allRecords) throws IOException {
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieInstant replaceCommit = metaClient.getActiveTimeline().getCompletedReplaceTimeline().lastInstant().get();
    assertEquals(clusteringInstant, replaceCommit.requestedTime());
    List<String> committedFiles = new ArrayList<>();
    metaClient.getActiveTimeline().readReplaceCommitMetadata(replaceCommit).getPartitionToWriteStats()
        .forEach((partition, stats) -> stats.forEach(stat -> committedFiles.add(stat.getPath())));
    assertEquals(1, committedFiles.size());
    assertEquals(committedFiles, baseFilesInPartition(clusteringInstant), "the failed attempt's file must be deleted");

    List<String> keys = sqlContext.read().format("hudi")
        .option(HoodieMetadataConfig.ENABLE.key(), "false")
        .load(basePath)
        .select(HoodieRecord.RECORD_KEY_METADATA_FIELD).collectAsList()
        .stream().map(row -> row.getString(0)).sorted().collect(Collectors.toList());
    assertEquals(allRecords.stream().map(HoodieRecord::getRecordKey).sorted().collect(Collectors.toList()), keys,
        "a read that lists the partition must return each record once");

    // the cleaner deletes the two replaced inputs and leaves the committed file alone in the partition
    client.clean();
    assertEquals(committedFiles, baseFilesInPartition(null));
  }

  /**
   * Returns the base files in {@link #PARTITION_PATH}, relative to the base path, optionally only those written
   * at the given instant.
   */
  private List<String> baseFilesInPartition(String instantTime) throws IOException {
    return Arrays.stream(fs.listStatus(new Path(basePath, PARTITION_PATH)))
        .map(status -> status.getPath().getName())
        .filter(name -> name.endsWith(HoodieFileFormat.PARQUET.getFileExtension()))
        .filter(name -> instantTime == null || FSUtils.getCommitTime(name).equals(instantTime))
        .map(name -> PARTITION_PATH + "/" + name)
        .sorted()
        .collect(Collectors.toList());
  }

  private List<String> recordKeys(String parquetFile) {
    return sqlContext.read().format("parquet").load(parquetFile)
        .select(HoodieRecord.RECORD_KEY_METADATA_FIELD).collectAsList()
        .stream().map(row -> row.getString(0)).sorted().collect(Collectors.toList());
  }

  private static boolean hasCause(Throwable failure, String messagePrefix, String messagePart) {
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      String message = cause.getMessage();
      if (message != null && message.startsWith(messagePrefix) && message.contains(messagePart)) {
        return true;
      }
    }
    return false;
  }

  private String makeTestFile(String fileName, HoodieSchema schema, MessageType messageType, BloomFilter filter) throws IOException {
    HoodieAvroWriteSupport writeSupport = new HoodieAvroWriteSupport(messageType, schema, Option.of(filter), new Properties());
    StoragePath filePath = new StoragePath(tempDir.resolve(fileName).toAbsolutePath().toString());
    HoodieConfig hoodieConfig = new HoodieConfig();
    hoodieConfig.setValue("hoodie.base.path", basePath);
    HoodieParquetConfig<HoodieAvroWriteSupport> parquetConfig =
        new HoodieParquetConfig(
            writeSupport,
            CompressionCodecName.GZIP,
            ParquetWriter.DEFAULT_BLOCK_SIZE,
            ParquetWriter.DEFAULT_PAGE_SIZE,
            1024 * 1024 * 1024,
            storageConf,
            0.1,
            true);

    HoodieAvroParquetWriter writer =
        new HoodieAvroParquetWriter(filePath, parquetConfig, "001", new LocalTaskContextSupplier(), true);
    writer.close();
    return filePath.toString();
  }

  private List<ClusteringGroupInfo> makeClusteringGroup(String... files) {
    List<ClusteringOperation> operations = Arrays.stream(files)
        .map(file -> {
          ClusteringOperation op = new ClusteringOperation();
          op.setDataFilePath(file);
          return op;
        })
        .collect(Collectors.toList());
    ClusteringGroupInfo group = new ClusteringGroupInfo();
    group.setNumOutputGroups(1);
    group.setOperations(operations);
    return Collections.singletonList(group);
  }

  /**
   * Binary-copy clustering whose first task attempt fails on the input given to {@link #failOnce}. The strategy
   * reads its write config in the clustering task, right before it creates the copy handle. At that point this
   * hides the input, so the copy fails on it after copying the inputs before it, and restores it when the
   * attempt fails.
   */
  public static class FailSecondInputOnceStrategy<T> extends SparkBinaryCopyClusteringExecutionStrategy<T> {
    private static final AtomicReference<String> INPUT_TO_FAIL = new AtomicReference<>();
    private static final AtomicInteger FAILED_ATTEMPTS = new AtomicInteger();

    public FailSecondInputOnceStrategy(HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig) {
      super(table, engineContext, writeConfig);
    }

    static void failOnce(String inputFile) {
      FAILED_ATTEMPTS.set(0);
      INPUT_TO_FAIL.set(inputFile);
    }

    @Override
    protected HoodieWriteConfig getWriteConfig() {
      TaskContext taskContext = TaskContext.get();
      String inputFile = taskContext == null ? null : INPUT_TO_FAIL.getAndSet(null);
      if (inputFile != null) {
        HoodieStorage storage = getHoodieTable().getStorage();
        StoragePath input = new StoragePath(inputFile);
        StoragePath hidden = new StoragePath(input.getParent(), input.getName() + ".hidden");
        rename(storage, input, hidden);
        taskContext.addTaskFailureListener((TaskFailureListener) (context, error) -> {
          rename(storage, hidden, input);
          FAILED_ATTEMPTS.incrementAndGet();
        });
      }
      return super.getWriteConfig();
    }

    private static void rename(HoodieStorage storage, StoragePath from, StoragePath to) {
      try {
        assertTrue(storage.rename(from, to), "failed to rename " + from + " to " + to);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }
}