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
import org.apache.hudi.avro.HoodieBloomFilterWriteSupport;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.clustering.run.strategy.SparkBinaryCopyClusteringExecutionStrategy;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieParquetConfig;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.model.ClusteringGroupInfo;
import org.apache.hudi.common.model.ClusteringOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.storage.hadoop.HoodieAvroParquetWriter;
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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
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
import java.util.stream.Collectors;

import static org.apache.hudi.common.bloom.BloomFilterTypeCode.DYNAMIC_V0;
import static org.apache.hudi.common.bloom.BloomFilterTypeCode.SIMPLE;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.AVRO_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.HOODIE_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_NESTED_EXAMPLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("functional")
public class TestSparkBinaryCopyClusteringAndValidationMeta extends HoodieClientTestBase {

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

    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieInstant replaceCommitInstant = metaClient.getActiveTimeline()
        .getCompletedReplaceTimeline().firstInstant().get();
    HoodieReplaceCommitMetadata replaceCommitMetadata =
        metaClient.getActiveTimeline().readReplaceCommitMetadata(replaceCommitInstant);

    List<String> filesFromReplaceCommit = new ArrayList<>();
    replaceCommitMetadata.getPartitionToWriteStats()
        .forEach((k, v) -> v.forEach(entry -> filesFromReplaceCommit.add(entry.getPath())));

    // find all parquet files created as part of clustering. Verify it matces w/ whats found in replace commit metadata.
    FileStatus[] fileStatuses = fs.listStatus(new Path(basePath + "/" + partitionPath));
    List<String> replacedFiles = new ArrayList<>();
    List<String> clusteredFiles = new ArrayList<>();
    String clusteredFileName = "";
    for (FileStatus status : fileStatuses) {
      if (status.getPath().getName().contains(replaceCommitInstant.requestedTime())) {
        clusteredFiles.add(partitionPath + "/" + status.getPath().getName());
        clusteredFileName = status.getPath().getName();
      } else if (!status.getPath().getName().startsWith(".")) {
        replacedFiles.add(partitionPath + "/" + status.getPath().getName());
      }
    }
    assertEquals(clusteredFiles, filesFromReplaceCommit);
    // clusteredFiles check
    Set<String> commitTimeSet = new HashSet();
    commitTimeSet.add(newCommitTime1);
    commitTimeSet.add(newCommitTime2);
    checkFileFooter(clusteredFiles, allRecord, commitTimeSet, clusteredFileName);
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
    BloomFilter dynmicBloomFilter = BloomFilterFactory.createBloomFilter(1000, 0.0001, 10000, DYNAMIC_V0.name());
    String file1 = makeTestFile("file-1.parquet", HOODIE_SCHEMA, legacySchema, simpleBloomFilter);
    String file2 = makeTestFile("file-2.parquet", HOODIE_SCHEMA, legacySchema, dynmicBloomFilter);
    String file3 = makeTestFile("file-3.parquet", HOODIE_SCHEMA, standardSchema, dynmicBloomFilter);
    String file4 = makeTestFile("file-4.parquet", HOODIE_SCHEMA, standardSchema, dynmicBloomFilter);

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

  private String makeTestFile(String fileName, HoodieSchema hoodieSchema, MessageType messageType, BloomFilter filter) throws IOException {
    HoodieAvroWriteSupport writeSupport = new HoodieAvroWriteSupport(messageType, hoodieSchema.getAvroSchema(), Option.of(filter), new Properties());
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
}