/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utils;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.model.CommitTimeFlinkRecordMerger;
import org.apache.hudi.client.model.EventTimeFlinkRecordMerger;
import org.apache.hudi.client.model.PartialUpdateFlinkRecordMerger;
import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.EventTimeAvroPayload;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.model.PartialUpdateAvroPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Triple;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodiePayloadConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.io.util.FileIOUtils;
import org.apache.hudi.keygen.ComplexAvroKeyGenerator;
import org.apache.hudi.keygen.SimpleAvroKeyGenerator;
import org.apache.hudi.sink.FlinkCheckpointClient;
import org.apache.hudi.sink.muttley.AthenaIngestionGateway;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.streamer.FlinkStreamerConfig;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link StreamerUtil}.
 */
class TestStreamerUtil {

  @TempDir
  File tempFile;

  @Test
  void testMetadataConfigIncludesMetadataTableBloomFilterSettings() {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.METADATA_ENABLED, true);
    conf.setString(HoodieMetadataConfig.BLOOM_FILTER_ENABLE.key(), "true");
    conf.setString(HoodieMetadataConfig.BLOOM_FILTER_TYPE.key(), BloomFilterTypeCode.SIMPLE.name());
    conf.setString(HoodieMetadataConfig.BLOOM_FILTER_NUM_ENTRIES.key(), "12345");
    conf.setString(HoodieMetadataConfig.BLOOM_FILTER_FPP.key(), "0.005");
    conf.setString(HoodieMetadataConfig.BLOOM_FILTER_DYNAMIC_MAX_ENTRIES.key(), "23456");
    conf.setString(FileSystemViewStorageConfig.REMOTE_HOST_NAME.key(), "localhost");

    HoodieMetadataConfig metadataConfig = StreamerUtil.metadataConfig(conf);

    assertTrue(metadataConfig.isEnabled());
    assertTrue(metadataConfig.enableBloomFilter());
    assertEquals(BloomFilterTypeCode.SIMPLE.name(), metadataConfig.getBloomFilterType());
    assertEquals(12345, metadataConfig.getBloomFilterNumEntries());
    assertEquals(0.005, metadataConfig.getBloomFilterFpp());
    assertEquals(23456, metadataConfig.getDynamicBloomFilterMaxNumEntries());
  }

  @Test
  void testInferMergingBehavior() {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    // default merge behavior
    Triple<RecordMergeMode, String, String> mergeBehavior = StreamerUtil.inferMergingBehavior(conf);
    assertEquals(RecordMergeMode.EVENT_TIME_ORDERING, mergeBehavior.getLeft());
    assertEquals(EventTimeAvroPayload.class.getName(), mergeBehavior.getMiddle());
    assertNull(mergeBehavior.getRight());

    // set commit time merge mode
    conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.RECORD_MERGE_MODE, RecordMergeMode.COMMIT_TIME_ORDERING.name());
    mergeBehavior = StreamerUtil.inferMergingBehavior(conf);
    assertEquals(RecordMergeMode.COMMIT_TIME_ORDERING, mergeBehavior.getLeft());
    assertEquals(OverwriteWithLatestAvroPayload.class.getName(), mergeBehavior.getMiddle());
    assertNull(mergeBehavior.getRight());

    // set partial update merger.
    conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.RECORD_MERGER_IMPLS, PartialUpdateFlinkRecordMerger.class.getName());
    mergeBehavior = StreamerUtil.inferMergingBehavior(conf);
    assertEquals(RecordMergeMode.EVENT_TIME_ORDERING, mergeBehavior.getLeft());
    assertEquals(PartialUpdateAvroPayload.class.getName(), mergeBehavior.getMiddle());
    assertNull(mergeBehavior.getRight());

    // set partial update payload
    conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.PAYLOAD_CLASS_NAME, PartialUpdateAvroPayload.class.getName());
    mergeBehavior = StreamerUtil.inferMergingBehavior(conf);
    assertEquals(RecordMergeMode.EVENT_TIME_ORDERING, mergeBehavior.getLeft());
    assertEquals(PartialUpdateAvroPayload.class.getName(), mergeBehavior.getMiddle());
    assertNull(mergeBehavior.getRight());

    // set partial update payload
    conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.PAYLOAD_CLASS_NAME, PartialUpdateAvroPayload.class.getName());
    conf.set(FlinkOptions.WRITE_TABLE_VERSION, HoodieTableVersion.EIGHT.versionCode());
    mergeBehavior = StreamerUtil.inferMergingBehavior(conf);
    assertEquals(RecordMergeMode.EVENT_TIME_ORDERING, mergeBehavior.getLeft());
    assertEquals(PartialUpdateAvroPayload.class.getName(), mergeBehavior.getMiddle());
    assertNull(mergeBehavior.getRight());
  }

  @Test
  void testGetIndexConfigUsesFlinkBucketRemotePartitionerConfig() {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.BUCKET.name());
    conf.set(FlinkOptions.BUCKET_INDEX_ENGINE_TYPE, HoodieIndex.BucketIndexEngineType.SIMPLE.name());

    assertFalse(StreamerUtil.getIndexConfig(conf).getBoolean(HoodieIndexConfig.BUCKET_PARTITIONER));

    conf.setString(HoodieIndexConfig.BUCKET_PARTITIONER.key(), "true");
    assertTrue(StreamerUtil.getIndexConfig(conf).getBoolean(HoodieIndexConfig.BUCKET_PARTITIONER));
  }

  @Test
  void testInitTableWithSpecificVersion() throws IOException {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());

    // Test for partitioned table.
    conf.set(FlinkOptions.PARTITION_PATH_FIELD, "p0,p1");
    conf.set(FlinkOptions.WRITE_TABLE_VERSION, HoodieTableVersion.SIX.versionCode());
    conf.setString(HoodieTableConfig.TABLE_STORAGE_LAYOUT.key(),
        HoodieTableConfig.TableStorageLayout.DEFAULT.configValue());
    StreamerUtil.initTableIfNotExists(conf);

    // Validate the partition fields & preCombineField in hoodie.properties.
    HoodieTableMetaClient metaClient1 = HoodieTestUtils.createMetaClient(tempFile.getAbsolutePath());
    assertArrayEquals(metaClient1.getTableConfig().getPartitionFields().get(), new String[] {"p0", "p1"});
    assertNotNull(metaClient1.getTableConfig().getKeyGeneratorClassName());
    assertEquals(HoodieTableVersion.SIX, metaClient1.getTableConfig().getTableVersion());
    assertEquals(HoodieTableConfig.TableStorageLayout.DEFAULT.configValue(),
        conf.getString(HoodieTableConfig.TABLE_STORAGE_LAYOUT.key(), null));
  }

  @Test
  void testInitLsmTreeTable() throws IOException {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.WRITE_TABLE_VERSION, HoodieTableVersion.TEN.versionCode());
    conf.setString(HoodieTableConfig.TABLE_STORAGE_LAYOUT.key(),
        HoodieTableConfig.TableStorageLayout.LSM_TREE.configValue());

    HoodieTableMetaClient metaClient = StreamerUtil.initTableIfNotExists(conf);

    assertEquals(HoodieTableVersion.TEN, metaClient.getTableConfig().getTableVersion());
    assertTrue(metaClient.getTableConfig().isLSMTreeStorageLayout());
    assertFalse(metaClient.getTableConfig().contains(HoodieTableConfig.LOG_FILE_FORMAT));
  }

  @Test
  void testInitInsertTableStorageLayout() throws IOException {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.OPERATION, "insert");

    HoodieTableMetaClient metaClient = StreamerUtil.initTableIfNotExists(conf);

    assertFalse(metaClient.getTableConfig().isLSMTreeStorageLayout());

    FileIOUtils.deleteDirectory(tempFile);
    conf.set(FlinkOptions.OPERATION, WriteOperationType.BULK_INSERT.value());
    metaClient = StreamerUtil.initTableIfNotExists(conf);

    assertTrue(metaClient.getTableConfig().isLSMTreeStorageLayout());
  }

  @Test
  void testInitTableIfNotExists() throws IOException {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());

    // Test for partitioned table.
    conf.set(FlinkOptions.ORDERING_FIELDS, "ts");
    conf.set(FlinkOptions.PARTITION_PATH_FIELD, "p0,p1");
    StreamerUtil.initTableIfNotExists(conf);

    // Validate the partition fields & preCombineField in hoodie.properties.
    HoodieTableMetaClient metaClient1 = HoodieTestUtils.createMetaClient(tempFile.getAbsolutePath());
    assertTrue(metaClient1.getTableConfig().getPartitionFields().isPresent(),
        "Missing partition columns in the hoodie.properties.");
    assertArrayEquals(metaClient1.getTableConfig().getPartitionFields().get(), new String[] {"p0", "p1"});
    assertEquals(metaClient1.getTableConfig().getOrderingFieldsStr().get(), "ts");
    assertEquals(metaClient1.getTableConfig().getKeyGeneratorClassName(), SimpleAvroKeyGenerator.class.getName());
    assertEquals(HoodieTableVersion.current(), metaClient1.getTableConfig().getTableVersion());
    assertTrue(metaClient1.getTableConfig().isLSMTreeStorageLayout());

    // Test for non-partitioned table.
    conf.removeConfig(FlinkOptions.PARTITION_PATH_FIELD);
    FileIOUtils.deleteDirectory(tempFile);
    StreamerUtil.initTableIfNotExists(conf);
    HoodieTableMetaClient metaClient2 = HoodieTestUtils.createMetaClient(tempFile.getAbsolutePath());
    assertFalse(metaClient2.getTableConfig().getPartitionFields().isPresent());
    assertEquals(metaClient2.getTableConfig().getKeyGeneratorClassName(), SimpleAvroKeyGenerator.class.getName());
  }

  @Test
  void testMedianInstantTime() {
    String higher = "20210705125921";
    String lower = "20210705125806";
    String expectedMedianInstant = "20210705125844499";
    String median1 = StreamerUtil.medianInstantTime(higher, lower).get();
    assertThat(median1, is(expectedMedianInstant));
    // test symmetry
    assertThrows(IllegalArgumentException.class,
        () -> StreamerUtil.medianInstantTime(lower, higher),
        "The first argument should have newer instant time");
    // test very near instant time
    assertFalse(StreamerUtil.medianInstantTime("20211116115634", "20211116115633").isPresent());
  }

  @Test
  void testInstantTimeDiff() {
    String higher = "20210705125921";
    String lower = "20210705125806";
    long diff = StreamerUtil.instantTimeDiffSeconds(higher, lower);
    assertThat(diff, is(75L));
  }

  @Test
  public void testAddCheckpointIdIntoMetadata() {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());

    // Test for write extra metadata.
    conf.set(FlinkOptions.WRITE_EXTRA_METADATA_ENABLED, true);

    HashMap<String, String> metadata = new HashMap<>();
    StreamerUtil.addFlinkCheckpointIdIntoMetaData(conf, metadata, 123L);
    assertEquals(metadata.get(StreamerUtil.FLINK_CHECKPOINT_ID), "123");
  }

  @Test
  void testTableExist() throws IOException {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    String basePath = tempFile.getAbsolutePath();

    assertFalse(StreamerUtil.tableExists(basePath, HadoopConfigurations.getHadoopConf(conf)));

    try (FileSystem fs = HadoopFSUtils.getFs(basePath, HadoopConfigurations.getHadoopConf(conf))) {
      fs.mkdirs(new Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME));
      assertFalse(StreamerUtil.tableExists(basePath, HadoopConfigurations.getHadoopConf(conf)));

      fs.create(new Path(new Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME), HoodieTableConfig.HOODIE_PROPERTIES_FILE));
      assertTrue(StreamerUtil.tableExists(basePath, HadoopConfigurations.getHadoopConf(conf)));
    }
  }

  @Test
  void testBuildProperties() {
    TypedProperties properties = StreamerUtil.buildProperties(
        Arrays.asList("hoodie.test.one=1", "hoodie.test.two=two"));

    assertEquals("1", properties.getString("hoodie.test.one"));
    assertEquals("two", properties.getString("hoodie.test.two"));
    assertThrows(IllegalArgumentException.class,
        () -> StreamerUtil.buildProperties(Collections.singletonList("invalid")));
  }

  @Test
  void testSourceSchemaConfiguration() {
    Configuration conf = new Configuration();
    String schema = "{\"type\":\"record\",\"name\":\"record\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}";
    conf.set(FlinkOptions.SOURCE_AVRO_SCHEMA, schema);

    HoodieSchema sourceSchema = StreamerUtil.getSourceSchema(conf);

    assertTrue(sourceSchema.getField("id").isPresent());
    assertThrows(HoodieException.class,
        () -> StreamerUtil.getSourceSchema(new Configuration()));
  }

  @Test
  void testConfigurationConversions() {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.COMPACTION_MAX_MEMORY, 256);
    conf.set(FlinkOptions.ORDERING_FIELDS, "ts");
    conf.set(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.BLOOM.name());

    TypedProperties properties = StreamerUtil.flinkConf2TypedProperties(conf);

    assertEquals(conf.get(FlinkOptions.TABLE_TYPE),
        properties.getString(HoodieTableConfig.TYPE.key()));
    assertEquals(256L * 1024 * 1024, StreamerUtil.getMaxCompactionMemoryInBytes(conf));
    assertEquals("ts", StreamerUtil.getPayloadConfig(conf).getString(HoodiePayloadConfig.ORDERING_FIELDS));
    assertEquals(HoodieIndex.IndexType.BLOOM.name(),
        StreamerUtil.getIndexConfig(conf).getString(HoodieIndexConfig.INDEX_TYPE));
  }

  @Test
  void testSimplePathAndFileUtilities() {
    assertEquals("partition_file-id", StreamerUtil.generateBucketKey("partition", "file-id"));

    assertFalse(StreamerUtil.isValidFile(pathInfo("file.parquet", 4)));
    assertTrue(StreamerUtil.isValidFile(pathInfo("file.parquet", 5)));
    assertFalse(StreamerUtil.isValidFile(pathInfo("file.log", 6)));
    assertTrue(StreamerUtil.isValidFile(pathInfo("file.log", 7)));
    assertFalse(StreamerUtil.isValidFile(pathInfo("file.orc", 3)));
    assertTrue(StreamerUtil.isValidFile(pathInfo("file.orc", 4)));
    assertFalse(StreamerUtil.isValidFile(pathInfo("file.unknown", 0)));
    assertTrue(StreamerUtil.isValidFile(pathInfo("file.unknown", 1)));
  }

  @Test
  void testPartitionExists() {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    org.apache.hadoop.conf.Configuration hadoopConf = HadoopConfigurations.getHadoopConf(conf);
    assertFalse(StreamerUtil.partitionExists(tempFile.getAbsolutePath(), "dt=2026-08-06", hadoopConf));

    assertTrue(new File(tempFile, "dt=2026-08-06").mkdir());
    assertTrue(StreamerUtil.partitionExists(tempFile.getAbsolutePath(), "dt=2026-08-06", hadoopConf));
  }

  @Test
  void testParsePartitionDate() {
    DateTimeFormatter dayFormatter = DateTimeFormatter.ofPattern(FlinkOptions.PARTITION_FORMAT_DAY);
    assertEquals(LocalDate.of(2026, 8, 6), StreamerUtil.parsePartitionDate("20260806", dayFormatter, false));

    DateTimeFormatter dashedDayFormatter = DateTimeFormatter.ofPattern(FlinkOptions.PARTITION_FORMAT_DASHED_DAY);
    assertEquals(LocalDate.of(2026, 8, 6), StreamerUtil.parsePartitionDate("2026-08-06", dashedDayFormatter, false));

    assertEquals(LocalDate.of(2026, 8, 6), StreamerUtil.parsePartitionDate("dt=20260806", dayFormatter, true));

    // hiveStylePartitioning=false must not strip the "dt=" prefix, so parsing fails.
    assertNull(StreamerUtil.parsePartitionDate("dt=20260806", dayFormatter, false));

    // no '=' present, hive-style parsing falls back to the raw path.
    assertEquals(LocalDate.of(2026, 8, 6), StreamerUtil.parsePartitionDate("20260806", dayFormatter, true));

    assertNull(StreamerUtil.parsePartitionDate("not-a-date", dayFormatter, false));
    assertNull(StreamerUtil.parsePartitionDate("2026-08-06", dayFormatter, false));
  }

  @Test
  void testOrderingFieldAndKeyGeneratorValidation() {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.ORDERING_FIELDS, "missing");
    assertThrows(HoodieValidationException.class,
        () -> StreamerUtil.checkOrderingFields(conf, Arrays.asList("id", "ts")));

    conf.set(FlinkOptions.ORDERING_FIELDS, "ts");
    StreamerUtil.checkOrderingFields(conf, Arrays.asList("id", "ts"));
    assertEquals("ts", conf.get(FlinkOptions.ORDERING_FIELDS));

    Configuration customPayloadConf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    customPayloadConf.set(FlinkOptions.PAYLOAD_CLASS_NAME, OverwriteWithLatestAvroPayload.class.getName());
    customPayloadConf.removeConfig(FlinkOptions.ORDERING_FIELDS);
    StreamerUtil.checkOrderingFields(customPayloadConf, Collections.singletonList("id"));
    assertEquals(FlinkOptions.NO_PRE_COMBINE, customPayloadConf.get(FlinkOptions.ORDERING_FIELDS));

    Configuration keygenConf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    StreamerUtil.checkKeygenGenerator(true, keygenConf);
    assertEquals(ComplexAvroKeyGenerator.class.getName(),
        keygenConf.get(FlinkOptions.KEYGEN_CLASS_NAME));
  }

  @Test
  void testKafkaOffsetStringFormatting() {
    Map<Integer, Long> offsets = new LinkedHashMap<>();
    offsets.put(2, 200L);
    offsets.put(0, 100L);

    assertEquals(
        "kafka_metadata%3Atopic%3A0:100;kafka_metadata%3Atopic%3A2:200;"
            + "kafka_metadata%3Akafka_cluster%3Atopic%3A:cluster",
        StreamerUtil.stringFy("topic", "cluster", offsets));
    assertEquals("kafka_metadata%3Akafka_cluster%3Atopic%3A:cluster",
        StreamerUtil.stringFy("topic", "cluster", null));
    assertEquals("", StreamerUtil.stringFy(null, "cluster", offsets));
  }

  @Test
  void testOptionalTransformerCreation() throws IOException {
    assertFalse(StreamerUtil.createTransformer(null).isPresent());
    assertThrows(IOException.class,
        () -> StreamerUtil.createTransformer(Collections.singletonList("not.a.Transformer")));
  }

  @Test
  void testCheckpointMetadataDisabled() {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.WRITE_EXTRA_METADATA_ENABLED, false);
    HashMap<String, String> metadata = new HashMap<>();

    StreamerUtil.addFlinkCheckpointIdIntoMetaData(conf, metadata, 123L);
    StreamerUtil.addKafkaOffsetMetaData(conf, metadata, 123L);

    assertTrue(metadata.isEmpty());
  }

  @Test
  void testStreamerPropertiesAndMergeConfiguration() {
    FlinkStreamerConfig streamerConfig = new FlinkStreamerConfig();
    streamerConfig.configs = Arrays.asList("hoodie.test.key=value", "hoodie.test.number=2");
    streamerConfig.kafkaBootstrapServers = "broker:9092";
    streamerConfig.kafkaGroupId = "group";

    TypedProperties properties = StreamerUtil.appendKafkaProps(streamerConfig);
    assertEquals("value", properties.getString("hoodie.test.key"));
    assertEquals("broker:9092", properties.getString("bootstrap.servers"));
    assertEquals("group", properties.getString("group.id"));

    streamerConfig.configs = Collections.singletonList("invalid");
    assertThrows(IllegalArgumentException.class, () -> StreamerUtil.getProps(streamerConfig));

    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    assertNull(StreamerUtil.getMergeMode(conf));
    conf.set(FlinkOptions.RECORD_MERGE_MODE, RecordMergeMode.EVENT_TIME_ORDERING.name());
    assertEquals(RecordMergeMode.EVENT_TIME_ORDERING, StreamerUtil.getMergeMode(conf));
    assertEquals(EventTimeFlinkRecordMerger.class.getName(),
        StreamerUtil.getMergerClasses(conf, RecordMergeMode.EVENT_TIME_ORDERING, EventTimeAvroPayload.class.getName()));
    assertEquals(PartialUpdateFlinkRecordMerger.class.getName(),
        StreamerUtil.getMergerClasses(conf, RecordMergeMode.EVENT_TIME_ORDERING, PartialUpdateAvroPayload.class.getName()));
    assertEquals(CommitTimeFlinkRecordMerger.class.getName(),
        StreamerUtil.getMergerClasses(conf, RecordMergeMode.COMMIT_TIME_ORDERING, OverwriteWithLatestAvroPayload.class.getName()));
    conf.set(FlinkOptions.RECORD_MERGER_IMPLS, "custom.merger");
    assertEquals("custom.merger",
        StreamerUtil.getMergerClasses(conf, RecordMergeMode.CUSTOM, OverwriteWithLatestAvroPayload.class.getName()));

    assertTrue(StreamerUtil.getLockConfig(conf).isPresent());
    assertEquals(tempFile.getAbsolutePath(), StreamerUtil.getTimeGeneratorConfig(conf).getBasePath());
  }

  @Test
  void testLanceAndMetaClientUtilities() throws Exception {
    org.apache.hadoop.conf.Configuration lanceConf = new org.apache.hadoop.conf.Configuration();
    lanceConf.set(HoodieStorageConfig.LANCE_READ_ALLOCATOR_SIZE_BYTES.key(), "1024");
    lanceConf.set(HoodieStorageConfig.LANCE_READ_METADATA_ALLOCATOR_SIZE_BYTES.key(), "256");
    assertEquals("1024", StreamerUtil.getLanceReadConfig(lanceConf)
        .getString(HoodieStorageConfig.LANCE_READ_ALLOCATOR_SIZE_BYTES));
    assertEquals("256", StreamerUtil.getLanceReadConfig(lanceConf)
        .getString(HoodieStorageConfig.LANCE_READ_METADATA_ALLOCATOR_SIZE_BYTES));

    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    org.apache.hadoop.conf.Configuration hadoopConf = HadoopConfigurations.getHadoopConf(conf);
    assertFalse(StreamerUtil.getTableConfig(tempFile.getAbsolutePath(), hadoopConf).isPresent());
    assertNull(StreamerUtil.getLatestTableSchema("", hadoopConf));

    Configuration streamingConf = new Configuration(conf);
    streamingConf.set(FlinkOptions.PATH, new File(tempFile, "missing").getAbsolutePath());
    streamingConf.set(FlinkOptions.READ_AS_STREAMING, true);
    assertNull(StreamerUtil.metaClientForReader(streamingConf, hadoopConf));

    StreamerUtil.initTableIfNotExists(conf, hadoopConf);
    assertTrue(StreamerUtil.getTableConfig(tempFile.getAbsolutePath(), hadoopConf).isPresent());
    assertNotNull(StreamerUtil.createMetaClient(tempFile.getAbsolutePath(), hadoopConf));
    assertNotNull(StreamerUtil.createMetaClient(conf));
    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(conf, hadoopConf);
    assertNotNull(metaClient);
    assertNotNull(StreamerUtil.metaClientForReader(conf, hadoopConf));
    assertNull(StreamerUtil.getLatestTableSchema(tempFile.getAbsolutePath(), hadoopConf));
    assertNull(StreamerUtil.getLastPendingInstant(metaClient));
    assertNull(StreamerUtil.getLastPendingInstant(metaClient, false));
    assertNull(StreamerUtil.getLastCompletedInstant(metaClient));
    assertFalse(StreamerUtil.haveSuccessfulCommits(metaClient));
    assertFalse(StreamerUtil.getPreviousCommitMetadata(metaClient).isPresent());
  }

  @Test
  void testKafkaCheckpointCollectionHappyPath() throws Exception {
    Configuration conf = kafkaCheckpointConf();
    Map<Integer, Long> offsetMap = new HashMap<>();
    offsetMap.put(1, 200L);
    offsetMap.put(0, 100L);
    AthenaIngestionGateway.CheckpointKafkaOffsetInfo.KafkaOffsetsInfo.Offsets offsets =
        new AthenaIngestionGateway.CheckpointKafkaOffsetInfo.KafkaOffsetsInfo.Offsets(offsetMap);
    AthenaIngestionGateway.CheckpointKafkaOffsetInfo.KafkaOffsetsInfo kafkaOffsetsInfo =
        new AthenaIngestionGateway.CheckpointKafkaOffsetInfo.KafkaOffsetsInfo("topic-id", "cluster", offsets);
    AthenaIngestionGateway.CheckpointKafkaOffsetInfo offsetInfo =
        new AthenaIngestionGateway.CheckpointKafkaOffsetInfo(
            "123", Collections.singletonList(kafkaOffsetsInfo), "20260806100000", "123");
    AthenaIngestionGateway gateway = Mockito.mock(AthenaIngestionGateway.class);
    Mockito.when(gateway.getKafkaCheckpointsInfo(
        Mockito.anyString(), Mockito.anyString(), Mockito.anyLong(),
        Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
        Mockito.anyString(), Mockito.anyInt(), Mockito.anyMap(),
        Mockito.anyString(), Mockito.anyString())).thenReturn(Option.of(offsetInfo));

    String result = StreamerUtil.collectKafkaOffsetCheckpoint(conf, 123L, new FlinkCheckpointClient(gateway));

    assertEquals("kafka_metadata%3Atopic%3A0:100;kafka_metadata%3Atopic%3A1:200;"
        + "kafka_metadata%3Akafka_cluster%3Atopic%3A:cluster", result);

    HashMap<String, String> metadata = new HashMap<>();
    Configuration incomplete = new Configuration();
    incomplete.set(FlinkOptions.WRITE_EXTRA_METADATA_ENABLED, true);
    incomplete.set(FlinkOptions.KAFKA_TOPIC_NAME, "topic");
    incomplete.set(FlinkOptions.SOURCE_KAFKA_CLUSTER, "cluster");
    StreamerUtil.addKafkaOffsetMetaData(incomplete, metadata, 123L);
    assertEquals("kafka_metadata%3Akafka_cluster%3Atopic%3A:cluster",
        metadata.get(StreamerUtil.HOODIE_METADATA_KEY));
  }

  @Test
  void testWriteStatusFailFastValidation() {
    WriteStatus writeStatus = new WriteStatus();
    writeStatus.markFailure("key", "partition", new RuntimeException("failure"));
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.WRITE_FAIL_FAST, true);

    assertThrows(HoodieException.class,
        () -> StreamerUtil.validateWriteStatus(conf, "123", Collections.singletonList(writeStatus)));
  }

  private Configuration kafkaCheckpointConf() {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.DC, "dc");
    conf.set(FlinkOptions.ENV, "production");
    conf.set(FlinkOptions.JOB_NAME, "job");
    conf.set(FlinkOptions.HADOOP_USER, "user");
    conf.set(FlinkOptions.SOURCE_KAFKA_CLUSTER, "cluster");
    conf.set(FlinkOptions.TARGET_KAFKA_CLUSTER, "target");
    conf.set(FlinkOptions.ATHENA_SERVICE, "athena");
    conf.set(FlinkOptions.CALLER_SERVICE_NAME, "caller");
    conf.set(FlinkOptions.KAFKA_TOPIC_NAME, "topic");
    conf.set(FlinkOptions.TOPIC_ID, "topic-id");
    conf.set(FlinkOptions.SERVICE_TIER, "tier");
    conf.set(FlinkOptions.SERVICE_NAME, "service");
    return conf;
  }

  private static StoragePathInfo pathInfo(String path, long length) {
    return new StoragePathInfo(new StoragePath(path), length, false, (short) 1, 128, 0);
  }
}
