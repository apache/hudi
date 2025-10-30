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

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.avro.model.HoodieFileStatus;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.bootstrap.BootstrapMode;
import org.apache.hudi.client.bootstrap.FullRecordBootstrapDataProvider;
import org.apache.hudi.client.bootstrap.selector.BootstrapModeSelector;
import org.apache.hudi.client.bootstrap.selector.FullRecordBootstrapModeSelector;
import org.apache.hudi.client.bootstrap.selector.MetadataOnlyBootstrapModeSelector;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.bootstrap.index.BootstrapIndex;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ParquetReaderIterator;
import org.apache.hudi.common.util.PartitionPathEncodeUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieBootstrapConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat;
import org.apache.hudi.index.HoodieIndex.IndexType;
import org.apache.hudi.io.hadoop.HoodieAvroParquetReader;
import org.apache.hudi.keygen.NonpartitionedKeyGenerator;
import org.apache.hudi.keygen.SimpleKeyGenerator;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.bootstrap.BootstrapUtils;
import org.apache.hudi.testutils.HoodieMergeOnReadTestUtils;
import org.apache.hudi.testutils.HoodieSparkClientTestBase;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_FILE_NAME_GENERATOR;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.spark.sql.functions.callUDF;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests Bootstrap Client functionality.
 */
@Disabled("HUDI-7353")
@Tag("functional")
public class TestBootstrap extends HoodieSparkClientTestBase {

  public static final String TRIP_HIVE_COLUMN_TYPES = "bigint,string,string,string,string,double,double,double,double,"
      + "struct<amount:double,currency:string>,array<struct<amount:double,currency:string>>,boolean";

  @TempDir
  public java.nio.file.Path tmpFolder;

  protected String bootstrapBasePath = null;

  private HoodieParquetInputFormat roInputFormat;
  private JobConf roJobConf;

  private HoodieParquetRealtimeInputFormat rtInputFormat;
  private JobConf rtJobConf;

  @BeforeEach
  public void setUp() throws Exception {
    bootstrapBasePath = tmpFolder.toAbsolutePath() + "/data";
    initPath();
    initSparkContexts();
    initTestDataGenerator();
    initMetaClient();
    // initialize parquet input format
    reloadInputFormats();
  }

  @AfterEach
  public void tearDown() throws IOException {
    cleanupSparkContexts();
    cleanupClients();
    cleanupTestDataGenerator();
  }

  private void reloadInputFormats() {
    // initialize parquet input format
    roInputFormat = new HoodieParquetInputFormat();
    roJobConf = new JobConf(jsc.hadoopConfiguration());
    roInputFormat.setConf(roJobConf);

    rtInputFormat = new HoodieParquetRealtimeInputFormat();
    rtJobConf = new JobConf(jsc.hadoopConfiguration());
    rtInputFormat.setConf(rtJobConf);
  }

  public Schema generateNewDataSetAndReturnSchema(long timestamp, int numRecords, List<String> partitionPaths,
      String srcPath) throws Exception {
    boolean isPartitioned = partitionPaths != null && !partitionPaths.isEmpty();
    Dataset<Row> df =
        generateTestRawTripDataset(timestamp, 0, numRecords, partitionPaths, jsc, sqlContext);
    df.printSchema();
    if (isPartitioned) {
      df.write().partitionBy("datestr").format("parquet").mode(SaveMode.Overwrite).save(srcPath);
    } else {
      df.write().format("parquet").mode(SaveMode.Overwrite).save(srcPath);
    }
    String filePath =
        HadoopFSUtils.toPath(
            BootstrapUtils.getAllLeafFoldersWithFiles(getConfig().getBaseFileFormat(),
                    metaClient.getStorage(),
                    srcPath, context).stream().findAny().map(p -> p.getValue().stream().findAny())
                .orElse(null).get().getPath()).toString();
    HoodieAvroParquetReader parquetReader =
        new HoodieAvroParquetReader(metaClient.getStorage(), new StoragePath(filePath));
    return parquetReader.getSchema();
  }

  @Test
  public void testMetadataBootstrapNonpartitionedCOW() throws Exception {
    testBootstrapCommon(false, false, EffectiveMode.METADATA_BOOTSTRAP_MODE);
  }

  @Test
  public void testMetadataBootstrapWithUpdatesCOW() throws Exception {
    testBootstrapCommon(true, false, EffectiveMode.METADATA_BOOTSTRAP_MODE);
  }

  private enum EffectiveMode {
    FULL_BOOTSTRAP_MODE,
    METADATA_BOOTSTRAP_MODE,
    MIXED_BOOTSTRAP_MODE
  }

  private void testBootstrapCommon(boolean partitioned, boolean deltaCommit, EffectiveMode mode) throws Exception {
    testBootstrapCommon(partitioned, deltaCommit, mode, BootstrapMode.METADATA_ONLY);
  }

  private void testBootstrapCommon(boolean partitioned, boolean deltaCommit, EffectiveMode mode, BootstrapMode modeForRegexMatch) throws Exception {

    String keyGeneratorClass = partitioned ? SimpleKeyGenerator.class.getCanonicalName()
        : NonpartitionedKeyGenerator.class.getCanonicalName();
    if (deltaCommit) {
      metaClient = HoodieTestUtils.init(basePath, HoodieTableType.MERGE_ON_READ, bootstrapBasePath, true, keyGeneratorClass, "partition_path");
    } else {
      metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE, bootstrapBasePath, true, keyGeneratorClass, "partition_path");
    }

    int totalRecords = 100;
    final String bootstrapModeSelectorClass;
    final String bootstrapCommitInstantTs;
    final boolean checkNumRawFiles;
    final boolean isBootstrapIndexCreated;
    final int numInstantsAfterBootstrap;
    final List<String> bootstrapInstants;
    switch (mode) {
      case FULL_BOOTSTRAP_MODE:
        bootstrapModeSelectorClass = FullRecordBootstrapModeSelector.class.getCanonicalName();
        bootstrapCommitInstantTs = HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS;
        checkNumRawFiles = false;
        isBootstrapIndexCreated = false;
        numInstantsAfterBootstrap = 1;
        bootstrapInstants = Arrays.asList(bootstrapCommitInstantTs);
        break;
      case METADATA_BOOTSTRAP_MODE:
        bootstrapModeSelectorClass = MetadataOnlyBootstrapModeSelector.class.getCanonicalName();
        bootstrapCommitInstantTs = HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS;
        checkNumRawFiles = true;
        isBootstrapIndexCreated = true;
        numInstantsAfterBootstrap = 1;
        bootstrapInstants = Arrays.asList(bootstrapCommitInstantTs);
        break;
      default:
        bootstrapModeSelectorClass = TestRandomBootstrapModeSelector.class.getName();
        bootstrapCommitInstantTs = HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS;
        checkNumRawFiles = false;
        isBootstrapIndexCreated = true;
        numInstantsAfterBootstrap = 2;
        bootstrapInstants = Arrays.asList(HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS,
            HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS);
        break;
    }
    List<String> partitions = partitioned ? Arrays.asList("2020/04/01", "2020/04/02", "2020/04/03") : Collections.EMPTY_LIST;
    long timestamp = Instant.now().toEpochMilli();
    Schema schema = generateNewDataSetAndReturnSchema(timestamp, totalRecords, partitions, bootstrapBasePath);
    HoodieWriteConfig config = getConfigBuilder(schema.toString())
        .withSchema(schema.toString())
        .withKeyGenerator(keyGeneratorClass)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withMaxNumDeltaCommitsBeforeCompaction(1)
            .build())
        .withBootstrapConfig(HoodieBootstrapConfig.newBuilder()
            .withBootstrapBasePath(bootstrapBasePath)
            .withFullBootstrapInputProvider(TestFullBootstrapDataProvider.class.getName())
            .withBootstrapParallelism(3)
            .withBootstrapModeSelector(bootstrapModeSelectorClass)
            .withBootstrapModeForRegexMatch(modeForRegexMatch).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true).withMaxNumDeltaCommitsBeforeCompaction(3)
            .withMetadataIndexColumnStats(false).build()) // HUDI-8774
        .build();
    config.setValue(HoodieTableConfig.ORDERING_FIELDS, "timestamp");

    SparkRDDWriteClient client = new SparkRDDWriteClient(context, config, true);
    client.bootstrap(Option.empty());
    checkBootstrapResults(totalRecords, schema, bootstrapCommitInstantTs, checkNumRawFiles, numInstantsAfterBootstrap,
        numInstantsAfterBootstrap, timestamp, timestamp, deltaCommit, bootstrapInstants, true);

    // Rollback Bootstrap
    metaClient.getActiveTimeline().reload().getInstantsAsStream()
        .filter(s -> s.equals(INSTANT_GENERATOR.createNewInstant(State.COMPLETED,
            deltaCommit ? HoodieTimeline.DELTA_COMMIT_ACTION : HoodieTimeline.COMMIT_ACTION,
            bootstrapCommitInstantTs)))
        .forEach(instant -> TimelineUtils.deleteInstantFile(
            metaClient.getStorage(), metaClient.getTimelinePath(), instant, INSTANT_FILE_NAME_GENERATOR));
    metaClient.reloadActiveTimeline();
    client.getTableServiceClient().rollbackFailedBootstrap();
    metaClient.reloadActiveTimeline();
    assertEquals(0, metaClient.getCommitsTimeline().countInstants());
    assertEquals(0L, BootstrapUtils.getAllLeafFoldersWithFiles(config.getBaseFileFormat(),
            metaClient.getStorage(), basePath, context)
        .stream().mapToLong(f -> f.getValue().size()).sum());

    BootstrapIndex index = BootstrapIndex.getBootstrapIndex(metaClient);
    assertFalse(index.useIndex());
    client.close();

    // Run bootstrap again
    client = new SparkRDDWriteClient(context, config, true);
    client.bootstrap(Option.empty());

    metaClient.reloadActiveTimeline();
    index = BootstrapIndex.getBootstrapIndex(metaClient);
    if (isBootstrapIndexCreated) {
      assertTrue(index.useIndex());
    } else {
      assertFalse(index.useIndex());
    }

    checkBootstrapResults(totalRecords, schema, bootstrapCommitInstantTs, checkNumRawFiles, numInstantsAfterBootstrap,
        numInstantsAfterBootstrap, timestamp, timestamp, deltaCommit, bootstrapInstants, true);

    // Upsert case
    long updateTimestamp = Instant.now().toEpochMilli();
    String updateSPath = tmpFolder.toAbsolutePath() + "/data2";
    generateNewDataSetAndReturnSchema(updateTimestamp, totalRecords, partitions, updateSPath);
    JavaRDD<HoodieRecord> updateBatch =
        generateInputBatch(jsc, BootstrapUtils.getAllLeafFoldersWithFiles(config.getBaseFileFormat(),
                metaClient.getStorage(), updateSPath, context),
                schema);
    String newInstantTs = client.startCommit();
    client.upsert(updateBatch, newInstantTs);
    checkBootstrapResults(totalRecords, schema, newInstantTs, false, numInstantsAfterBootstrap + 1,
        updateTimestamp, deltaCommit ? timestamp : updateTimestamp, deltaCommit, true);

    if (deltaCommit) {
      Option<String> compactionInstant = client.scheduleCompaction(Option.empty());
      assertTrue(compactionInstant.isPresent());
      HoodieWriteMetadata result = client.compact(compactionInstant.get());
      client.commitCompaction(compactionInstant.get(), result, Option.empty());
      assertTrue(metaClient.reloadActiveTimeline().filterCompletedInstants().containsInstant(compactionInstant.get()));
      checkBootstrapResults(totalRecords, schema, compactionInstant.get(), checkNumRawFiles,
          numInstantsAfterBootstrap + 2, 2, updateTimestamp, updateTimestamp, !deltaCommit,
          Arrays.asList(compactionInstant.get()), false);
    }
    client.close();
  }

  @Test
  public void testMetadataBootstrapWithUpdatesMOR() throws Exception {
    testBootstrapCommon(true, true, EffectiveMode.METADATA_BOOTSTRAP_MODE);
  }

  @Test
  public void testFullBootstrapOnlyCOW() throws Exception {
    testBootstrapCommon(true, false, EffectiveMode.FULL_BOOTSTRAP_MODE);
  }

  @Test
  public void testFullBootstrapWithUpdatesMOR() throws Exception {
    testBootstrapCommon(true, true, EffectiveMode.FULL_BOOTSTRAP_MODE);
  }

  @Test
  public void testFullBootstrapWithRegexModeWithOnlyCOW() throws Exception {
    testBootstrapCommon(true, false, EffectiveMode.FULL_BOOTSTRAP_MODE, BootstrapMode.FULL_RECORD);
  }

  @Test
  public void testFullBootstrapWithRegexModeWithUpdatesMOR() throws Exception {
    testBootstrapCommon(true, true, EffectiveMode.FULL_BOOTSTRAP_MODE, BootstrapMode.FULL_RECORD);
  }

  @Test
  public void testMetaAndFullBootstrapCOW() throws Exception {
    testBootstrapCommon(true, false, EffectiveMode.MIXED_BOOTSTRAP_MODE);
  }

  @Test
  public void testMetadataAndFullBootstrapWithUpdatesMOR() throws Exception {
    testBootstrapCommon(true, true, EffectiveMode.MIXED_BOOTSTRAP_MODE);
  }

  private void checkBootstrapResults(int totalRecords, Schema schema, String maxInstant, boolean checkNumRawFiles,
      int expNumInstants, long expTimestamp, long expROTimestamp, boolean isDeltaCommit, boolean validateRecordsForCommitTime) throws Exception {
    checkBootstrapResults(totalRecords, schema, maxInstant, checkNumRawFiles, expNumInstants, expNumInstants,
        expTimestamp, expROTimestamp, isDeltaCommit, Arrays.asList(maxInstant), validateRecordsForCommitTime);
  }

  private void checkBootstrapResults(int totalRecords, Schema schema, String instant, boolean checkNumRawFiles,
      int expNumInstants, int numVersions, long expTimestamp, long expROTimestamp, boolean isDeltaCommit,
      List<String> instantsWithValidRecords, boolean validateRecordsForCommitTime) throws Exception {
    metaClient.reloadActiveTimeline();
    assertEquals(expNumInstants, metaClient.getCommitsTimeline().filterCompletedInstants().countInstants());
    assertEquals(instant, metaClient.getActiveTimeline()
        .getCommitsTimeline().filterCompletedInstants().lastInstant().get().requestedTime());
    verifyNoMarkerInTempFolder();

    Dataset<Row> bootstrapped = sqlContext.read().format("parquet").load(basePath);
    Dataset<Row> original = sqlContext.read().format("parquet").load(bootstrapBasePath);
    bootstrapped.registerTempTable("bootstrapped");
    original.registerTempTable("original");
    if (checkNumRawFiles) {
      List<HoodieFileStatus> files =
          BootstrapUtils.getAllLeafFoldersWithFiles(getConfig().getBaseFileFormat(),
              metaClient.getStorage(),
          bootstrapBasePath, context).stream().flatMap(x -> x.getValue().stream()).collect(Collectors.toList());
      assertEquals(files.size() * numVersions,
          sqlContext.sql("select distinct _hoodie_file_name from bootstrapped").count());
    }

    if (!isDeltaCommit) {
      String predicate = String.join(", ",
          instantsWithValidRecords.stream().map(p -> "\"" + p + "\"").collect(Collectors.toList()));
      if (validateRecordsForCommitTime) {
        assertEquals(totalRecords, sqlContext.sql("select * from bootstrapped where _hoodie_commit_time IN "
            + "(" + predicate + ")").count());
      }
      Dataset<Row> missingOriginal = sqlContext.sql("select a._row_key from original a where a._row_key not "
          + "in (select _hoodie_record_key from bootstrapped)");
      assertEquals(0, missingOriginal.count());
      Dataset<Row> missingBootstrapped = sqlContext.sql("select a._hoodie_record_key from bootstrapped a "
          + "where a._hoodie_record_key not in (select _row_key from original)");
      assertEquals(0, missingBootstrapped.count());
    }

    // RO Input Format Read
    reloadInputFormats();
    List<GenericRecord> records = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(
        HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration()),
        FSUtils.getAllPartitionPaths(context, metaClient, false).stream()
            .map(f -> basePath + "/" + f).collect(Collectors.toList()),
        basePath, roJobConf, false, schema, TRIP_HIVE_COLUMN_TYPES, false, new ArrayList<>());
    assertEquals(totalRecords, records.size());
    Set<String> seenKeys = new HashSet<>();
    for (GenericRecord r : records) {
      assertEquals(r.get("_row_key").toString(), r.get("_hoodie_record_key").toString(), "Record :" + r);
      assertEquals(expROTimestamp, ((LongWritable)r.get("timestamp")).get(), 0.1, "Record :" + r);
      assertFalse(seenKeys.contains(r.get("_hoodie_record_key").toString()));
      seenKeys.add(r.get("_hoodie_record_key").toString());
    }
    assertEquals(totalRecords, seenKeys.size());

    // RT Input Format Read
    reloadInputFormats();
    seenKeys = new HashSet<>();
    records = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(
        HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration()),
        FSUtils.getAllPartitionPaths(context, metaClient, HoodieMetadataConfig.DEFAULT_METADATA_ENABLE_FOR_READERS).stream()
            .map(f -> basePath + "/" + f).collect(Collectors.toList()),
        basePath, rtJobConf, true, schema, TRIP_HIVE_COLUMN_TYPES, false, new ArrayList<>());
    assertEquals(totalRecords, records.size());
    for (GenericRecord r : records) {
      assertEquals(r.get("_row_key").toString(), r.get("_hoodie_record_key").toString(), "Realtime Record :" + r);
      assertEquals(expTimestamp, ((LongWritable)r.get("timestamp")).get(),0.1, "Realtime Record :" + r);
      assertFalse(seenKeys.contains(r.get("_hoodie_record_key").toString()));
      seenKeys.add(r.get("_hoodie_record_key").toString());
    }
    assertEquals(totalRecords, seenKeys.size());

    // RO Input Format Read - Project only Hoodie Columns
    reloadInputFormats();
    records = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(
        HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration()),
        FSUtils.getAllPartitionPaths(context, metaClient, HoodieMetadataConfig.DEFAULT_METADATA_ENABLE_FOR_READERS).stream()
            .map(f -> basePath + "/" + f).collect(Collectors.toList()),
        basePath, roJobConf, false, schema, TRIP_HIVE_COLUMN_TYPES,
        true, HoodieRecord.HOODIE_META_COLUMNS);
    assertEquals(totalRecords, records.size());
    seenKeys = new HashSet<>();
    for (GenericRecord r : records) {
      assertFalse(seenKeys.contains(r.get("_hoodie_record_key").toString()));
      seenKeys.add(r.get("_hoodie_record_key").toString());
    }
    assertEquals(totalRecords, seenKeys.size());

    //RT Input Format Read - Project only Hoodie Columns
    reloadInputFormats();
    seenKeys = new HashSet<>();
    records = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(
        HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration()),
        FSUtils.getAllPartitionPaths(context, metaClient, HoodieMetadataConfig.DEFAULT_METADATA_ENABLE_FOR_READERS).stream()
            .map(f -> basePath + "/" + f).collect(Collectors.toList()),
        basePath, rtJobConf, true, schema, TRIP_HIVE_COLUMN_TYPES, true,
        HoodieRecord.HOODIE_META_COLUMNS);
    assertEquals(totalRecords, records.size());
    for (GenericRecord r : records) {
      assertFalse(seenKeys.contains(r.get("_hoodie_record_key").toString()));
      seenKeys.add(r.get("_hoodie_record_key").toString());
    }
    assertEquals(totalRecords, seenKeys.size());

    // RO Input Format Read - Project only non-hoodie column
    reloadInputFormats();
    records = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(
        HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration()),
        FSUtils.getAllPartitionPaths(context, metaClient, HoodieMetadataConfig.DEFAULT_METADATA_ENABLE_FOR_READERS).stream()
            .map(f -> basePath + "/" + f).collect(Collectors.toList()),
        basePath, roJobConf, false, schema, TRIP_HIVE_COLUMN_TYPES, true,
        Arrays.asList("_row_key"));
    assertEquals(totalRecords, records.size());
    seenKeys = new HashSet<>();
    for (GenericRecord r : records) {
      assertFalse(seenKeys.contains(r.get("_row_key").toString()));
      seenKeys.add(r.get("_row_key").toString());
    }
    assertEquals(totalRecords, seenKeys.size());

    // RT Input Format Read - Project only non-hoodie column
    reloadInputFormats();
    seenKeys = new HashSet<>();
    records = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(
        HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration()),
        FSUtils.getAllPartitionPaths(context, metaClient, HoodieMetadataConfig.DEFAULT_METADATA_ENABLE_FOR_READERS).stream()
            .map(f -> basePath + "/" + f).collect(Collectors.toList()),
        basePath, rtJobConf, true, schema, TRIP_HIVE_COLUMN_TYPES, true,
        Arrays.asList("_row_key"));
    assertEquals(totalRecords, records.size());
    for (GenericRecord r : records) {
      assertFalse(seenKeys.contains(r.get("_row_key").toString()));
      seenKeys.add(r.get("_row_key").toString());
    }
    assertEquals(totalRecords, seenKeys.size());
  }

  private void verifyNoMarkerInTempFolder() throws IOException {
    String tempFolderPath = metaClient.getTempFolderPath();
    FileSystem fileSystem = HadoopFSUtils.getFs(tempFolderPath, jsc.hadoopConfiguration());
    assertEquals(0, fileSystem.listStatus(new Path(tempFolderPath)).length);
  }

  public static class TestFullBootstrapDataProvider extends FullRecordBootstrapDataProvider<JavaRDD<HoodieRecord>> {

    public TestFullBootstrapDataProvider(TypedProperties props, HoodieSparkEngineContext context) {
      super(props, context);
    }

    @Override
    public JavaRDD<HoodieRecord> generateInputRecords(String tableName, String sourceBasePath,
        List<Pair<String, List<HoodieFileStatus>>> partitionPaths, HoodieWriteConfig config) {
      String filePath = HadoopFSUtils.toPath(partitionPaths.stream().flatMap(p -> p.getValue().stream())
          .findAny().get().getPath()).toString();
      ParquetFileReader reader = null;
      JavaSparkContext jsc = HoodieSparkEngineContext.getSparkContext(context);
      try {
        reader = ParquetFileReader.open(jsc.hadoopConfiguration(), new Path(filePath));
      } catch (IOException e) {
        throw new HoodieIOException(e.getMessage(), e);
      }
      MessageType parquetSchema = reader.getFooter().getFileMetaData().getSchema();
      Schema schema =  new AvroSchemaConverter().convert(parquetSchema);
      return generateInputBatch(jsc, partitionPaths, schema);
    }
  }

  private static JavaRDD<HoodieRecord> generateInputBatch(JavaSparkContext jsc,
      List<Pair<String, List<HoodieFileStatus>>> partitionPaths, Schema writerSchema) {
    List<Pair<String, Path>> fullFilePathsWithPartition = partitionPaths.stream().flatMap(p -> p.getValue().stream()
        .map(x -> Pair.of(p.getKey(), HadoopFSUtils.toPath(x.getPath())))).collect(Collectors.toList());
    return jsc.parallelize(fullFilePathsWithPartition.stream().flatMap(p -> {
      try {
        Configuration conf = jsc.hadoopConfiguration();
        AvroReadSupport.setAvroReadSchema(conf, writerSchema);
        Iterator<GenericRecord> recIterator = new ParquetReaderIterator(
            AvroParquetReader.<GenericRecord>builder(p.getValue()).withConf(conf).build());
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(recIterator, 0), false).map(gr -> {
          String key = gr.get("_row_key").toString();
          String pPath = p.getKey();
          return new HoodieAvroIndexedRecord(new HoodieKey(key, pPath), gr);
        });
      } catch (IOException ioe) {
        throw new HoodieIOException(ioe.getMessage(), ioe);
      }
    }).collect(Collectors.toList()));
  }

  public static class TestRandomBootstrapModeSelector extends BootstrapModeSelector {

    private int currIdx = new Random().nextInt(2);

    public TestRandomBootstrapModeSelector(HoodieWriteConfig writeConfig) {
      super(writeConfig);
    }

    @Override
    public Map<BootstrapMode, List<String>> select(List<Pair<String, List<HoodieFileStatus>>> partitions) {
      List<Pair<BootstrapMode, String>> selections = new ArrayList<>();
      partitions.stream().forEach(p -> {
        final BootstrapMode mode;
        if (currIdx == 0) {
          mode = BootstrapMode.METADATA_ONLY;
        } else {
          mode = BootstrapMode.FULL_RECORD;
        }
        currIdx = (currIdx + 1) % 2;
        selections.add(Pair.of(mode, p.getKey()));
      });
      return selections.stream().collect(Collectors.groupingBy(Pair::getKey, mapping(Pair::getValue, toList())));
    }
  }

  @Override
  public HoodieWriteConfig.Builder getConfigBuilder(String schemaStr) {
    HoodieWriteConfig.Builder builder = getConfigBuilder(schemaStr, IndexType.BLOOM)
        .withExternalSchemaTrasformation(true);
    TypedProperties properties = new TypedProperties();
    properties.setProperty(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "_row_key");
    properties.setProperty(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "datestr");
    builder = builder.withProps(properties);
    return builder;
  }

  public static Dataset<Row> generateTestRawTripDataset(long timestamp, int from, int to, List<String> partitionPaths,
                                                        JavaSparkContext jsc, SQLContext sqlContext) {
    boolean isPartitioned = partitionPaths != null && !partitionPaths.isEmpty();
    final List<String> records = new ArrayList<>();
    IntStream.range(from, to).forEach(i -> {
      String id = "" + i;
      records.add(new HoodieTestDataGenerator().generateGenericRecord("trip_" + id, Long.toString(timestamp), "rider_" + id, "driver_" + id, timestamp, false, false).toString());
    });
    if (isPartitioned) {
      sqlContext.udf().register("partgen",
          (UDF1<String, String>) (val) -> PartitionPathEncodeUtils.escapePathName(partitionPaths.get(
              Integer.parseInt(val.split("_")[1]) % partitionPaths.size())),
          DataTypes.StringType);
    }
    JavaRDD rdd = jsc.parallelize(records);
    Dataset<Row> df = sqlContext.read().json(rdd);
    if (isPartitioned) {
      df = df.withColumn("datestr", callUDF("partgen", new Column("_row_key")));
      // Order the columns to ensure generated avro schema aligns with Hive schema
      df = df.select("timestamp", "_row_key", "partition_path", "rider", "driver", "begin_lat", "begin_lon",
          "end_lat", "end_lon", "fare", "tip_history", "_hoodie_is_deleted", "datestr");
    } else {
      // Order the columns to ensure generated avro schema aligns with Hive schema
      df = df.select("timestamp", "_row_key", "partition_path", "rider", "driver", "begin_lat", "begin_lon",
          "end_lat", "end_lon", "fare", "tip_history", "_hoodie_is_deleted");
    }
    return df;
  }
}
