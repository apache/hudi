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

import org.apache.hudi.avro.model.HoodieFileStatus;
import org.apache.hudi.client.bootstrap.FullRecordBootstrapDataProvider;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.bootstrap.FileStatusUtils;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.RawTripTestPayload;
import org.apache.hudi.common.util.ParquetReaderIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;
import org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat;
import org.apache.hudi.io.storage.HoodieAvroParquetReader;
import org.apache.hudi.table.action.bootstrap.BootstrapUtils;
import org.apache.hudi.testutils.HoodieMergeOnReadTestUtils;

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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Tests Bootstrap Client functionality.
 */
@Tag("functional")
public class TestParquetBootstrap extends TestBootstrapBase {

  public static final String TRIP_HIVE_COLUMN_TYPES =
      "bigint,string,string,string,string,double,double,double,double,"
          +
          "struct<amount:double,currency:string>,array<struct<amount:double,currency:string>>,boolean";

  private HoodieParquetInputFormat roInputFormat;
  private JobConf roJobConf;

  private HoodieParquetRealtimeInputFormat rtInputFormat;
  private JobConf rtJobConf;

  @BeforeEach
  @Override
  public void setUp() throws Exception {
    super.setUp();
    // initialize parquet input format
    reloadInputFormats();
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

  @Override
  protected boolean shouldRunTest() {
    return true;
  }

  @Override
  protected HoodieFileFormat getFileFormat() {
    return HoodieFileFormat.PARQUET;
  }

  @Override
  protected Schema getSchema(String srcPath) throws IOException {
    String filePath = FileStatusUtils.toPath(
        BootstrapUtils.getAllLeafFoldersWithFiles(metaClient,
                metaClient.getFs(),
                srcPath, context).stream().findAny().map(p -> p.getValue().stream().findAny())
            .orElse(null).get().getPath()).toString();
    HoodieAvroParquetReader parquetReader =
        new HoodieAvroParquetReader(metaClient.getHadoopConf(), new Path(filePath));
    return parquetReader.getSchema();
  }

  @Override
  protected String getFullBootstrapDataProviderName() {
    return TestFullBootstrapDataProvider.class.getName();
  }

  @Override
  protected void checkBootstrapResults(int totalRecords, Schema schema, String instant,
                                       boolean checkNumRawFiles,
                                       int expNumInstants, int numVersions, long expTimestamp,
                                       long expROTimestamp, boolean isDeltaCommit,
                                       List<String> instantsWithValidRecords,
                                       boolean validateRecordsForCommitTime) throws Exception {
    metaClient.reloadActiveTimeline();
    assertEquals(expNumInstants,
        metaClient.getCommitsTimeline().filterCompletedInstants().countInstants());
    assertEquals(instant, metaClient.getActiveTimeline()
        .getCommitsTimeline().filterCompletedInstants().lastInstant().get().getTimestamp());
    verifyNoMarkerInTempFolder();

    Dataset<Row> bootstrapped = sqlContext.read().format("parquet").load(basePath);
    Dataset<Row> original = sqlContext.read().format("parquet").load(bootstrapBasePath);
    bootstrapped.registerTempTable("bootstrapped");
    original.registerTempTable("original");
    if (checkNumRawFiles) {
      List<HoodieFileStatus> files =
          BootstrapUtils.getAllLeafFoldersWithFiles(metaClient,
                  metaClient.getFs(),
                  bootstrapBasePath, context).stream().flatMap(x -> x.getValue().stream())
              .collect(Collectors.toList());
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
        jsc.hadoopConfiguration(),
        FSUtils.getAllPartitionPaths(context, basePath, HoodieMetadataConfig.DEFAULT_METADATA_ENABLE_FOR_READERS, false).stream()
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
        jsc.hadoopConfiguration(),
        FSUtils.getAllPartitionPaths(context, basePath, HoodieMetadataConfig.DEFAULT_METADATA_ENABLE_FOR_READERS, false).stream()
            .map(f -> basePath + "/" + f).collect(Collectors.toList()),
        basePath, rtJobConf, true, schema,  TRIP_HIVE_COLUMN_TYPES, false, new ArrayList<>());
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
        jsc.hadoopConfiguration(),
        FSUtils.getAllPartitionPaths(context, basePath, HoodieMetadataConfig.DEFAULT_METADATA_ENABLE_FOR_READERS, false).stream()
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
        jsc.hadoopConfiguration(),
        FSUtils.getAllPartitionPaths(context, basePath, HoodieMetadataConfig.DEFAULT_METADATA_ENABLE_FOR_READERS, false).stream()
            .map(f -> basePath + "/" + f).collect(Collectors.toList()),
        basePath, rtJobConf, true, schema,  TRIP_HIVE_COLUMN_TYPES, true,
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
        jsc.hadoopConfiguration(),
        FSUtils.getAllPartitionPaths(context, basePath, HoodieMetadataConfig.DEFAULT_METADATA_ENABLE_FOR_READERS, false).stream()
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
        jsc.hadoopConfiguration(),
        FSUtils.getAllPartitionPaths(context, basePath, HoodieMetadataConfig.DEFAULT_METADATA_ENABLE_FOR_READERS, false).stream()
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
    FileSystem fileSystem = FSUtils.getFs(tempFolderPath, jsc.hadoopConfiguration());
    assertEquals(0, fileSystem.listStatus(new Path(tempFolderPath)).length);
  }

  @Override
  protected JavaRDD<HoodieRecord> generateInputBatch(JavaSparkContext jsc,
                                                     List<Pair<String, List<HoodieFileStatus>>> partitionPaths,
                                                     Schema writerSchema) {
    return generateInputBatchInternal(jsc, partitionPaths, writerSchema);
  }

  private static JavaRDD<HoodieRecord> generateInputBatchInternal(JavaSparkContext jsc,
                                                                  List<Pair<String, List<HoodieFileStatus>>> partitionPaths,
                                                                  Schema writerSchema) {
    List<Pair<String, Path>> fullFilePathsWithPartition =
        partitionPaths.stream().flatMap(p -> p.getValue().stream()
                .map(x -> Pair.of(p.getKey(), FileStatusUtils.toPath(x.getPath()))))
            .collect(Collectors.toList());
    return jsc.parallelize(fullFilePathsWithPartition.stream().flatMap(p -> {
      try {
        Configuration conf = jsc.hadoopConfiguration();
        AvroReadSupport.setAvroReadSchema(conf, writerSchema);
        Iterator<GenericRecord> recIterator = new ParquetReaderIterator(
            AvroParquetReader.<GenericRecord>builder(p.getValue()).withConf(conf).build());
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(recIterator, 0), false)
            .map(gr -> {
              try {
                String key = gr.get("_row_key").toString();
                String pPath = p.getKey();
                return new HoodieAvroRecord<>(new HoodieKey(key, pPath),
                    new RawTripTestPayload(gr.toString(), key, pPath,
                        HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA));
              } catch (IOException e) {
                throw new HoodieIOException(e.getMessage(), e);
              }
            });
      } catch (IOException ioe) {
        throw new HoodieIOException(ioe.getMessage(), ioe);
      }
    }).collect(Collectors.toList()));
  }

  public static class TestFullBootstrapDataProvider extends
      FullRecordBootstrapDataProvider<JavaRDD<HoodieRecord>> {

    public TestFullBootstrapDataProvider(TypedProperties props, HoodieSparkEngineContext context) {
      super(props, context);
    }

    @Override
    public JavaRDD<HoodieRecord> generateInputRecords(String tableName, String sourceBasePath,
                                                      List<Pair<String, List<HoodieFileStatus>>> partitionPaths,
                                                      HoodieWriteConfig config) {
      String filePath =
          FileStatusUtils.toPath(partitionPaths.stream().flatMap(p -> p.getValue().stream())
              .findAny().get().getPath()).toString();
      ParquetFileReader reader = null;
      JavaSparkContext jsc = HoodieSparkEngineContext.getSparkContext(context);
      try {
        reader = ParquetFileReader.open(jsc.hadoopConfiguration(), new Path(filePath));
      } catch (IOException e) {
        throw new HoodieIOException(e.getMessage(), e);
      }
      MessageType parquetSchema = reader.getFooter().getFileMetaData().getSchema();
      Schema schema = new AvroSchemaConverter().convert(parquetSchema);
      return generateInputBatchInternal(jsc, partitionPaths, schema);
    }
  }
}
