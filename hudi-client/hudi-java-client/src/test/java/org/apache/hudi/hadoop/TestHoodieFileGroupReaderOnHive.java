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

package org.apache.hudi.hadoop;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.config.HoodieMemoryConfig;
import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.read.CustomPayloadForTesting;
import org.apache.hudi.common.table.read.TestHoodieFileGroupReaderBase;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.minicluster.HdfsTestService;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.hadoop.hive.HoodieCombineHiveInputFormat;
import org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat;
import org.apache.hudi.hadoop.realtime.HoodieRealtimeRecordReader;
import org.apache.hudi.hadoop.utils.ObjectInspectorCache;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;
import org.apache.hudi.testutils.ArrayWritableTestUtil;
import org.apache.hudi.testutils.HoodieJavaClientTestHarness;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapper;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.ql.exec.Utilities.HAS_MAP_WORK;
import static org.apache.hadoop.hive.ql.exec.Utilities.MAPRED_MAPPER_CLASS;
import static org.apache.hudi.hadoop.HoodieFileGroupReaderBasedRecordReader.getRecordKeyField;
import static org.apache.hudi.hadoop.HoodieFileGroupReaderBasedRecordReader.getStoredPartitionFieldNames;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieFileGroupReaderOnHive extends TestHoodieFileGroupReaderBase<ArrayWritable> {

  @Override
  @Disabled("[HUDI-8072]")
  public void testReadLogFilesOnlyInMergeOnReadTable(RecordMergeMode recordMergeMode, String logDataBlockFormat) throws Exception {
  }

  private static final String PARTITION_COLUMN = "datestr";
  private static JobConf baseJobConf;
  private static HdfsTestService hdfsTestService;
  private static HoodieStorage storage;
  private static FileSystem fs;
  private static StorageConfiguration<Configuration> storageConf;

  //currently always true. If we ever have a test with a nonpartitioned table, the usages of this should be tied together
  private static final boolean USE_FAKE_PARTITION = true;

  @BeforeAll
  public static void setUpClass() throws IOException, InterruptedException {
    // Append is not supported in LocalFileSystem. HDFS needs to be setup.
    hdfsTestService = new HdfsTestService();
    fs = hdfsTestService.start(true).getFileSystem();
    storageConf = HoodieTestUtils.getDefaultStorageConf();
    baseJobConf = new JobConf(storageConf.unwrap());
    baseJobConf.set(HoodieMemoryConfig.MAX_DFS_STREAM_BUFFER_SIZE.key(), String.valueOf(1024 * 1024));
    fs.setConf(baseJobConf);
    storage = new HoodieHadoopStorage(fs);
  }

  @AfterAll
  public static void tearDownClass() throws IOException {
    hdfsTestService.stop();
    if (fs != null) {
      fs.close();
      storage.close();
    }
  }

  @Override
  public StorageConfiguration<?> getStorageConf() {
    return storageConf;
  }

  @Override
  public String getBasePath() {
    return tempDir.toAbsolutePath() + "/myTable";
  }

  @Override
  public HoodieReaderContext<ArrayWritable> getHoodieReaderContext(String tablePath, Schema avroSchema, StorageConfiguration<?> storageConf) {
    HoodieFileGroupReaderBasedRecordReader.HiveReaderCreator readerCreator = (inputSplit, jobConf) -> new MapredParquetInputFormat().getRecordReader(inputSplit, jobConf, null);
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(storageConf).setBasePath(tablePath).build();
    JobConf jobConf = new JobConf(storageConf.unwrapAs(Configuration.class));
    setupJobconf(jobConf);
    return new HiveHoodieReaderContext(readerCreator, getRecordKeyField(metaClient),
        getStoredPartitionFieldNames(new JobConf(storageConf.unwrapAs(Configuration.class)), avroSchema),
        new ObjectInspectorCache(avroSchema, jobConf));
  }

  @Override
  public String getCustomPayload() {
    return CustomPayloadForTesting.class.getName();
  }

  @Override
  public void bootstrapTable(List<HoodieRecord> recordList,
                             Map<String, String> writeConfigs) {
    throw new HoodieNotSupportedException(
        "HUDI-8773: Not supporting bootstrap table testing at the file group reader layer in Hive yet.");
  }

  @Override
  public void commitToTable(List<HoodieRecord> recordList, String operation, Map<String, String> writeConfigs) {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withEngineType(EngineType.JAVA)
        .withEmbeddedTimelineServerEnabled(false)
        .withProps(writeConfigs)
        .withPath(getBasePath())
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .build();

    HoodieJavaClientTestHarness.TestJavaTaskContextSupplier taskContextSupplier = new HoodieJavaClientTestHarness.TestJavaTaskContextSupplier();
    HoodieJavaEngineContext context = new HoodieJavaEngineContext(getStorageConf(), taskContextSupplier);
    //init table if not exists
    Path basePath = new Path(getBasePath());
    try {
      try (FileSystem lfs = basePath.getFileSystem(baseJobConf)) {
        boolean basepathExists = lfs.exists(basePath);
        boolean operationIsInsert = operation.equalsIgnoreCase("insert");
        if (!basepathExists || operationIsInsert) {
          if (basepathExists) {
            lfs.delete(new Path(getBasePath()), true);
          }
          Map<String, Object> initConfigs = new HashMap<>(writeConfigs);
          HoodieTableMetaClient.TableBuilder builder = HoodieTableMetaClient.newTableBuilder()
              .setTableType(writeConfigs.getOrDefault("hoodie.datasource.write.table.type", "MERGE_ON_READ"))
              .setTableName(writeConfigs.get("hoodie.table.name"))
              .setPartitionFields(writeConfigs.getOrDefault("hoodie.datasource.write.partitionpath.field", ""))
              .setRecordMergeMode(RecordMergeMode.getValue(writeConfigs.get("hoodie.record.merge.mode")))
              .set(initConfigs);
          if (writeConfigs.containsKey("hoodie.datasource.write.payload.class")) {
            builder = builder.setPayloadClassName(writeConfigs.get("hoodie.datasource.write.payload.class"));
          }
          builder.initTable(storageConf, getBasePath());
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    HoodieJavaWriteClient writeClient = new HoodieJavaWriteClient(context, writeConfig);
    String instantTime = writeClient.createNewInstantTime();
    writeClient.startCommitWithTime(instantTime);
    if (operation.toLowerCase().equals("insert")) {
      writeClient.insert(recordList, instantTime);
    } else {
      writeClient.upsert(recordList, instantTime);
    }
  }

  @Override
  public void validateRecordsInFileGroup(String tablePath, List<ArrayWritable> actualRecordList, Schema schema, FileSlice fileSlice,
                                         boolean skipMerge) {
    assertEquals(HoodieAvroUtils.addMetadataFields(HoodieTestDataGenerator.AVRO_SCHEMA), schema);
    String fileGroupId = fileSlice.getFileId();
    try {
      //prepare fg reader records to be compared to the baseline reader
      HoodieReaderContext<ArrayWritable> readerContext = getHoodieReaderContext(tablePath, schema, storageConf);
      Map<String, ArrayWritable> recordMap = new HashMap<>();
      for (ArrayWritable record : actualRecordList) {
        recordMap.put(createUniqueKey(readerContext, schema, record, skipMerge), record);
      }

      RecordReader<NullWritable, ArrayWritable> reader = createRecordReader(tablePath, skipMerge);
      // use reader to read log file.
      NullWritable key = reader.createKey();
      ArrayWritable value = reader.createValue();
      //TODO: [HUDI-8209] get rid of logFileCounts and don't guard recordMap.remove(uniqueKey);
      Map<String, Integer> logFileCounts = new HashMap<>();
      while (reader.next(key, value)) {
        if (readerContext.getValue(value, schema, HoodieRecord.FILENAME_METADATA_FIELD).toString().contains(fileGroupId)) {
          //only evaluate records from the specified filegroup. Maybe there is a way to get
          //hive to do this?
          String uniqueKey = createUniqueKey(readerContext, schema, value, skipMerge);
          Integer seenCount = logFileCounts.get(uniqueKey);
          boolean isLogFile = isLogFileRec(readerContext, schema, value);
          if (!skipMerge || seenCount == null) {
            ArrayWritable compVal = recordMap.remove(uniqueKey);
            assertNotNull(compVal);
            ArrayWritableTestUtil.assertArrayWritableEqual(schema, value, compVal, USE_FAKE_PARTITION);
            if (skipMerge && isLogFile) {
              logFileCounts.put(uniqueKey, 1);
            }
          } else {
            assertTrue(isLogFile);
            logFileCounts.put(uniqueKey, seenCount + 1);
          }
        }
        key = reader.createKey();
        value = reader.createValue();
      }
      reader.close();
      assertEquals(0, recordMap.size());
      for (Integer v : logFileCounts.values()) {
        assertEquals(2, v);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void assertRecordsEqual(Schema schema, ArrayWritable expected, ArrayWritable actual) {
    ArrayWritableTestUtil.assertArrayWritableEqual(schema, expected, actual, false);
  }

  private static boolean isLogFileRec(HoodieReaderContext<ArrayWritable> readerContext, Schema schema, ArrayWritable record) {
    return !readerContext.getValue(record, schema, HoodieRecord.FILENAME_METADATA_FIELD).toString().contains(".parquet");
  }

  private static String createUniqueKey(HoodieReaderContext<ArrayWritable> readerContext, Schema schema, ArrayWritable record, boolean isSkipMerge) {
    if (isSkipMerge) {
      return readerContext.getRecordKey(record, schema) + "_" + readerContext.getValue(record, schema, HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString();
    } else {
      return readerContext.getRecordKey(record, schema);
    }
  }

  private RecordReader<NullWritable, ArrayWritable> createRecordReader(String tablePath, boolean isSkipMerge) throws IOException {
    JobConf jobConf = new JobConf(baseJobConf);
    jobConf.set(HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key(), "false");
    jobConf.set(HoodieRealtimeRecordReader.REALTIME_SKIP_MERGE_PROP, String.valueOf(isSkipMerge));

    TableDesc tblDesc = Utilities.defaultTd;
    // Set the input format
    tblDesc.setInputFileFormatClass(HoodieParquetRealtimeInputFormat.class);
    LinkedHashMap<Path, PartitionDesc> pt = new LinkedHashMap<>();
    LinkedHashMap<Path, ArrayList<String>> talias = new LinkedHashMap<>();

    PartitionDesc partDesc = new PartitionDesc(tblDesc, null);

    pt.put(new Path(tablePath), partDesc);

    ArrayList<String> arrayList = new ArrayList<>();
    arrayList.add(tablePath);
    talias.put(new Path(tablePath), arrayList);

    MapredWork mrwork = new MapredWork();
    mrwork.getMapWork().setPathToPartitionInfo(pt);
    mrwork.getMapWork().setPathToAliases(talias);

    Path mapWorkPath = new Path(tablePath);
    Utilities.setMapRedWork(jobConf, mrwork, mapWorkPath);

    // Add three partition path to InputPaths
    Path[] partitionDirArray = new Path[HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS.length];
    Arrays.stream(HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS).map(s -> new Path(tablePath, s)).collect(Collectors.toList()).toArray(partitionDirArray);
    FileInputFormat.setInputPaths(jobConf, partitionDirArray);
    jobConf.set(HAS_MAP_WORK, "true");
    // The following config tells Hive to choose ExecMapper to read the MAP_WORK
    jobConf.set(MAPRED_MAPPER_CLASS, ExecMapper.class.getName());
    // setting the split size to be 3 to create one split for 3 file groups
    jobConf.set(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.SPLIT_MAXSIZE, "128000000");
    setupJobconf(jobConf);

    HoodieCombineHiveInputFormat combineHiveInputFormat = new HoodieCombineHiveInputFormat();
    InputSplit[] splits = combineHiveInputFormat.getSplits(jobConf, 1);

    assertEquals(1, splits.length);
    return  combineHiveInputFormat.getRecordReader(splits[0], jobConf, Reporter.NULL);
  }

  private void setupJobconf(JobConf jobConf) {
    Schema schema = HoodieAvroUtils.addMetadataFields(HoodieTestDataGenerator.AVRO_SCHEMA);
    List<Schema.Field> fields = schema.getFields();
    setHiveColumnNameProps(fields, jobConf, USE_FAKE_PARTITION);
    jobConf.set("columns.types","string,string,string,string,string," + HoodieTestDataGenerator.TRIP_HIVE_COLUMN_TYPES + ",string");
  }

  private void setHiveColumnNameProps(List<Schema.Field> fields, JobConf jobConf, boolean isPartitioned) {
    String names = fields.stream().map(Schema.Field::name).collect(Collectors.joining(","));
    String positions = fields.stream().map(f -> String.valueOf(f.pos())).collect(Collectors.joining(","));
    jobConf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, names);
    jobConf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, positions);

    String hiveOrderedColumnNames = fields.stream().filter(field -> !field.name().equalsIgnoreCase(PARTITION_COLUMN))
        .map(Schema.Field::name).collect(Collectors.joining(","));
    if (isPartitioned) {
      hiveOrderedColumnNames += "," + PARTITION_COLUMN;
      jobConf.set(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, PARTITION_COLUMN);
    }
    jobConf.set(hive_metastoreConstants.META_TABLE_COLUMNS, hiveOrderedColumnNames);
  }
}
