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

package org.apache.hudi.hadoop.hive;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.FileCreateUtilsLegacy;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.testutils.minicluster.HdfsTestService;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.SchemaEvolutionContext;
import org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat;
import org.apache.hudi.hadoop.testutils.InputFormatTestUtil;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.convert.InternalSchemaConverter;
import org.apache.hudi.internal.schema.utils.SerDeHelper;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapper;
import org.apache.hadoop.hive.ql.io.IOContextMap;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.ql.exec.Utilities.HAS_MAP_WORK;
import static org.apache.hadoop.hive.ql.exec.Utilities.MAPRED_MAPPER_CLASS;
import static org.apache.hudi.common.testutils.HoodieTestUtils.COMMIT_METADATA_SER_DE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieCombineHiveInputFormat extends HoodieCommonTestHarness {

  private static HdfsTestService hdfsTestService;
  private static HoodieStorage storage;
  private static FileSystem fs;

  @BeforeAll
  public static void setUpClass() throws IOException, InterruptedException {
    // Append is not supported in LocalFileSystem. HDFS needs to be setup.
    hdfsTestService = new HdfsTestService();
    fs = hdfsTestService.start(true).getFileSystem();
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

  @BeforeEach
  public void setUp() throws IOException, InterruptedException {
    assertTrue(fs.mkdirs(new Path(tempDir.toAbsolutePath().toString())));
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (fs != null) {
      fs.delete(new Path(tempDir.toAbsolutePath().toString()), true);
    }
  }

  @Test
  public void testInternalSchemaCacheForMR() throws Exception {
    // test for HUDI-8182
    StorageConfiguration<Configuration> conf = HoodieTestUtils.getDefaultStorageConf();
    Schema schema = HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getEvolvedSchema());
    java.nio.file.Path path1 = tempDir.resolve("tblOne");
    java.nio.file.Path path2 = tempDir.resolve("tblTwo");
    HoodieTestUtils.init(conf, path1.toString(), HoodieTableType.MERGE_ON_READ);
    HoodieTestUtils.init(conf, path2.toString(), HoodieTableType.MERGE_ON_READ);
    String commitTime = "100";
    final int numRecords = 10;
    // Create 3 parquet files with 10 records each for partition 1
    File partitionDirOne = InputFormatTestUtil.prepareParquetTable(path1, schema, 3, numRecords, commitTime);
    HoodieCommitMetadata commitMetadataOne = CommitUtils.buildMetadata(Collections.emptyList(), Collections.emptyMap(), Option.empty(), WriteOperationType.UPSERT,
        schema.toString(), HoodieTimeline.COMMIT_ACTION);
    // mock the latest schema to the commit metadata
    InternalSchema internalSchema = InternalSchemaConverter.convert(HoodieSchema.fromAvroSchema(schema));
    commitMetadataOne.addMetadata(SerDeHelper.LATEST_SCHEMA, SerDeHelper.toJson(internalSchema));
    FileCreateUtilsLegacy.createCommit(COMMIT_METADATA_SER_DE, path1.toString(), commitTime, Option.of(commitMetadataOne));
    // Create 3 parquet files with 10 records each for partition 2
    File partitionDirTwo = InputFormatTestUtil.prepareParquetTable(path2, schema, 3, numRecords, commitTime);
    HoodieCommitMetadata commitMetadataTwo = CommitUtils.buildMetadata(Collections.emptyList(), Collections.emptyMap(), Option.empty(), WriteOperationType.UPSERT,
        schema.toString(), HoodieTimeline.COMMIT_ACTION);
    // Mock the latest schema to the commit metadata
    commitMetadataTwo.addMetadata(SerDeHelper.LATEST_SCHEMA, SerDeHelper.toJson(internalSchema));
    FileCreateUtilsLegacy.createCommit(COMMIT_METADATA_SER_DE, path2.toString(), commitTime, Option.of(commitMetadataTwo));

    // Enable schema evolution
    conf.set("hoodie.schema.on.read.enable", "true");

    TableDesc tblDesc = Utilities.defaultTd;
    // Set the input format
    tblDesc.setInputFileFormatClass(HoodieParquetRealtimeInputFormat.class);
    PartitionDesc partDesc = new PartitionDesc(tblDesc, null);
    LinkedHashMap<Path, PartitionDesc> pt = new LinkedHashMap<>();
    LinkedHashMap<Path, ArrayList<String>> tableAlias = new LinkedHashMap<>();
    ArrayList<String> alias = new ArrayList<>();
    // Add partition info one
    alias.add(path1.toAbsolutePath().toString());
    tableAlias.put(new Path(path1.toAbsolutePath().toString()), alias);
    pt.put(new Path(path1.toAbsolutePath().toString()), partDesc);
    // Add partition info two
    alias.add(path2.toAbsolutePath().toString());
    tableAlias.put(new Path(path2.toAbsolutePath().toString()), alias);
    pt.put(new Path(path2.toAbsolutePath().toString()), partDesc);

    MapredWork mrwork = new MapredWork();
    mrwork.getMapWork().setPathToPartitionInfo(pt);
    mrwork.getMapWork().setPathToAliases(tableAlias);
    mrwork.getMapWork().setMapperCannotSpanPartns(true);
    Path mapWorkPath = new Path(tempDir.toAbsolutePath().toString());
    Utilities.setMapRedWork(conf.unwrap(), mrwork, mapWorkPath);
    JobConf jobConf = new JobConf(conf.unwrap());
    // Add the paths
    FileInputFormat.setInputPaths(jobConf, partitionDirOne.getPath() + "," + partitionDirTwo.getPath());
    jobConf.set(HAS_MAP_WORK, "true");
    // The following config tells Hive to choose ExecMapper to read the MAP_WORK
    jobConf.set(MAPRED_MAPPER_CLASS, ExecMapper.class.getName());
    // set SPLIT_MAXSIZE larger  to create one split for 3 files groups
    jobConf.set(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.SPLIT_MAXSIZE, "128000000");

    HoodieCombineHiveInputFormat combineHiveInputFormat = new HoodieCombineHiveInputFormat();
    String tripsHiveColumnTypes = "double,string,string,string,double,double,double,double,double";
    InputFormatTestUtil.setProjectFieldsForInputFormat(jobConf, schema, tripsHiveColumnTypes);
    InputSplit[] splits = combineHiveInputFormat.getSplits(jobConf, 2);
    // Check the internal schema and avro is the same as the original one
    for (InputSplit split : splits) {
      HoodieCombineRealtimeFileSplit inputSplitShim = (HoodieCombineRealtimeFileSplit) ((HoodieCombineRealtimeHiveSplit) split).getInputSplitShim();
      List<FileSplit> fileSplits = inputSplitShim.getRealtimeFileSplits();
      for (FileSplit fileSplit : fileSplits) {
        SchemaEvolutionContext schemaEvolutionContext = new SchemaEvolutionContext(fileSplit, jobConf);
        Option<InternalSchema> internalSchemaFromCache = schemaEvolutionContext.getInternalSchemaFromCache();
        assertEquals(internalSchemaFromCache.get(), internalSchema);
        Schema avroSchemaFromCache = schemaEvolutionContext.getAvroSchemaFromCache();
        assertEquals(avroSchemaFromCache, schema);
      }
    }
  }

  @Test
  public void multiPartitionReadersRealtimeCombineHoodieInputFormat() throws Exception {
    // test for HUDI-1718
    StorageConfiguration<Configuration> conf = HoodieTestUtils.getDefaultStorageConf();
    // initial commit
    Schema schema = HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getEvolvedSchema());
    HoodieTestUtils.init(conf, tempDir.toAbsolutePath().toString(), HoodieTableType.MERGE_ON_READ);
    String commitTime = "100";
    final int numRecords = 1000;
    // Create 3 partitions, each partition holds one parquet file and 1000 records
    List<File> partitionDirs = InputFormatTestUtil
        .prepareMultiPartitionedParquetTable(tempDir, schema, 3, numRecords, commitTime, HoodieTableType.MERGE_ON_READ);
    HoodieCommitMetadata commitMetadata = CommitUtils.buildMetadata(Collections.emptyList(), Collections.emptyMap(), Option.empty(), WriteOperationType.UPSERT,
        schema.toString(), HoodieTimeline.COMMIT_ACTION);
    FileCreateUtilsLegacy.createCommit(COMMIT_METADATA_SER_DE, tempDir.toString(), commitTime, Option.of(commitMetadata));

    TableDesc tblDesc = Utilities.defaultTd;
    // Set the input format
    tblDesc.setInputFileFormatClass(HoodieParquetRealtimeInputFormat.class);
    LinkedHashMap<Path, PartitionDesc> pt = new LinkedHashMap<>();
    LinkedHashMap<Path, ArrayList<String>> talias = new LinkedHashMap<>();

    PartitionDesc partDesc = new PartitionDesc(tblDesc, null);

    pt.put(new Path(tempDir.toAbsolutePath().toString()), partDesc);

    ArrayList<String> arrayList = new ArrayList<>();
    arrayList.add(tempDir.toAbsolutePath().toString());
    talias.put(new Path(tempDir.toAbsolutePath().toString()), arrayList);

    MapredWork mrwork = new MapredWork();
    mrwork.getMapWork().setPathToPartitionInfo(pt);
    mrwork.getMapWork().setPathToAliases(talias);

    Path mapWorkPath = new Path(tempDir.toAbsolutePath().toString());
    Utilities.setMapRedWork(conf.unwrap(), mrwork, mapWorkPath);
    JobConf jobConf = new JobConf(conf.unwrap());
    // Add three partition path to InputPaths
    Path[] partitionDirArray = new Path[partitionDirs.size()];
    partitionDirs.stream().map(p -> new Path(p.getPath())).collect(Collectors.toList()).toArray(partitionDirArray);
    FileInputFormat.setInputPaths(jobConf, partitionDirArray);
    jobConf.set(HAS_MAP_WORK, "true");
    // The following config tells Hive to choose ExecMapper to read the MAP_WORK
    jobConf.set(MAPRED_MAPPER_CLASS, ExecMapper.class.getName());
    // set SPLIT_MAXSIZE larger  to create one split for 3 files groups
    jobConf.set(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.SPLIT_MAXSIZE, "128000000");

    HoodieCombineHiveInputFormat combineHiveInputFormat = new HoodieCombineHiveInputFormat();
    String tripsHiveColumnTypes = "double,string,string,string,double,double,double,double,double";
    InputFormatTestUtil.setPropsForInputFormat(jobConf, schema, tripsHiveColumnTypes);

    InputSplit[] splits = combineHiveInputFormat.getSplits(jobConf, 1);
    // Since the SPLIT_SIZE is large enough, we should create only 1 split with all 3 file groups
    assertEquals(1, splits.length);

    RecordReader<NullWritable, ArrayWritable> recordReader =
        combineHiveInputFormat.getRecordReader(splits[0], jobConf, null);
    NullWritable nullWritable = recordReader.createKey();
    ArrayWritable arrayWritable = recordReader.createValue();
    int counter = 0;

    HoodieCombineRealtimeHiveSplit hiveSplit = (HoodieCombineRealtimeHiveSplit) splits[0];
    HoodieCombineRealtimeFileSplit fileSplit = (HoodieCombineRealtimeFileSplit) hiveSplit.getInputSplitShim();
    List<FileSplit> realtimeFileSplits = fileSplit.getRealtimeFileSplits();

    while (recordReader.next(nullWritable, arrayWritable)) {
      // since each file holds 1000 records, when counter % 1000 == 0,
      // HoodieCombineRealtimeRecordReader will switch reader internal
      // Hive use ioctx to extract partition info, when switch reader, ioctx should be updated.
      if (counter < 1000) {
        assertEquals(IOContextMap.get(jobConf).getInputPath().toString(), realtimeFileSplits.get(0).getPath().toString());
      } else if (counter < 2000) {
        assertEquals(IOContextMap.get(jobConf).getInputPath().toString(), realtimeFileSplits.get(1).getPath().toString());
      } else {
        assertEquals(IOContextMap.get(jobConf).getInputPath().toString(), realtimeFileSplits.get(2).getPath().toString());
      }
      counter++;
    }
    // should read out 3 splits, each for file0, file1, file2 containing 1000 records each
    assertEquals(3000, counter);
    recordReader.close();
  }

  @Test
  public void multiLevelPartitionReadersRealtimeCombineHoodieInputFormat() throws Exception {
    // test for HUDI-1718
    StorageConfiguration<Configuration> conf = HoodieTestUtils.getDefaultStorageConf();
    // initial commit
    Schema schema = HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getEvolvedSchema());
    HoodieTestUtils.init(conf, tempDir.toAbsolutePath().toString(), HoodieTableType.MERGE_ON_READ);
    String commitTime = "100";
    final int numRecords = 1000;
    // Create 3 parquet files with 1000 records each
    File partitionDir = InputFormatTestUtil.prepareParquetTable(tempDir, schema, 3, numRecords, commitTime);
    HoodieCommitMetadata commitMetadata = CommitUtils.buildMetadata(Collections.emptyList(), Collections.emptyMap(), Option.empty(), WriteOperationType.UPSERT,
        schema.toString(), HoodieTimeline.COMMIT_ACTION);
    FileCreateUtilsLegacy.createCommit(COMMIT_METADATA_SER_DE, tempDir.toString(), commitTime, Option.of(commitMetadata));

    TableDesc tblDesc = Utilities.defaultTd;
    // Set the input format
    tblDesc.setInputFileFormatClass(HoodieParquetRealtimeInputFormat.class);
    LinkedHashMap<Path, PartitionDesc> pt = new LinkedHashMap<>();
    LinkedHashMap<Path, ArrayList<String>> talias = new LinkedHashMap<>();
    LinkedHashMap<String, String> partSpec = new LinkedHashMap<>();
    // add three level partitions info
    partSpec.put("year", "2016");
    partSpec.put("month", "05");
    partSpec.put("day", "01");
    PartitionDesc partDesc = new PartitionDesc(tblDesc, partSpec);

    pt.put(new Path(tempDir.toAbsolutePath().toString()), partDesc);

    ArrayList<String> arrayList = new ArrayList<>();
    arrayList.add(tempDir.toAbsolutePath().toString());
    talias.put(new Path(tempDir.toAbsolutePath().toString()), arrayList);

    MapredWork mrwork = new MapredWork();
    mrwork.getMapWork().setPathToPartitionInfo(pt);
    mrwork.getMapWork().setPathToAliases(talias);

    Path mapWorkPath = new Path(tempDir.toAbsolutePath().toString());
    Utilities.setMapRedWork(conf.unwrap(), mrwork, mapWorkPath);
    JobConf jobConf = new JobConf(conf.unwrap());
    // Add the paths
    FileInputFormat.setInputPaths(jobConf, partitionDir.getPath());
    jobConf.set(HAS_MAP_WORK, "true");
    // The following config tells Hive to choose ExecMapper to read the MAP_WORK
    jobConf.set(MAPRED_MAPPER_CLASS, ExecMapper.class.getName());
    // set SPLIT_MAXSIZE larger  to create one split for 3 files groups
    jobConf.set(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.SPLIT_MAXSIZE, "128000000");

    HoodieCombineHiveInputFormat combineHiveInputFormat = new HoodieCombineHiveInputFormat();
    String tripsHiveColumnTypes = "double,string,string,string,double,double,double,double,double";
    List<Schema.Field> fields = schema.getFields();
    String names = fields.stream().map(f -> f.name().toString()).collect(Collectors.joining(","));
    String positions = fields.stream().map(f -> String.valueOf(f.pos())).collect(Collectors.joining(","));

    String hiveColumnNames = fields.stream().map(Schema.Field::name).collect(Collectors.joining(","));
    hiveColumnNames = hiveColumnNames + ",year,month,day";
    String modifiedHiveColumnTypes = HoodieAvroUtils.addMetadataColumnTypes(tripsHiveColumnTypes);
    modifiedHiveColumnTypes = modifiedHiveColumnTypes + ",string,string,string";
    jobConf.set(hive_metastoreConstants.META_TABLE_COLUMNS, hiveColumnNames);
    jobConf.set(hive_metastoreConstants.META_TABLE_COLUMN_TYPES, modifiedHiveColumnTypes);
    jobConf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, names);
    jobConf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, positions);
    // unset META_TABLE_PARTITION_COLUMNS to trigger HUDI-1718
    jobConf.set(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, "");
    InputSplit[] splits = combineHiveInputFormat.getSplits(jobConf, 1);
    // Since the SPLIT_SIZE is large enough, we should create only 1 split with all 3 file groups
    assertEquals(1, splits.length);

    // if HUDI-1718 is not fixed, the follow code will throw exception
    RecordReader<NullWritable, ArrayWritable> recordReader =
        combineHiveInputFormat.getRecordReader(splits[0], jobConf, null);
    NullWritable nullWritable = recordReader.createKey();
    ArrayWritable arrayWritable = recordReader.createValue();
    int counter = 0;
    while (recordReader.next(nullWritable, arrayWritable)) {
      // read over all the splits
      counter++;
    }
    // should read out 3 splits, each for file0, file1, file2 containing 1000 records each
    assertEquals(3000, counter);
    recordReader.close();
  }

  @Test
  public void testMultiReaderRealtimeCombineHoodieInputFormat() throws Exception {
    // test for hudi-1722
    StorageConfiguration<Configuration> conf = HoodieTestUtils.getDefaultStorageConf();
    // initial commit
    Schema schema = HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getEvolvedSchema());
    HoodieTestUtils.init(conf, tempDir.toAbsolutePath().toString(), HoodieTableType.MERGE_ON_READ);
    String commitTime = "100";
    final int numRecords = 1000;
    // Create 3 parquet files with 1000 records each
    File partitionDir = InputFormatTestUtil.prepareParquetTable(tempDir, schema, 3, numRecords, commitTime);
    HoodieCommitMetadata commitMetadata = CommitUtils.buildMetadata(Collections.emptyList(), Collections.emptyMap(), Option.empty(), WriteOperationType.UPSERT,
        schema.toString(), HoodieTimeline.COMMIT_ACTION);
    FileCreateUtilsLegacy.createCommit(COMMIT_METADATA_SER_DE, tempDir.toString(), commitTime, Option.of(commitMetadata));

    String newCommitTime = "101";
    // to trigger the bug of HUDI-1772, only update fileid2
    // insert 1000 update records to log file 2
    // now fileid0, fileid1 has no log files, fileid2 has log file
    HoodieLogFormat.Writer writer =
        InputFormatTestUtil.writeDataBlockToLogFile(partitionDir, storage, schema, "fileid2",
            commitTime, newCommitTime,
            numRecords, numRecords, 0);
    writer.close();

    TableDesc tblDesc = Utilities.defaultTd;
    // Set the input format
    tblDesc.setInputFileFormatClass(HoodieParquetRealtimeInputFormat.class);
    PartitionDesc partDesc = new PartitionDesc(tblDesc, null);
    LinkedHashMap<Path, PartitionDesc> pt = new LinkedHashMap<>();
    LinkedHashMap<Path, ArrayList<String>> tableAlias = new LinkedHashMap<>();
    ArrayList<String> alias = new ArrayList<>();
    alias.add(tempDir.toAbsolutePath().toString());
    tableAlias.put(new Path(tempDir.toAbsolutePath().toString()), alias);
    pt.put(new Path(tempDir.toAbsolutePath().toString()), partDesc);

    MapredWork mrwork = new MapredWork();
    mrwork.getMapWork().setPathToPartitionInfo(pt);
    mrwork.getMapWork().setPathToAliases(tableAlias);
    Path mapWorkPath = new Path(tempDir.toAbsolutePath().toString());
    Utilities.setMapRedWork(conf.unwrap(), mrwork, mapWorkPath);
    JobConf jobConf = new JobConf(conf.unwrap());
    // Add the paths
    FileInputFormat.setInputPaths(jobConf, partitionDir.getPath());
    jobConf.set(HAS_MAP_WORK, "true");
    // The following config tells Hive to choose ExecMapper to read the MAP_WORK
    jobConf.set(MAPRED_MAPPER_CLASS, ExecMapper.class.getName());
    // set SPLIT_MAXSIZE larger  to create one split for 3 files groups
    jobConf.set(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.SPLIT_MAXSIZE, "128000000");

    HoodieCombineHiveInputFormat combineHiveInputFormat = new HoodieCombineHiveInputFormat();
    String tripsHiveColumnTypes = "double,string,string,string,double,double,double,double,double";
    InputFormatTestUtil.setProjectFieldsForInputFormat(jobConf, schema, tripsHiveColumnTypes);
    InputSplit[] splits = combineHiveInputFormat.getSplits(jobConf, 1);
    // Since the SPLIT_SIZE is large enough, we should create only 1 split with all 3 file groups
    assertEquals(1, splits.length);
    RecordReader<NullWritable, ArrayWritable> recordReader =
        combineHiveInputFormat.getRecordReader(splits[0], jobConf, null);
    NullWritable nullWritable = recordReader.createKey();
    ArrayWritable arrayWritable = recordReader.createValue();
    int counter = 0;
    while (recordReader.next(nullWritable, arrayWritable)) {
      // read over all the splits
      counter++;
    }
    // should read out 3 splits, each for file0, file1, file2 containing 1000 records each
    assertEquals(3000, counter);
    recordReader.close();
  }

  @Test
  public void testHoodieRealtimeCombineHoodieInputFormat() throws Exception {

    StorageConfiguration<Configuration> conf = HoodieTestUtils.getDefaultStorageConf();
    // initial commit
    Schema schema = HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getEvolvedSchema());
    HoodieTestUtils.init(conf, tempDir.toAbsolutePath().toString(), HoodieTableType.MERGE_ON_READ);
    String commitTime = "100";
    final int numRecords = 1000;
    // Create 3 parquet files with 1000 records each
    File partitionDir = InputFormatTestUtil.prepareParquetTable(tempDir, schema, 3, numRecords, commitTime);
    HoodieCommitMetadata commitMetadata = CommitUtils.buildMetadata(Collections.emptyList(), Collections.emptyMap(), Option.empty(), WriteOperationType.UPSERT,
        schema.toString(), HoodieTimeline.COMMIT_ACTION);
    FileCreateUtilsLegacy.createCommit(COMMIT_METADATA_SER_DE, tempDir.toString(), commitTime, Option.of(commitMetadata));

    long writtenBytes = 0;
    // insert 1000 update records to log file 0
    String newCommitTime = "101";
    HoodieLogFormat.Writer writer =
        InputFormatTestUtil.writeDataBlockToLogFile(partitionDir, storage, schema, "fileid0",
            commitTime, newCommitTime,
            numRecords, numRecords, 0);
    writtenBytes += writer.getCurrentSize();
    writer.close();
    // insert 1000 update records to log file 1
    writer =
        InputFormatTestUtil.writeDataBlockToLogFile(partitionDir, storage, schema, "fileid1",
            commitTime, newCommitTime,
            numRecords, numRecords, 0);
    writtenBytes += writer.getCurrentSize();
    writer.close();
    // insert 1000 update records to log file 2
    writer =
        InputFormatTestUtil.writeDataBlockToLogFile(partitionDir, storage, schema, "fileid2",
            commitTime, newCommitTime,
            numRecords, numRecords, 0);
    writtenBytes += writer.getCurrentSize();
    writer.close();

    TableDesc tblDesc = Utilities.defaultTd;
    // Set the input format
    tblDesc.setInputFileFormatClass(HoodieParquetRealtimeInputFormat.class);
    PartitionDesc partDesc = new PartitionDesc(tblDesc, null);
    LinkedHashMap<Path, PartitionDesc> pt = new LinkedHashMap<>();
    pt.put(new Path(tempDir.toAbsolutePath().toString()), partDesc);
    LinkedHashMap<Path, ArrayList<String>> tableAlias = new LinkedHashMap<>();
    ArrayList<String> alias = new ArrayList<>();
    alias.add(tempDir.toAbsolutePath().toString());
    tableAlias.put(new Path(tempDir.toAbsolutePath().toString()), alias);
    MapredWork mrwork = new MapredWork();
    mrwork.getMapWork().setPathToPartitionInfo(pt);
    mrwork.getMapWork().setPathToAliases(tableAlias);
    Path mapWorkPath = new Path(tempDir.toAbsolutePath().toString());
    Utilities.setMapRedWork(conf.unwrap(), mrwork, mapWorkPath);
    JobConf jobConf = new JobConf(conf.unwrap());
    // Add the paths
    FileInputFormat.setInputPaths(jobConf, partitionDir.getPath());
    jobConf.set(HAS_MAP_WORK, "true");
    // The following config tells Hive to choose ExecMapper to read the MAP_WORK
    jobConf.set(MAPRED_MAPPER_CLASS, ExecMapper.class.getName());
    // set SPLIT_MAXSIZE larger  to create one split for 3 files groups

    String tripsHiveColumnTypes = "double,string,string,string,double,double,double,double,double";
    InputFormatTestUtil.setPropsForInputFormat(jobConf, schema, tripsHiveColumnTypes);

    HoodieCombineHiveInputFormat combineHiveInputFormat = new HoodieCombineHiveInputFormat();
    InputSplit[] splits = combineHiveInputFormat.getSplits(jobConf, 1);
    // We should get 3 split by default (SPLIT_MAXSIZE is not set)
    assertEquals(3, splits.length);

    jobConf.set(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.SPLIT_MAXSIZE, String.valueOf(writtenBytes / 2));
    splits = combineHiveInputFormat.getSplits(jobConf, 1);
    // Since the SPLIT_SIZE is set half of writtenBytes, we should create 2 split with all 3 file groups
    assertEquals(2, splits.length);

    jobConf.set(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.SPLIT_MAXSIZE, "128000000");
    splits = combineHiveInputFormat.getSplits(jobConf, 1);
    // Since the SPLIT_SIZE is large enough, we should create only 1 split with all 3 file groups
    assertEquals(1, splits.length);
    RecordReader<NullWritable, ArrayWritable> recordReader =
        combineHiveInputFormat.getRecordReader(splits[0], jobConf, null);
    NullWritable nullWritable = recordReader.createKey();
    ArrayWritable arrayWritable = recordReader.createValue();
    int counter = 0;
    while (recordReader.next(nullWritable, arrayWritable)) {
      // read over all the splits
      counter++;
    }
    // should read out 3 splits, each for file0, file1, file2 containing 1000 records each
    assertEquals(3000, counter);
  }

}
