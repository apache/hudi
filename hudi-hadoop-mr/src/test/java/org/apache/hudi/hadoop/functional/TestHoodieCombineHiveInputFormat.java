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

package org.apache.hudi.hadoop.functional;

import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.io.IOContextMap;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.FileCreateUtils;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.testutils.minicluster.MiniClusterUtil;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.hive.HoodieCombineHiveInputFormat;
import org.apache.hudi.hadoop.hive.HoodieCombineRealtimeFileSplit;
import org.apache.hudi.hadoop.hive.HoodieCombineRealtimeHiveSplit;
import org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat;
import org.apache.hudi.hadoop.testutils.InputFormatTestUtil;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapper;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.FileSplit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Disabled
public class TestHoodieCombineHiveInputFormat extends HoodieCommonTestHarness {

  private JobConf jobConf;
  private FileSystem fs;
  private Configuration hadoopConf;

  @BeforeAll
  public static void setUpClass() throws IOException, InterruptedException {
    // Append is not supported in LocalFileSystem. HDFS needs to be setup.
    MiniClusterUtil.setUp();
  }

  @AfterAll
  public static void tearDownClass() {
    MiniClusterUtil.shutdown();
  }

  @BeforeEach
  public void setUp() throws IOException, InterruptedException {
    this.fs = MiniClusterUtil.fileSystem;
    jobConf = new JobConf();
    hadoopConf = HoodieTestUtils.getDefaultHadoopConf();
    assertTrue(fs.mkdirs(new Path(tempDir.toAbsolutePath().toString())));
    HoodieTestUtils.init(MiniClusterUtil.configuration, tempDir.toAbsolutePath().toString(), HoodieTableType.MERGE_ON_READ);
  }

  @Test
  public void multiPartitionReadersRealtimeCombineHoodieInputFormat() throws Exception {
    // test for HUDI-1718
    Configuration conf = new Configuration();
    // initial commit
    Schema schema = HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getEvolvedSchema());
    HoodieTestUtils.init(hadoopConf, tempDir.toAbsolutePath().toString(), HoodieTableType.MERGE_ON_READ);
    String commitTime = "100";
    final int numRecords = 1000;
    // Create 3 partitions, each partition holds one parquet file and 1000 records
    List<File> partitionDirs = InputFormatTestUtil
        .prepareMultiPartitionedParquetTable(tempDir, schema, 3, numRecords, commitTime, HoodieTableType.MERGE_ON_READ);
    HoodieCommitMetadata commitMetadata = CommitUtils.buildMetadata(Collections.emptyList(), Collections.emptyMap(), Option.empty(), WriteOperationType.UPSERT,
        schema.toString(), HoodieTimeline.COMMIT_ACTION);
    FileCreateUtils.createCommit(tempDir.toString(), commitTime, Option.of(commitMetadata));

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
    Utilities.setMapRedWork(conf, mrwork, mapWorkPath);
    jobConf = new JobConf(conf);
    // Add three partition path to InputPaths
    Path[] partitionDirArray = new Path[partitionDirs.size()];
    partitionDirs.stream().map(p -> new Path(p.getPath())).collect(Collectors.toList()).toArray(partitionDirArray);
    FileInputFormat.setInputPaths(jobConf, partitionDirArray);
    jobConf.set(HAS_MAP_WORK, "true");
    // The following config tells Hive to choose ExecMapper to read the MAP_WORK
    jobConf.set(MAPRED_MAPPER_CLASS, ExecMapper.class.getName());
    // setting the split size to be 3 to create one split for 3 file groups
    jobConf.set(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.SPLIT_MAXSIZE, "128000000");

    HoodieCombineHiveInputFormat combineHiveInputFormat = new HoodieCombineHiveInputFormat();
    String tripsHiveColumnTypes = "double,string,string,string,double,double,double,double,double";
    InputFormatTestUtil.setPropsForInputFormat(jobConf, schema, tripsHiveColumnTypes);

    InputSplit[] splits = combineHiveInputFormat.getSplits(jobConf, 1);
    // Since the SPLIT_SIZE is 3, we should create only 1 split with all 3 file groups
    assertEquals(1, splits.length);

    RecordReader<NullWritable, ArrayWritable> recordReader =
        combineHiveInputFormat.getRecordReader(splits[0], jobConf, null);
    NullWritable nullWritable = recordReader.createKey();
    ArrayWritable arrayWritable = recordReader.createValue();
    int counter = 0;

    HoodieCombineRealtimeHiveSplit hiveSplit = (HoodieCombineRealtimeHiveSplit)splits[0];
    HoodieCombineRealtimeFileSplit fileSplit = (HoodieCombineRealtimeFileSplit)hiveSplit.getInputSplitShim();
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
    Configuration conf = new Configuration();
    // initial commit
    Schema schema = HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getEvolvedSchema());
    HoodieTestUtils.init(hadoopConf, tempDir.toAbsolutePath().toString(), HoodieTableType.MERGE_ON_READ);
    String commitTime = "100";
    final int numRecords = 1000;
    // Create 3 parquet files with 1000 records each
    File partitionDir = InputFormatTestUtil.prepareParquetTable(tempDir, schema, 3, numRecords, commitTime);
    HoodieCommitMetadata commitMetadata = CommitUtils.buildMetadata(Collections.emptyList(), Collections.emptyMap(), Option.empty(), WriteOperationType.UPSERT,
        schema.toString(), HoodieTimeline.COMMIT_ACTION);
    FileCreateUtils.createCommit(tempDir.toString(), commitTime, Option.of(commitMetadata));

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
    Utilities.setMapRedWork(conf, mrwork, mapWorkPath);
    jobConf = new JobConf(conf);
    // Add the paths
    FileInputFormat.setInputPaths(jobConf, partitionDir.getPath());
    jobConf.set(HAS_MAP_WORK, "true");
    // The following config tells Hive to choose ExecMapper to read the MAP_WORK
    jobConf.set(MAPRED_MAPPER_CLASS, ExecMapper.class.getName());
    // setting the split size to be 3 to create one split for 3 file groups
    jobConf.set(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.SPLIT_MAXSIZE, "128000000");

    HoodieCombineHiveInputFormat combineHiveInputFormat = new HoodieCombineHiveInputFormat();
    String tripsHiveColumnTypes = "double,string,string,string,double,double,double,double,double";
    InputFormatTestUtil.setPropsForInputFormat(jobConf, schema, tripsHiveColumnTypes);
    // unset META_TABLE_PARTITION_COLUMNS to trigger HUDI-1718
    jobConf.set(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, "");
    InputSplit[] splits = combineHiveInputFormat.getSplits(jobConf, 1);
    // Since the SPLIT_SIZE is 3, we should create only 1 split with all 3 file groups
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
    Configuration conf = new Configuration();
    // initial commit
    Schema schema = HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getEvolvedSchema());
    HoodieTestUtils.init(hadoopConf, tempDir.toAbsolutePath().toString(), HoodieTableType.MERGE_ON_READ);
    String commitTime = "100";
    final int numRecords = 1000;
    // Create 3 parquet files with 1000 records each
    File partitionDir = InputFormatTestUtil.prepareParquetTable(tempDir, schema, 3, numRecords, commitTime);
    HoodieCommitMetadata commitMetadata = CommitUtils.buildMetadata(Collections.emptyList(), Collections.emptyMap(), Option.empty(), WriteOperationType.UPSERT,
        schema.toString(), HoodieTimeline.COMMIT_ACTION);
    FileCreateUtils.createCommit(tempDir.toString(), commitTime, Option.of(commitMetadata));

    String newCommitTime = "101";
    // to trigger the bug of HUDI-1772, only update fileid2
    // insert 1000 update records to log file 2
    // now fileid0, fileid1 has no log files, fileid2 has log file
    HoodieLogFormat.Writer writer =
            InputFormatTestUtil.writeDataBlockToLogFile(partitionDir, fs, schema, "fileid2", commitTime, newCommitTime,
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
    Utilities.setMapRedWork(conf, mrwork, mapWorkPath);
    jobConf = new JobConf(conf);
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
    // Since the SPLIT_SIZE is 3, we should create only 1 split with all 3 file groups
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
  @Disabled
  public void testHoodieRealtimeCombineHoodieInputFormat() throws Exception {

    Configuration conf = new Configuration();
    // initial commit
    Schema schema = HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getEvolvedSchema());
    HoodieTestUtils.init(hadoopConf, tempDir.toAbsolutePath().toString(), HoodieTableType.MERGE_ON_READ);
    String commitTime = "100";
    final int numRecords = 1000;
    // Create 3 parquet files with 1000 records each
    File partitionDir = InputFormatTestUtil.prepareParquetTable(tempDir, schema, 3, numRecords, commitTime);
    HoodieCommitMetadata commitMetadata = CommitUtils.buildMetadata(Collections.emptyList(), Collections.emptyMap(), Option.empty(), WriteOperationType.UPSERT,
        schema.toString(), HoodieTimeline.COMMIT_ACTION);
    FileCreateUtils.createCommit(tempDir.toString(), commitTime, Option.of(commitMetadata));

    // insert 1000 update records to log file 0
    String newCommitTime = "101";
    HoodieLogFormat.Writer writer =
        InputFormatTestUtil.writeDataBlockToLogFile(partitionDir, fs, schema, "fileid0", commitTime, newCommitTime,
            numRecords, numRecords, 0);
    writer.close();
    // insert 1000 update records to log file 1
    writer =
        InputFormatTestUtil.writeDataBlockToLogFile(partitionDir, fs, schema, "fileid1", commitTime, newCommitTime,
            numRecords, numRecords, 0);
    writer.close();
    // insert 1000 update records to log file 2
    writer =
        InputFormatTestUtil.writeDataBlockToLogFile(partitionDir, fs, schema, "fileid2", commitTime, newCommitTime,
            numRecords, numRecords, 0);
    writer.close();

    TableDesc tblDesc = Utilities.defaultTd;
    // Set the input format
    tblDesc.setInputFileFormatClass(HoodieCombineHiveInputFormat.class);
    PartitionDesc partDesc = new PartitionDesc(tblDesc, null);
    LinkedHashMap<Path, PartitionDesc> pt = new LinkedHashMap<>();
    pt.put(new Path(tempDir.toAbsolutePath().toString()), partDesc);
    MapredWork mrwork = new MapredWork();
    mrwork.getMapWork().setPathToPartitionInfo(pt);
    Path mapWorkPath = new Path(tempDir.toAbsolutePath().toString());
    Utilities.setMapRedWork(conf, mrwork, mapWorkPath);
    jobConf = new JobConf(conf);
    // Add the paths
    FileInputFormat.setInputPaths(jobConf, partitionDir.getPath());
    jobConf.set(HAS_MAP_WORK, "true");
    // The following config tells Hive to choose ExecMapper to read the MAP_WORK
    jobConf.set(MAPRED_MAPPER_CLASS, ExecMapper.class.getName());
    // setting the split size to be 3 to create one split for 3 file groups
    jobConf.set(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.SPLIT_MAXSIZE, "3");

    HoodieCombineHiveInputFormat combineHiveInputFormat = new HoodieCombineHiveInputFormat();
    String tripsHiveColumnTypes = "double,string,string,string,double,double,double,double,double";
    InputFormatTestUtil.setPropsForInputFormat(jobConf, schema, tripsHiveColumnTypes);
    InputSplit[] splits = combineHiveInputFormat.getSplits(jobConf, 1);
    // Since the SPLIT_SIZE is 3, we should create only 1 split with all 3 file groups
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
