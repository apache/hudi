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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.testutils.minicluster.MiniClusterUtil;
import org.apache.hudi.hadoop.hive.HoodieCombineHiveInputFormat;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;

import static org.apache.hadoop.hive.ql.exec.Utilities.HAS_MAP_WORK;
import static org.apache.hadoop.hive.ql.exec.Utilities.MAPRED_MAPPER_CLASS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    InputFormatTestUtil.commit(tempDir, commitTime);

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
