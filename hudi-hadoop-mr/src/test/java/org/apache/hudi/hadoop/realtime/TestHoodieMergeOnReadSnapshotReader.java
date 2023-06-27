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

package org.apache.hudi.hadoop.realtime;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.FileCreateUtils;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.config.HoodieRealtimeConfig;
import org.apache.hudi.hadoop.testutils.InputFormatTestUtil;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieMergeOnReadSnapshotReader extends TestHoodieRealtimeRecordReader {

  private static final String PARTITION_COLUMN = "datestr";
  private JobConf baseJobConf;
  private FileSystem fs;
  private Configuration hadoopConf;

  @BeforeEach
  public void setUp() {
    hadoopConf = HoodieTestUtils.getDefaultHadoopConf();
    hadoopConf.set("fs.defaultFS", "file:///");
    hadoopConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    baseJobConf = new JobConf(hadoopConf);
    baseJobConf.set(HoodieRealtimeConfig.MAX_DFS_STREAM_BUFFER_SIZE_PROP, String.valueOf(1024 * 1024));
    fs = FSUtils.getFs(basePath.toUri().toString(), baseJobConf);
  }

  @TempDir
  public java.nio.file.Path basePath;

  @Test
  public void testSnapshotReader() throws Exception {
    testReaderInternal(ExternalSpillableMap.DiskMapType.BITCASK, false, false,
        HoodieLogBlock.HoodieLogBlockType.AVRO_DATA_BLOCK);
  }

  private void setHiveColumnNameProps(List<Schema.Field> fields, JobConf jobConf, boolean isPartitioned) {
    String names = fields.stream().map(Schema.Field::name).collect(Collectors.joining(","));
    String positions = fields.stream().map(f -> String.valueOf(f.pos())).collect(Collectors.joining(","));
    jobConf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, names);
    jobConf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, positions);

    String hiveOrderedColumnNames = fields.stream()
        .map(Schema.Field::name)
        .filter(name -> !name.equalsIgnoreCase(PARTITION_COLUMN))
        .collect(Collectors.joining(","));
    if (isPartitioned) {
      hiveOrderedColumnNames += "," + PARTITION_COLUMN;
      jobConf.set(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, PARTITION_COLUMN);
    }
    jobConf.set(hive_metastoreConstants.META_TABLE_COLUMNS, hiveOrderedColumnNames);
  }

  private static File getLogTempFile(long startTime, long endTime, String diskType) {
    return Arrays.stream(Objects.requireNonNull(new File("/tmp").listFiles()))
        .filter(f -> f.isDirectory() && f.getName().startsWith("hudi-" + diskType) && f.lastModified() > startTime && f.lastModified() < endTime)
        .findFirst()
        .orElse(new File(""));
  }

  private void testReaderInternal(ExternalSpillableMap.DiskMapType diskMapType,
                                  boolean isCompressionEnabled,
                                  boolean partitioned, HoodieLogBlock.HoodieLogBlockType logBlockType) throws Exception {
    // initial commit
    Schema schema = HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getEvolvedSchema());
    HoodieTestUtils.init(hadoopConf, basePath.toString(), HoodieTableType.MERGE_ON_READ);
    String baseInstant = "100";
    File partitionDir = partitioned ? InputFormatTestUtil.prepareParquetTable(basePath, schema, 1, 100, baseInstant,
        HoodieTableType.MERGE_ON_READ)
        : InputFormatTestUtil.prepareNonPartitionedParquetTable(basePath, schema, 1, 100, baseInstant,
        HoodieTableType.MERGE_ON_READ);

    HoodieCommitMetadata commitMetadata = CommitUtils.buildMetadata(Collections.emptyList(), Collections.emptyMap(), Option.empty(), WriteOperationType.UPSERT,
        schema.toString(), HoodieTimeline.DELTA_COMMIT_ACTION);
    FileCreateUtils.createDeltaCommit(basePath.toString(), baseInstant, commitMetadata);
    // Add the paths
    FileInputFormat.setInputPaths(baseJobConf, partitionDir.getPath());

    List<Pair<String, Integer>> logVersionsWithAction = new ArrayList<>();
    logVersionsWithAction.add(Pair.of(HoodieTimeline.DELTA_COMMIT_ACTION, 1));
    logVersionsWithAction.add(Pair.of(HoodieTimeline.DELTA_COMMIT_ACTION, 2));
    FileSlice fileSlice =
        new FileSlice(partitioned ? FSUtils.getRelativePartitionPath(new Path(basePath.toString()),
            new Path(partitionDir.getAbsolutePath())) : "default", baseInstant, "fileid0");
    logVersionsWithAction.forEach(logVersionWithAction -> {
      try {
        // update files or generate new log file
        int logVersion = logVersionWithAction.getRight();
        String action = logVersionWithAction.getKey();
        int baseInstantTs = Integer.parseInt(baseInstant);
        String instantTime = String.valueOf(baseInstantTs + logVersion);
        String latestInstant =
            action.equals(HoodieTimeline.ROLLBACK_ACTION) ? String.valueOf(baseInstantTs + logVersion - 2)
                : instantTime;

        HoodieLogFormat.Writer writer;
        if (action.equals(HoodieTimeline.ROLLBACK_ACTION)) {
          writer = InputFormatTestUtil.writeRollback(partitionDir, fs, "fileid0", baseInstant, instantTime,
              String.valueOf(baseInstantTs + logVersion - 1), logVersion);
        } else {
          writer =
              InputFormatTestUtil.writeDataBlockToLogFile(partitionDir, fs, schema, "fileid0", baseInstant,
                  instantTime, 120, 0, logVersion, logBlockType);
        }
        long size = writer.getCurrentSize();
        writer.close();
        assertTrue(size > 0, "block - size should be > 0");
        FileCreateUtils.createDeltaCommit(basePath.toString(), instantTime, commitMetadata);

        // create a split with baseFile (parquet file written earlier) and new log file(s)
        fileSlice.addLogFile(writer.getLogFile());
        HoodieRealtimeFileSplit split = new HoodieRealtimeFileSplit(
            new FileSplit(new Path(partitionDir + "/fileid0_1-0-1_" + baseInstant + ".parquet"), 0, 1, baseJobConf),
            basePath.toUri().toString(), fileSlice.getLogFiles().sorted(HoodieLogFile.getLogFileComparator())
            .collect(Collectors.toList()),
            instantTime,
            false,
            Option.empty());

        HoodieMergeOnReadSnapshotReader snapshotReader = new HoodieMergeOnReadSnapshotReader(
            basePath.toString(),
            fileSlice.getBaseFile().get().getPath(),
            fileSlice.getLogFiles().collect(Collectors.toList()),
            instantTime,
            schema,
            baseJobConf,
            0,
            1,
            new String[0]);

        // create a RecordReader to be used by HoodieRealtimeRecordReader
        RecordReader<NullWritable, ArrayWritable> reader = new MapredParquetInputFormat().getRecordReader(
            new FileSplit(split.getPath(), 0, fs.getLength(split.getPath()), (String[]) null), baseJobConf, null);
        JobConf jobConf = new JobConf(baseJobConf);
        List<Schema.Field> fields = schema.getFields();
        setHiveColumnNameProps(fields, jobConf, partitioned);

        jobConf.setEnum(HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE.key(), diskMapType);
        jobConf.setBoolean(HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED.key(), isCompressionEnabled);

        // validate record reader compaction
        long logTmpFileStartTime = System.currentTimeMillis();
        HoodieRealtimeRecordReader recordReader = new HoodieRealtimeRecordReader(split, jobConf, reader);

        // use reader to read base Parquet File and log file, merge in flight and return latest commit
        // here all 100 records should be updated, see above
        // another 20 new insert records should also output with new commit time.
        NullWritable key = recordReader.createKey();
        ArrayWritable value = recordReader.createValue();
        int recordCnt = 0;
        while (recordReader.next(key, value)) {
          Writable[] values = value.get();
          // check if the record written is with latest commit, here "101"
          assertEquals(latestInstant, values[0].toString());
          key = recordReader.createKey();
          value = recordReader.createValue();
          recordCnt++;
        }
        recordReader.getPos();
        assertEquals(1.0, recordReader.getProgress(), 0.05);
        assertEquals(120, recordCnt);
        recordReader.close();
        // the temp file produced by logScanner should be deleted
        assertFalse(getLogTempFile(logTmpFileStartTime, System.currentTimeMillis(), diskMapType.toString()).exists());
      } catch (Exception ioe) {
        throw new HoodieException(ioe.getMessage(), ioe);
      }
    });
  }
}
