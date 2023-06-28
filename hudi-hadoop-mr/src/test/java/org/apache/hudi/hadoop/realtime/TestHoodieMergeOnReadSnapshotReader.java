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
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
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
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.config.HoodieRealtimeConfig;
import org.apache.hudi.hadoop.testutils.InputFormatTestUtil;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.common.fs.FSUtils.getFs;
import static org.apache.hudi.common.fs.FSUtils.getRelativePartitionPath;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieMergeOnReadSnapshotReader extends TestHoodieRealtimeRecordReader {

  private static final String PARTITION_COLUMN = "datestr";
  private static final String FILE_ID = "fileid0";
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
    fs = getFs(basePath.toUri().toString(), baseJobConf);
  }

  @TempDir
  public java.nio.file.Path basePath;

  @Test
  public void testSnapshotReader() throws Exception {
    testReaderInternal(false, HoodieLogBlock.HoodieLogBlockType.AVRO_DATA_BLOCK);
  }

  private void testReaderInternal(boolean partitioned, HoodieLogBlock.HoodieLogBlockType logBlockType) throws Exception {
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
    String baseFilePath = partitionDir + "/" + FILE_ID + "_1-0-1_" + baseInstant + ".parquet";
    String partitionPath = partitioned ? getRelativePartitionPath(new Path(basePath.toString()), new Path(partitionDir.getAbsolutePath())) : "default";
    FileSlice fileSlice = new FileSlice(
        new HoodieFileGroupId(partitionPath, FILE_ID),
        baseInstant,
        new HoodieBaseFile(fs.getFileStatus(new Path(baseFilePath))),
        new ArrayList<>());
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
          writer = InputFormatTestUtil.writeRollback(partitionDir, fs, FILE_ID, baseInstant, instantTime,
              String.valueOf(baseInstantTs + logVersion - 1), logVersion);
        } else {
          writer =
              InputFormatTestUtil.writeDataBlockToLogFile(partitionDir, fs, schema, FILE_ID, baseInstant,
                  instantTime, 120, 0, logVersion, logBlockType);
        }
        long size = writer.getCurrentSize();
        writer.close();
        assertTrue(size > 0, "block - size should be > 0");
        FileCreateUtils.createDeltaCommit(basePath.toString(), instantTime, commitMetadata);

        // create a split with baseFile (parquet file written earlier) and new log file(s)
        fileSlice.addLogFile(writer.getLogFile());
        HoodieRealtimeFileSplit split = new HoodieRealtimeFileSplit(
            new FileSplit(new Path(baseFilePath), 0, 1, baseJobConf),
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

        List<HoodieRecord> records = new ArrayList<>();
        while (snapshotReader.hasNext()) {
          records.add(snapshotReader.next());
        }
        assertEquals(100, records.size());
        snapshotReader.close();
      } catch (Exception ioe) {
        throw new HoodieException(ioe.getMessage(), ioe);
      }
    });
  }
}
