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

package org.apache.hudi.sink;

import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.BaseFileUtils;
import org.apache.hudi.common.util.ClosableIterator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.sink.transform.Transformer;
import org.apache.hudi.sink.utils.Pipelines;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.table.format.FormatUtils;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.FlinkClientUtil;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.source.ContinuousFileSource;

import org.apache.avro.Schema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.TestLogger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FilenameFilter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.stream.Collectors.toList;
import static org.apache.hudi.util.StreamerUtil.isValidFile;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration test for BoostrapOperator.
 */
public class TestBoostrapOperator extends TestLogger {
  @TempDir
  File tempFile;

  @Test
  public void testLoadRecords() throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.setInteger(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS, 4);
    conf.setString(FlinkOptions.INDEX_KEY_FIELD, "id");
    conf.setInteger(FlinkOptions.COMPACTION_DELTA_COMMITS, 1);
    conf.setString(FlinkOptions.TABLE_TYPE, HoodieTableType.MERGE_ON_READ.name());

    testWriteToHoodie(conf, Option.empty(), "mor_write_with_compact", 5);

    deleteLastCompactionCommit();

    HoodieWriteConfig writeConfig = StreamerUtil.getHoodieClientConfig(conf, true);
    HoodieFlinkTable hoodieTable = HoodieFlinkTable.create(writeConfig, HoodieFlinkEngineContext.DEFAULT);
    HoodieTimeline commitsTimeline = hoodieTable.getMetaClient().getCommitsTimeline();
    Option<HoodieInstant> latestCommitTime = commitsTimeline.filterCompletedInstants().lastInstant();
    AtomicInteger count = new AtomicInteger();
    BaseFileUtils fileUtils = BaseFileUtils.getInstance(hoodieTable.getBaseFileFormat());
    Schema schema = new TableSchemaResolver(hoodieTable.getMetaClient()).getTableAvroSchema();
    if (latestCommitTime.isPresent()) {
      List<FileSlice> fileSlices = hoodieTable.getSliceView()
          .getLatestFileSlicesBeforeOrOn("par1", latestCommitTime.get().getTimestamp(), true, true)
          .collect(toList());
      for (FileSlice fileSlice : fileSlices) {
        fileSlice.getBaseFile().ifPresent(baseFile -> {
          // filter out crushed files
          if (!isValidFile(baseFile.getFileStatus())) {
            return;
          }
          try (ClosableIterator<HoodieKey> iterator = fileUtils.getHoodieKeyIterator(FlinkClientUtil.getHadoopConf(), new org.apache.hadoop.fs.Path(baseFile.getPath()))) {
            iterator.forEachRemaining(hoodieKey -> {
              count.getAndIncrement();
            });
          }
        });

        // load avro log records
        List<String> logPaths = fileSlice.getLogFiles()
            // filter out crushed files
            .filter(logFile -> isValidFile(logFile.getFileStatus()))
            .map(logFile -> logFile.getPath().toString())
            .collect(toList());
        HoodieMergedLogRecordScanner scanner = FormatUtils.logScanner(logPaths, schema, latestCommitTime.get().getTimestamp(),
            writeConfig, FlinkClientUtil.getHadoopConf());

        try {
          for (String recordKey : scanner.getRecords().keySet()) {
            count.getAndIncrement();
          }
        } catch (Exception e) {
          throw new HoodieException(String.format("Error when loading record keys from files: %s", logPaths), e);
        } finally {
          scanner.close();
        }
      }
    }
    assertEquals(8, count.get());
  }

  private void deleteLastCompactionCommit() {
    File allCommits = new File(tempFile.getPath(), ".hoodie");
    final File[] files = allCommits.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith(".commit");
      }
    });
    if (files.length > 0) {
      files[files.length - 1].delete();
    }
  }

  private void testWriteToHoodie(
      Configuration conf,
      Option<Transformer> transformer,
      String jobName,
      int checkpoints) throws Exception {

    StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    execEnv.getConfig().disableObjectReuse();
    execEnv.setParallelism(4);
    // set up checkpoint interval
    execEnv.enableCheckpointing(4000, CheckpointingMode.EXACTLY_ONCE);
    execEnv.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

    // Read from file source
    RowType rowType =
        (RowType) AvroSchemaConverter.convertToDataType(StreamerUtil.getSourceSchema(conf))
            .getLogicalType();

    JsonRowDataDeserializationSchema deserializationSchema = new JsonRowDataDeserializationSchema(
        rowType,
        InternalTypeInfo.of(rowType),
        false,
        true,
        TimestampFormat.ISO_8601
    );
    String sourcePath = Objects.requireNonNull(Thread.currentThread()
        .getContextClassLoader().getResource("test_source6.data")).toString();

    DataStream<RowData> dataStream;

    dataStream = execEnv
        // use continuous file source to trigger checkpoint
        .addSource(new ContinuousFileSource.BoundedSourceFunction(new Path(sourcePath), checkpoints))
        .name("continuous_file_source")
        .setParallelism(1)
        .map(record -> deserializationSchema.deserialize(record.getBytes(StandardCharsets.UTF_8)))
        .setParallelism(1);

    if (transformer.isPresent()) {
      dataStream = transformer.get().apply(dataStream);
    }

    int parallelism = execEnv.getParallelism();
    DataStream<HoodieRecord> hoodieRecordDataStream = Pipelines.bootstrap(conf, rowType, parallelism, dataStream);
    DataStream<Object> pipeline = Pipelines.hoodieStreamWrite(conf, parallelism, hoodieRecordDataStream);
    execEnv.addOperator(pipeline.getTransformation());

    Pipelines.clean(conf, pipeline);
    Pipelines.compact(conf, pipeline);

    JobClient client = execEnv.executeAsync(jobName);
    // wait for the streaming job to finish
    client.getJobExecutionResult().get();
  }
}
