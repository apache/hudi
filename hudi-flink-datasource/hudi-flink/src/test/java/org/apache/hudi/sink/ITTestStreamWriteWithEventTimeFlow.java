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

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.DateTimeUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.util.HoodiePipeline;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.source.ContinuousFileSource;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.TestLogger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class ITTestStreamWriteWithEventTimeFlow extends TestLogger {

  @TempDir
  File tempFile;

  public static final DataType ROW_DATA_TYPE = DataTypes.ROW(
      DataTypes.FIELD("id", DataTypes.VARCHAR(20)),// record key
      DataTypes.FIELD("data", DataTypes.VARCHAR(10)),
      DataTypes.FIELD("ts", DataTypes.TIMESTAMP(3)), // precombine field
      DataTypes.FIELD("partition", DataTypes.VARCHAR(10)))
      .notNull();

  private static Stream<Arguments> parameters() {
    return Stream.of(
      Arguments.of("COPY_ON_WRITE", "FLINK_STATE", 1, false),
      Arguments.of("COPY_ON_WRITE", "FLINK_STATE", 2, true),
      Arguments.of("MERGE_ON_READ", "FLINK_STATE", 1, false),
      Arguments.of("MERGE_ON_READ", "FLINK_STATE", 2, true),
      Arguments.of("COPY_ON_WRITE", "BUCKET", 1, false),
      Arguments.of("COPY_ON_WRITE", "BUCKET", 2, true),
      Arguments.of("MERGE_ON_READ", "BUCKET", 1, false),
      Arguments.of("MERGE_ON_READ", "BUCKET", 2, true));
  }

  @ParameterizedTest
  @MethodSource("parameters")
  void testWriteWithEventTime(String tableType, String indexType, int parallelism, boolean partitioned) throws Exception {
    Configuration conf = getConf(tableType, indexType, parallelism);

    List<Row> rows =
        Lists.newArrayList(
          Row.of("1", "hello", LocalDateTime.parse("2012-12-12T12:12:12")),
          Row.of("2", "world", LocalDateTime.parse("2012-12-12T12:12:01")),
          Row.of("3", "world", LocalDateTime.parse("2012-12-12T12:12:02")),
          Row.of("4", "foo", LocalDateTime.parse("2012-12-12T12:12:10")));

    // write rowData with eventTime
    testWriteToHoodie(conf, parallelism, partitioned, ((RowType) ROW_DATA_TYPE.getLogicalType()), rows);

    // check eventTime
    checkWriteEventTime(indexType, parallelism, conf);
  }

  public Configuration getConf(String tableType, String indexType, int parallelism) {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath(), ROW_DATA_TYPE);
    conf.setString(FlinkOptions.TABLE_TYPE, tableType);
    conf.setString(FlinkOptions.INDEX_TYPE, indexType);
    conf.setString(FlinkOptions.RECORD_KEY_FIELD, "id");
    conf.setString(FlinkOptions.INDEX_KEY_FIELD, "id");
    conf.setString(FlinkOptions.EVENT_TIME_FIELD, "ts");
    conf.setInteger(FlinkOptions.WRITE_TASKS, parallelism);
    conf.setString("hoodie.metrics.on", "false");
    conf.setInteger(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS, parallelism);
    return conf;
  }

  private void checkWriteEventTime(String indexType, int parallelism, Configuration conf) throws IOException {
    if (parallelism <= 1) { // single mode
      Assertions.assertEquals(DateTimeUtils.timeStampToMillis(LocalDateTime.parse("2012-12-12T12:12:12")),
          getLatestEventTime(conf));
    } else if (indexType.equals("BUCKET") || indexType.equals("NON_INDEX")) { // hash mode
      Assertions.assertEquals(DateTimeUtils.timeStampToMillis(LocalDateTime.parse("2012-12-12T12:12:10")),
          getLatestEventTime(conf));
    } else { // partition mode
      Assertions.assertEquals(DateTimeUtils.timeStampToMillis(LocalDateTime.parse("2012-12-12T12:12:02")),
          getLatestEventTime(conf));
    }
  }

  public long getLatestEventTime(Configuration conf) throws IOException {
    String path = conf.getString(FlinkOptions.PATH);
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(path);
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    HoodieTimeline timeline = activeTimeline.getCommitsTimeline().filterCompletedInstants();
    HoodieInstant hoodieInstant = timeline.getReverseOrderedInstants()
        .findFirst().orElse(null);
    return Objects.isNull(hoodieInstant) ? -1L :
      Long.parseLong(TimelineUtils.getMetadataValue(metaClient, FlinkOptions.EVENT_TIME_FIELD.key(),
      hoodieInstant).orElse("-1"));
  }

  public List<String> getRowDataString(List<Row> rows, boolean partitioned) {
    List<String> dataBuffer = new ArrayList<>();
    for (Row row : rows) {
      String id = (String) row.getField(0);
      String data = (String) row.getField(1);
      LocalDateTime ts = (LocalDateTime) row.getField(2);
      String rowData = String.format("{\"id\": \"%s\", \"data\": \"%s\", \"ts\": \"%s\", \"partition\": \"%s\"}",
          id, data, ts.toString(), partitioned ? data : "par");
      dataBuffer.add(rowData);
    }
    return dataBuffer;
  }

  private void testWriteToHoodie(
      Configuration conf,
      int parallelism,
      boolean partitioned,
      RowType rowType,
      List<Row> rows) throws Exception {

    StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    execEnv.getConfig().disableObjectReuse();
    execEnv.setParallelism(parallelism);
    execEnv.setMaxParallelism(parallelism);
    // set up checkpoint interval
    execEnv.enableCheckpointing(4000, CheckpointingMode.EXACTLY_ONCE);
    execEnv.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

    JsonRowDataDeserializationSchema deserializationSchema = new JsonRowDataDeserializationSchema(
        rowType,
        InternalTypeInfo.of(rowType),
        false,
        true,
        TimestampFormat.ISO_8601
    );

    boolean isMor = conf.getString(FlinkOptions.TABLE_TYPE).equals(HoodieTableType.MERGE_ON_READ.name());

    List<String> dataBuffer = getRowDataString(rows, partitioned);

    DataStream<RowData> dataStream;
    dataStream = execEnv
      // use continuous file source to trigger checkpoint
      .addSource(new ContinuousFileSource.BoundedSourceFunction(null, dataBuffer, 1))
      .name("continuous_file_source")
      .setParallelism(1)
      .map(record -> deserializationSchema.deserialize(record.getBytes(StandardCharsets.UTF_8)))
      .setParallelism(1);


    //sink to hoodie table use low-level sink api.
    HoodiePipeline.Builder builder = HoodiePipeline.builder("t_event_sink")
        .column("id string not null")
        .column("data string")
        .column("`ts` timestamp(3)")
        .column("`partition` string")
        .pk("id")
        .partition("partition")
        .options(conf.toMap());

    builder.sink(dataStream, false);
    execute(execEnv, isMor, "EventTime_Sink_Test");
  }

  public void execute(StreamExecutionEnvironment execEnv, boolean isMor, String jobName) throws Exception {
    if (isMor) {
      JobClient client = execEnv.executeAsync(jobName);
      if (client.getJobStatus().get() != JobStatus.FAILED) {
        try {
          TimeUnit.SECONDS.sleep(20); // wait long enough for the compaction to finish
          client.cancel();
        } catch (Throwable var1) {
          // ignored
        }
      }
    } else {
      // wait for the streaming job to finish
      execEnv.execute(jobName);
    }
  }
}
