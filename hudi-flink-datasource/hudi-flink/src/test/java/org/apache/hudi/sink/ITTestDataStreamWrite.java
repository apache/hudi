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

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.transform.ChainedTransformer;
import org.apache.hudi.sink.transform.Transformer;
import org.apache.hudi.sink.utils.Pipelines;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;
import org.apache.hudi.utils.source.ContinuousFileSource;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.TestLogger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Integration test for Flink Hoodie stream sink.
 */
public class ITTestDataStreamWrite extends TestLogger {

  private static final Map<String, List<String>> EXPECTED = new HashMap<>();
  private static final Map<String, List<String>> EXPECTED_TRANSFORMER = new HashMap<>();
  private static final Map<String, List<String>> EXPECTED_CHAINED_TRANSFORMER = new HashMap<>();

  static {
    EXPECTED.put("par1", Arrays.asList("id1,par1,id1,Danny,23,1000,par1", "id2,par1,id2,Stephen,33,2000,par1"));
    EXPECTED.put("par2", Arrays.asList("id3,par2,id3,Julian,53,3000,par2", "id4,par2,id4,Fabian,31,4000,par2"));
    EXPECTED.put("par3", Arrays.asList("id5,par3,id5,Sophia,18,5000,par3", "id6,par3,id6,Emma,20,6000,par3"));
    EXPECTED.put("par4", Arrays.asList("id7,par4,id7,Bob,44,7000,par4", "id8,par4,id8,Han,56,8000,par4"));

    EXPECTED_TRANSFORMER.put("par1", Arrays.asList("id1,par1,id1,Danny,24,1000,par1", "id2,par1,id2,Stephen,34,2000,par1"));
    EXPECTED_TRANSFORMER.put("par2", Arrays.asList("id3,par2,id3,Julian,54,3000,par2", "id4,par2,id4,Fabian,32,4000,par2"));
    EXPECTED_TRANSFORMER.put("par3", Arrays.asList("id5,par3,id5,Sophia,19,5000,par3", "id6,par3,id6,Emma,21,6000,par3"));
    EXPECTED_TRANSFORMER.put("par4", Arrays.asList("id7,par4,id7,Bob,45,7000,par4", "id8,par4,id8,Han,57,8000,par4"));

    EXPECTED_CHAINED_TRANSFORMER.put("par1", Arrays.asList("id1,par1,id1,Danny,25,1000,par1", "id2,par1,id2,Stephen,35,2000,par1"));
    EXPECTED_CHAINED_TRANSFORMER.put("par2", Arrays.asList("id3,par2,id3,Julian,55,3000,par2", "id4,par2,id4,Fabian,33,4000,par2"));
    EXPECTED_CHAINED_TRANSFORMER.put("par3", Arrays.asList("id5,par3,id5,Sophia,20,5000,par3", "id6,par3,id6,Emma,22,6000,par3"));
    EXPECTED_CHAINED_TRANSFORMER.put("par4", Arrays.asList("id7,par4,id7,Bob,46,7000,par4", "id8,par4,id8,Han,58,8000,par4"));
  }

  @TempDir
  File tempFile;

  @ParameterizedTest
  @ValueSource(strings = {"BUCKET", "FLINK_STATE"})
  public void testWriteCopyOnWrite(String indexType) throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.setString(FlinkOptions.INDEX_TYPE, indexType);
    conf.setInteger(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS, 1);
    conf.setString(FlinkOptions.INDEX_KEY_FIELD, "id");
    conf.setBoolean(FlinkOptions.PRE_COMBINE,true);

    testWriteToHoodie(conf, "cow_write", 1, EXPECTED);
  }

  @Test
  public void testWriteCopyOnWriteWithTransformer() throws Exception {
    Transformer transformer = (ds) -> ds.map((rowdata) -> {
      if (rowdata instanceof GenericRowData) {
        GenericRowData genericRD = (GenericRowData) rowdata;
        //update age field to age + 1
        genericRD.setField(2, genericRD.getInt(2) + 1);
        return genericRD;
      } else {
        throw new RuntimeException("Unrecognized row type information: " + rowdata.getClass().getSimpleName());
      }
    });

    testWriteToHoodie(transformer, "cow_write_with_transformer", EXPECTED_TRANSFORMER);
  }

  @Test
  public void testWriteCopyOnWriteWithChainedTransformer() throws Exception {
    Transformer t1 = (ds) -> ds.map(rowData -> {
      if (rowData instanceof GenericRowData) {
        GenericRowData genericRD = (GenericRowData) rowData;
        //update age field to age + 1
        genericRD.setField(2, genericRD.getInt(2) + 1);
        return genericRD;
      } else {
        throw new RuntimeException("Unrecognized row type : " + rowData.getClass().getSimpleName());
      }
    });

    ChainedTransformer chainedTransformer = new ChainedTransformer(Arrays.asList(t1, t1));

    testWriteToHoodie(chainedTransformer, "cow_write_with_chained_transformer", EXPECTED_CHAINED_TRANSFORMER);
  }

  @ParameterizedTest
  @ValueSource(strings = {"BUCKET", "FLINK_STATE"})
  public void testWriteMergeOnReadWithCompaction(String indexType) throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.setString(FlinkOptions.INDEX_TYPE, indexType);
    conf.setInteger(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS, 4);
    conf.setString(FlinkOptions.INDEX_KEY_FIELD, "id");
    conf.setInteger(FlinkOptions.COMPACTION_DELTA_COMMITS, 1);
    conf.setString(FlinkOptions.TABLE_TYPE, HoodieTableType.MERGE_ON_READ.name());

    testWriteToHoodie(conf, "mor_write_with_compact", 1, EXPECTED);
  }

  private void testWriteToHoodie(
      Transformer transformer,
      String jobName,
      Map<String, List<String>> expected) throws Exception {
    testWriteToHoodie(TestConfigurations.getDefaultConf(tempFile.getAbsolutePath()),
        Option.of(transformer), jobName, 2, expected);
  }

  private void testWriteToHoodie(
      Configuration conf,
      String jobName,
      int checkpoints,
      Map<String, List<String>> expected) throws Exception {
    testWriteToHoodie(conf, Option.empty(), jobName, checkpoints, expected);
  }

  private void testWriteToHoodie(
      Configuration conf,
      Option<Transformer> transformer,
      String jobName,
      int checkpoints,
      Map<String, List<String>> expected) throws Exception {

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
        .getContextClassLoader().getResource("test_source.data")).toString();

    boolean isMor = conf.getString(FlinkOptions.TABLE_TYPE).equals(HoodieTableType.MERGE_ON_READ.name());

    DataStream<RowData> dataStream;
    if (isMor) {
      TextInputFormat format = new TextInputFormat(new Path(sourcePath));
      format.setFilesFilter(FilePathFilter.createDefaultFilter());
      TypeInformation<String> typeInfo = BasicTypeInfo.STRING_TYPE_INFO;
      format.setCharsetName("UTF-8");

      dataStream = execEnv
          // use PROCESS_CONTINUOUSLY mode to trigger checkpoint
          .readFile(format, sourcePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 1000, typeInfo)
          .map(record -> deserializationSchema.deserialize(record.getBytes(StandardCharsets.UTF_8)))
          .setParallelism(1);
    } else {
      dataStream = execEnv
          // use continuous file source to trigger checkpoint
          .addSource(new ContinuousFileSource.BoundedSourceFunction(new Path(sourcePath), checkpoints))
          .name("continuous_file_source")
          .setParallelism(1)
          .map(record -> deserializationSchema.deserialize(record.getBytes(StandardCharsets.UTF_8)))
          .setParallelism(4);
    }

    if (transformer.isPresent()) {
      dataStream = transformer.get().apply(dataStream);
    }
    ObjectIdentifier identifier =  ObjectIdentifier.of("default_catalog", "default_database", "t1");
    int parallelism = execEnv.getParallelism();
    DataStream<HoodieRecord> hoodieRecordDataStream = Pipelines.bootstrap(conf, rowType, parallelism, dataStream, identifier);
    DataStream<Object> pipeline = Pipelines.hoodieStreamWrite(conf, parallelism, hoodieRecordDataStream, identifier);
    execEnv.addOperator(pipeline.getTransformation());

    if (isMor) {
      Pipelines.clean(conf, pipeline, identifier);
      Pipelines.compact(conf, pipeline, identifier);
    }
    JobClient client = execEnv.executeAsync(jobName);
    if (isMor) {
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
      client.getJobExecutionResult().get();
    }

    TestData.checkWrittenFullData(tempFile, expected);

  }
}
