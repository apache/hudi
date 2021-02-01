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

package org.apache.hudi.operator;

import org.apache.hudi.operator.utils.TestConfigurations;
import org.apache.hudi.operator.utils.TestData;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.TestLogger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Integration test for Flink Hoodie stream sink.
 */
public class StreamWriteITCase extends TestLogger {

  private static final Map<String, String> EXPECTED = new HashMap<>();

  static {
    EXPECTED.put("par1", "[id1,par1,id1,Danny,23,1000,par1, id2,par1,id2,Stephen,33,2000,par1]");
    EXPECTED.put("par2", "[id3,par2,id3,Julian,53,3000,par2, id4,par2,id4,Fabian,31,4000,par2]");
    EXPECTED.put("par3", "[id5,par3,id5,Sophia,18,5000,par3, id6,par3,id6,Emma,20,6000,par3]");
    EXPECTED.put("par4", "[id7,par4,id7,Bob,44,7000,par4, id8,par4,id8,Han,56,8000,par4]");
  }

  @TempDir
  File tempFile;

  @Test
  public void testWriteToHoodie() throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    execEnv.getConfig().disableObjectReuse();
    execEnv.setParallelism(4);
    // 1 second a time
    execEnv.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

    // Read from kafka source
    RowType rowType =
        (RowType) AvroSchemaConverter.convertToDataType(StreamerUtil.getSourceSchema(conf))
            .getLogicalType();
    StreamWriteOperatorFactory<RowData> operatorFactory =
        new StreamWriteOperatorFactory<>(rowType, conf, 4);

    int partitionFieldIndex = rowType.getFieldIndex(conf.getString(FlinkOptions.PARTITION_PATH_FIELD));
    final RowData.FieldGetter partitionFieldGetter =
        RowData.createFieldGetter(rowType.getTypeAt(partitionFieldIndex), partitionFieldIndex);

    JsonRowDataDeserializationSchema deserializationSchema = new JsonRowDataDeserializationSchema(
        rowType,
        new RowDataTypeInfo(rowType),
        false,
        true,
        TimestampFormat.ISO_8601
    );
    String sourcePath = Objects.requireNonNull(Thread.currentThread()
        .getContextClassLoader().getResource("test_source.data")).toString();

    TextInputFormat format = new TextInputFormat(new Path(sourcePath));
    format.setFilesFilter(FilePathFilter.createDefaultFilter());
    TypeInformation<String> typeInfo = BasicTypeInfo.STRING_TYPE_INFO;
    format.setCharsetName("UTF-8");

    DataStream<Object> dataStream = execEnv
        // use PROCESS_CONTINUOUSLY mode to trigger checkpoint
        .readFile(format, sourcePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 1000, typeInfo)
        .map(record -> deserializationSchema.deserialize(record.getBytes(StandardCharsets.UTF_8)))
        // Key-by partition path, to avoid multiple subtasks write to a partition at the same time
        .keyBy(partitionFieldGetter::getFieldOrNull)
        .transform("hoodie_stream_write", null, operatorFactory)
        .uid("uid_hoodie_stream_write")
        .setParallelism(4);
    execEnv.addOperator(dataStream.getTransformation());

    JobClient client = execEnv.executeAsync(execEnv.getStreamGraph(conf.getString(FlinkOptions.TABLE_NAME)));
    if (client.getJobStatus().get() != JobStatus.FAILED) {
      try {
        TimeUnit.SECONDS.sleep(10);
        client.cancel();
      } catch (Throwable var1) {
        // ignored
      }
    }

    TestData.checkWrittenData(tempFile, EXPECTED);
  }
}
