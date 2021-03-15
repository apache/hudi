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

package org.apache.hudi.sink.transform;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.testutils.RawTripTestPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.testutils.HoodieFlinkClientTestHarness;

import org.apache.avro.Schema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.AVRO_SCHEMA;

public class TestJsonStringToHoodieRecordMapFunction extends HoodieFlinkClientTestHarness {
  @BeforeEach
  public void init() {
    initPath();
    initTestDataGenerator();
    initFileSystem();
    initFlinkMiniCluster();
  }

  @AfterEach
  public void clean() throws Exception {
    cleanupTestDataGenerator();
    cleanupFileSystem();
    cleanupFlinkMiniCluster();
  }

  @Test
  public void testMapFunction() throws Exception {
    final String newCommitTime = "001";
    final int numRecords = 10;
    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, numRecords);
    List<String> recordStr = RawTripTestPayload.recordsToStrings(records);
    Schema schema = AVRO_SCHEMA;

    TypedProperties props = new TypedProperties();
    props.put(HoodieWriteConfig.WRITE_PAYLOAD_CLASS, OverwriteWithLatestAvroPayload.class.getName());
    props.put(HoodieWriteConfig.PRECOMBINE_FIELD_PROP, "timestamp");
    props.put(KeyGeneratorOptions.RECORDKEY_FIELD_OPT_KEY, "_row_key");
    props.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_OPT_KEY, "current_date");
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    SimpleTestSinkFunction.valuesList.clear();
    env.fromCollection(recordStr)
        .map(new JsonStringToHoodieRecordMapFunction(props, Option.of(schema.toString())))
        .addSink(new SimpleTestSinkFunction());
    env.execute();

    // input records all present in the sink
    Assertions.assertEquals(10, SimpleTestSinkFunction.valuesList.size());

    // input keys all present in the sink
    Set<String> inputKeySet = records.stream().map(r -> r.getKey().getRecordKey()).collect(Collectors.toSet());
    Assertions.assertEquals(10, SimpleTestSinkFunction.valuesList.stream()
        .map(r -> inputKeySet.contains(r.getRecordKey())).filter(b -> b).count());
  }
}
