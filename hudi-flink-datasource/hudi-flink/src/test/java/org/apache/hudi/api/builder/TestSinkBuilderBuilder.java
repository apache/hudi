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

package org.apache.hudi.api.builder;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class TestSinkBuilderBuilder extends SourceSinkBuilderTestBase {


    @Test @Ignore
    public void testBuildHoodieDataStreamSink() throws Exception {

        DataStream<Row> input = dataStreamGen();
        Map<String, String> confMap = new HashMap<>();
        confMap.put("connector" , "hudi");
        confMap.put("table.type", "MERGE_ON_READ");
        confMap.put("path" , "hdfs://127.0.0.1:9000/hudi/hudi_db/mor_test9");

        SinkBuilder.builder()
                         .input(input)
                         .options(confMap)
                         .partitions(Arrays.asList("dt", "hr"))
                         .schema(CREATE_TABLE_SCHEMA)
                         .sink();
        execute();
    }
}
