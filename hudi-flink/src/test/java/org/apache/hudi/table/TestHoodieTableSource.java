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

package org.apache.hudi.table;

import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.table.format.mor.MergeOnReadInputFormat;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test cases for HoodieTableSource.
 */
public class TestHoodieTableSource {
  private static final Logger LOG = LoggerFactory.getLogger(TestHoodieTableSource.class);

  private Configuration conf;

  @TempDir
  File tempFile;

  @BeforeEach
  void beforeEach() throws IOException {
    final String path = tempFile.getAbsolutePath();
    conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    StreamerUtil.initTableIfNotExists(conf);
    IntStream.range(1, 5)
        .forEach(i -> new File(path + File.separator + "par" + i).mkdirs());
  }

  @Test
  void testGetReadPaths() {
    HoodieTableSource tableSource = new HoodieTableSource(
        TestConfigurations.TABLE_SCHEMA,
        new Path(tempFile.getPath()),
        Arrays.asList(conf.getString(FlinkOptions.PARTITION_PATH_FIELD).split(",")),
        "default-par",
        conf);
    Path[] paths = tableSource.getReadPaths();
    assertNotNull(paths);
    String[] names = Arrays.stream(paths).map(Path::getName)
        .sorted(Comparator.naturalOrder()).toArray(String[]::new);
    assertThat(Arrays.toString(names), is("[par1, par2, par3, par4]"));
    // apply partition pruning
    Map<String, String> partitions = new HashMap<>();
    partitions.put("partition", "par1");

    tableSource = (HoodieTableSource) tableSource
        .applyPartitionPruning(Collections.singletonList(partitions));

    Path[] paths2 = tableSource.getReadPaths();
    assertNotNull(paths2);
    String[] names2 = Arrays.stream(paths2).map(Path::getName)
        .sorted(Comparator.naturalOrder()).toArray(String[]::new);
    assertThat(Arrays.toString(names2), is("[par1]"));
  }

  @Test
  void testGetInputFormat() throws Exception {
    // write some data to let the TableSchemaResolver get the right instant
    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    HoodieTableSource tableSource = new HoodieTableSource(
        TestConfigurations.TABLE_SCHEMA,
        new Path(tempFile.getPath()),
        Arrays.asList(conf.getString(FlinkOptions.PARTITION_PATH_FIELD).split(",")),
        "default-par",
        conf);
    InputFormat<RowData, ?> inputFormat = tableSource.getInputFormat();
    assertThat(inputFormat, is(instanceOf(FileInputFormat.class)));
    conf.setString(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_MERGE_ON_READ);
    inputFormat = tableSource.getInputFormat();
    assertThat(inputFormat, is(instanceOf(MergeOnReadInputFormat.class)));
    conf.setString(FlinkOptions.QUERY_TYPE.key(), FlinkOptions.QUERY_TYPE_INCREMENTAL);
    assertThrows(HoodieException.class,
        () -> tableSource.getInputFormat(),
        "Invalid query type : 'incremental'. Only 'snapshot' is supported now");
  }
}
