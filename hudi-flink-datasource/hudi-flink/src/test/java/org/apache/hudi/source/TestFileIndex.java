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

package org.apache.hudi.source;

import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.keygen.NonpartitionedAvroKeyGenerator;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;

import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link FileIndex}.
 */
public class TestFileIndex {
  @TempDir
  File tempFile;

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testFileListingUsingMetadata(boolean hiveStylePartitioning) throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.setBoolean(FlinkOptions.METADATA_ENABLED, true);
    conf.setBoolean(FlinkOptions.HIVE_STYLE_PARTITIONING, hiveStylePartitioning);
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    FileIndex fileIndex = FileIndex.instance(new Path(tempFile.getAbsolutePath()), conf, TestConfigurations.ROW_TYPE);
    List<String> partitionKeys = Collections.singletonList("partition");
    List<Map<String, String>> partitions = fileIndex.getPartitions(partitionKeys, "default", hiveStylePartitioning);
    assertTrue(partitions.stream().allMatch(m -> m.size() == 1));
    String partitionPaths = partitions.stream()
        .map(Map::values).flatMap(Collection::stream).sorted().collect(Collectors.joining(","));
    assertThat("should have 4 partitions", partitionPaths, is("par1,par2,par3,par4"));

    FileStatus[] fileStatuses = fileIndex.getFilesInPartitions();
    assertThat(fileStatuses.length, is(4));
    assertTrue(Arrays.stream(fileStatuses)
        .allMatch(fileStatus -> fileStatus.getPath().toString().endsWith(HoodieFileFormat.PARQUET.getFileExtension())));
  }

  @Test
  void testFileListingUsingMetadataNonPartitionedTable() throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.setString(FlinkOptions.PARTITION_PATH_FIELD, "");
    conf.setString(FlinkOptions.KEYGEN_CLASS_NAME, NonpartitionedAvroKeyGenerator.class.getName());
    conf.setBoolean(FlinkOptions.METADATA_ENABLED, true);
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    FileIndex fileIndex = FileIndex.instance(new Path(tempFile.getAbsolutePath()), conf, TestConfigurations.ROW_TYPE);
    List<String> partitionKeys = Collections.singletonList("");
    List<Map<String, String>> partitions = fileIndex.getPartitions(partitionKeys, "default", false);
    assertThat(partitions.size(), is(0));

    FileStatus[] fileStatuses = fileIndex.getFilesInPartitions();
    assertThat(fileStatuses.length, is(1));
    assertTrue(fileStatuses[0].getPath().toString().endsWith(HoodieFileFormat.PARQUET.getFileExtension()));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testFileListingEmptyTable(boolean enableMetadata) {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.setBoolean(FlinkOptions.METADATA_ENABLED, enableMetadata);
    FileIndex fileIndex = FileIndex.instance(new Path(tempFile.getAbsolutePath()), conf, TestConfigurations.ROW_TYPE);
    List<String> partitionKeys = Collections.singletonList("partition");
    List<Map<String, String>> partitions = fileIndex.getPartitions(partitionKeys, "default", false);
    assertThat(partitions.size(), is(0));

    FileStatus[] fileStatuses = fileIndex.getFilesInPartitions();
    assertThat(fileStatuses.length, is(0));
  }
}
