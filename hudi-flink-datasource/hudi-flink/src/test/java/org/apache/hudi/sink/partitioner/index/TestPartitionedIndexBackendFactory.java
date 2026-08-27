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

package org.apache.hudi.sink.partitioner.index;

import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * Test cases for {@link PartitionedIndexBackendFactory}.
 */
public class TestPartitionedIndexBackendFactory {

  private Configuration conf;

  @TempDir
  File tempFile;

  @BeforeEach
  public void before() throws Exception {
    conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.setString("hadoop.fs.defaultFS", "file:///");
    StreamerUtil.initTableIfNotExists(conf);
  }

  @Test
  void testInsertOverwriteReturnsDummyBackend() throws Exception {
    try (PartitionedIndexBackend backend = PartitionedIndexBackendFactory.create(
        conf, true, (partitionPath, recordKey, fileId) -> true)) {
      assertInstanceOf(DummyPartitionedIndexBackend.class, backend);
    }
  }

  @Test
  void testDefaultBackendTypeReturnsRecordLevelIndexBackend() throws Exception {
    try (PartitionedIndexBackend backend = PartitionedIndexBackendFactory.create(
        conf, false, (partitionPath, recordKey, fileId) -> true)) {
      assertInstanceOf(RecordLevelIndexBackend.class, backend);
    }
  }

  @Test
  void testRocksDBBackendTypeReturnsRocksDBPartitionedIndexBackend() throws Exception {
    conf.set(FlinkOptions.INDEX_RLI_BACKEND_TYPE, "rocksdb");
    conf.set(FlinkOptions.INDEX_RLI_CACHE_ROCKSDB_BASE_PATH, tempFile.getAbsolutePath());
    try (PartitionedIndexBackend backend = PartitionedIndexBackendFactory.create(
        conf, false, (partitionPath, recordKey, fileId) -> true)) {
      assertInstanceOf(RocksDBPartitionedIndexBackend.class, backend);
    }
  }

  @Test
  void testRocksDBBackendTypeIsCaseInsensitive() throws Exception {
    conf.set(FlinkOptions.INDEX_RLI_BACKEND_TYPE, "RocksDB");
    conf.set(FlinkOptions.INDEX_RLI_CACHE_ROCKSDB_BASE_PATH, tempFile.getAbsolutePath());
    try (PartitionedIndexBackend backend = PartitionedIndexBackendFactory.create(
        conf, false, (partitionPath, recordKey, fileId) -> true)) {
      assertInstanceOf(RocksDBPartitionedIndexBackend.class, backend);
    }
  }

  @Test
  void testUnknownBackendTypeFallsBackToRecordLevelIndexBackend() throws Exception {
    conf.set(FlinkOptions.INDEX_RLI_BACKEND_TYPE, "unknown");
    try (PartitionedIndexBackend backend = PartitionedIndexBackendFactory.create(
        conf, false, (partitionPath, recordKey, fileId) -> true)) {
      assertInstanceOf(RecordLevelIndexBackend.class, backend);
    }
  }

  @Test
  void testInsertOverwriteTakesPrecedenceOverRocksDBBackendType() throws Exception {
    conf.set(FlinkOptions.INDEX_RLI_BACKEND_TYPE, "rocksdb");
    conf.set(FlinkOptions.INDEX_RLI_CACHE_ROCKSDB_BASE_PATH, tempFile.getAbsolutePath());
    try (PartitionedIndexBackend backend = PartitionedIndexBackendFactory.create(
        conf, true, (partitionPath, recordKey, fileId) -> true)) {
      assertInstanceOf(DummyPartitionedIndexBackend.class, backend);
    }
  }
}
