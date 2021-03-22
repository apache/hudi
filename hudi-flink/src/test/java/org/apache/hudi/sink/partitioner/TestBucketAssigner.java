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

package org.apache.hudi.sink.partitioner;

import org.apache.hudi.client.FlinkTaskContextSupplier;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.action.commit.BucketInfo;
import org.apache.hudi.table.action.commit.BucketType;
import org.apache.hudi.table.action.commit.SmallFile;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Test cases for {@link BucketAssigner}.
 */
public class TestBucketAssigner {
  private HoodieWriteConfig writeConfig;
  private HoodieFlinkEngineContext context;

  @TempDir
  File tempFile;

  @BeforeEach
  public void before() throws IOException {
    final String basePath = tempFile.getAbsolutePath();
    final Configuration conf = TestConfigurations.getDefaultConf(basePath);

    writeConfig = StreamerUtil.getHoodieClientConfig(conf);
    context = new HoodieFlinkEngineContext(
        new SerializableConfiguration(StreamerUtil.getHadoopConf()),
        new FlinkTaskContextSupplier(null));
    StreamerUtil.initTableIfNotExists(conf);
  }

  @Test
  public void testAddUpdate() {
    MockBucketAssigner mockBucketAssigner = new MockBucketAssigner(context, writeConfig);
    BucketInfo bucketInfo = mockBucketAssigner.addUpdate("par1", "file_id_0");
    assertBucketEquals(bucketInfo, "par1", BucketType.UPDATE, "file_id_0");

    mockBucketAssigner.addUpdate("par1", "file_id_0");
    bucketInfo = mockBucketAssigner.addUpdate("par1", "file_id_0");
    assertBucketEquals(bucketInfo, "par1", BucketType.UPDATE, "file_id_0");

    mockBucketAssigner.addUpdate("par1", "file_id_1");
    bucketInfo = mockBucketAssigner.addUpdate("par1", "file_id_1");
    assertBucketEquals(bucketInfo, "par1", BucketType.UPDATE, "file_id_1");

    bucketInfo = mockBucketAssigner.addUpdate("par2", "file_id_0");
    assertBucketEquals(bucketInfo, "par2", BucketType.UPDATE, "file_id_0");

    bucketInfo = mockBucketAssigner.addUpdate("par3", "file_id_2");
    assertBucketEquals(bucketInfo, "par3", BucketType.UPDATE, "file_id_2");
  }

  @Test
  public void testAddInsert() {
    MockBucketAssigner mockBucketAssigner = new MockBucketAssigner(context, writeConfig);
    BucketInfo bucketInfo = mockBucketAssigner.addInsert("par1");
    assertBucketEquals(bucketInfo, "par1", BucketType.INSERT);

    mockBucketAssigner.addInsert("par1");
    bucketInfo = mockBucketAssigner.addInsert("par1");
    assertBucketEquals(bucketInfo, "par1", BucketType.INSERT);

    mockBucketAssigner.addInsert("par2");
    bucketInfo = mockBucketAssigner.addInsert("par2");
    assertBucketEquals(bucketInfo, "par2", BucketType.INSERT);

    bucketInfo = mockBucketAssigner.addInsert("par3");
    assertBucketEquals(bucketInfo, "par3", BucketType.INSERT);

    bucketInfo = mockBucketAssigner.addInsert("par3");
    assertBucketEquals(bucketInfo, "par3", BucketType.INSERT);
  }

  @Test
  public void testInsertWithSmallFiles() {
    SmallFile f0 = new SmallFile();
    f0.location = new HoodieRecordLocation("t0", "f0");
    f0.sizeBytes = 12;

    SmallFile f1 = new SmallFile();
    f1.location = new HoodieRecordLocation("t0", "f1");
    f1.sizeBytes = 122879; // no left space to append new records to this bucket

    SmallFile f2 = new SmallFile();
    f2.location = new HoodieRecordLocation("t0", "f2");
    f2.sizeBytes = 56;

    Map<String, List<SmallFile>> smallFilesMap = new HashMap<>();
    smallFilesMap.put("par1", Arrays.asList(f0, f1));
    smallFilesMap.put("par2", Collections.singletonList(f2));

    MockBucketAssigner mockBucketAssigner = new MockBucketAssigner(context, writeConfig, smallFilesMap);
    BucketInfo bucketInfo = mockBucketAssigner.addInsert("par1");
    assertBucketEquals(bucketInfo, "par1", BucketType.UPDATE, "f0");

    mockBucketAssigner.addInsert("par1");
    bucketInfo = mockBucketAssigner.addInsert("par1");
    assertBucketEquals(bucketInfo, "par1", BucketType.UPDATE, "f0");

    mockBucketAssigner.addInsert("par2");
    bucketInfo = mockBucketAssigner.addInsert("par2");
    assertBucketEquals(bucketInfo, "par2", BucketType.UPDATE, "f2");

    bucketInfo = mockBucketAssigner.addInsert("par3");
    assertBucketEquals(bucketInfo, "par3", BucketType.INSERT);

    bucketInfo = mockBucketAssigner.addInsert("par3");
    assertBucketEquals(bucketInfo, "par3", BucketType.INSERT);
  }

  @Test
  public void testUpdateAndInsertWithSmallFiles() {
    SmallFile f0 = new SmallFile();
    f0.location = new HoodieRecordLocation("t0", "f0");
    f0.sizeBytes = 12;

    SmallFile f1 = new SmallFile();
    f1.location = new HoodieRecordLocation("t0", "f1");
    f1.sizeBytes = 122879; // no left space to append new records to this bucket

    SmallFile f2 = new SmallFile();
    f2.location = new HoodieRecordLocation("t0", "f2");
    f2.sizeBytes = 56;

    Map<String, List<SmallFile>> smallFilesMap = new HashMap<>();
    smallFilesMap.put("par1", Arrays.asList(f0, f1));
    smallFilesMap.put("par2", Collections.singletonList(f2));

    MockBucketAssigner mockBucketAssigner = new MockBucketAssigner(context, writeConfig, smallFilesMap);
    mockBucketAssigner.addUpdate("par1", "f0");

    BucketInfo bucketInfo = mockBucketAssigner.addInsert("par1");
    assertBucketEquals(bucketInfo, "par1", BucketType.UPDATE, "f0");

    mockBucketAssigner.addInsert("par1");
    bucketInfo = mockBucketAssigner.addInsert("par1");
    assertBucketEquals(bucketInfo, "par1", BucketType.UPDATE, "f0");

    mockBucketAssigner.addUpdate("par1", "f2");

    mockBucketAssigner.addInsert("par1");
    bucketInfo = mockBucketAssigner.addInsert("par1");
    assertBucketEquals(bucketInfo, "par1", BucketType.UPDATE, "f0");

    mockBucketAssigner.addUpdate("par2", "f0");

    mockBucketAssigner.addInsert("par2");
    bucketInfo = mockBucketAssigner.addInsert("par2");
    assertBucketEquals(bucketInfo, "par2", BucketType.UPDATE, "f2");
  }

  private void assertBucketEquals(
      BucketInfo bucketInfo,
      String partition,
      BucketType bucketType,
      String fileId) {
    BucketInfo actual = new BucketInfo(bucketType, fileId, partition);
    assertThat(bucketInfo, is(actual));
  }

  private void assertBucketEquals(
      BucketInfo bucketInfo,
      String partition,
      BucketType bucketType) {
    assertThat(bucketInfo.getPartitionPath(), is(partition));
    assertThat(bucketInfo.getBucketType(), is(bucketType));
  }

  /**
   * Mock BucketAssigner that can specify small files explicitly.
   */
  static class MockBucketAssigner extends BucketAssigner {
    private final Map<String, List<SmallFile>> smallFilesMap;

    MockBucketAssigner(
        HoodieFlinkEngineContext context,
        HoodieWriteConfig config) {
      this(context, config, Collections.emptyMap());
    }

    MockBucketAssigner(
        HoodieFlinkEngineContext context,
        HoodieWriteConfig config,
        Map<String, List<SmallFile>> smallFilesMap) {
      super(context, config);
      this.smallFilesMap = smallFilesMap;
    }

    @Override
    protected List<SmallFile> getSmallFiles(String partitionPath) {
      if (this.smallFilesMap.containsKey(partitionPath)) {
        return this.smallFilesMap.get(partitionPath);
      }
      return Collections.emptyList();
    }
  }
}
