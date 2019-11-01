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

package org.apache.hudi.bench.writer;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import java.util.Properties;
import org.apache.hudi.HoodieReadClient;
import org.apache.hudi.HoodieWriteClient;
import org.apache.hudi.bench.job.HoodieDeltaStreamerWrapper;
import org.apache.hudi.bench.job.HoodieTestSuiteJob.HoodieTestSuiteConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DeltaWriter.class})
public class TestDeltaWriter {

  @Test
  public void testWorkloadWriterWithDeltaStreamer() throws Exception {
    JavaSparkContext mockSC = mock(JavaSparkContext.class);
    HoodieTestSuiteConfig mockCfg = mock(HoodieTestSuiteConfig.class);
    mockCfg.useDeltaStreamer = true;
    Properties props = mock(Properties.class);
    HoodieWriteClient mockWriteClient = PowerMockito.mock(HoodieWriteClient.class);
    HoodieReadClient mockReadClient = PowerMockito.mock(HoodieReadClient.class);
    PowerMockito.whenNew(HoodieReadClient.class).withAnyArguments().thenReturn(mockReadClient);
    PowerMockito.whenNew(HoodieWriteClient.class).withAnyArguments().thenReturn(mockWriteClient);
    HoodieDeltaStreamerWrapper mockDeltaWrapper = PowerMockito.mock(HoodieDeltaStreamerWrapper.class);
    PowerMockito.whenNew(HoodieDeltaStreamerWrapper.class).withArguments(mockCfg, mockSC).thenReturn(mockDeltaWrapper);

    String schema = "schema";
    String checkPoint = "DUMMY";
    String commitTime = "0";
    when(mockDeltaWrapper.fetchSource()).thenReturn(Pair.of(null, Pair.of(checkPoint, null)));
    when(mockDeltaWrapper.insert(Option.empty())).thenReturn(null);
    when(mockDeltaWrapper.upsert(Option.empty())).thenReturn(null);
    when(mockDeltaWrapper.bulkInsert(Option.empty())).thenReturn(null);

    when(mockWriteClient.startCommit()).thenReturn(commitTime);
    DeltaWriter deltaWriter = new DeltaWriter(mockSC, props, mockCfg, schema);
    assertEquals(deltaWriter.fetchSource().getRight().getKey(), checkPoint);
    assertEquals(deltaWriter.fetchSource().getRight().getValue(), null);
    assertFalse(deltaWriter.startCommit().get() == commitTime);
    assertTrue(deltaWriter.bulkInsert(Option.of(commitTime)) == null);
    assertTrue(deltaWriter.insert(Option.of(commitTime)) == null);
    assertTrue(deltaWriter.upsert(Option.of(commitTime)) == null);
  }

  @Test
  public void testWorkloadWriterWithHoodieWriteClient() throws Exception {
    JavaSparkContext mockSC = mock(JavaSparkContext.class);
    HoodieTestSuiteConfig mockCfg = mock(HoodieTestSuiteConfig.class);
    mockCfg.useDeltaStreamer = false;
    mockCfg.targetBasePath = "test";
    mockCfg.targetTableName = "test";
    mockCfg.payloadClassName = "test";
    Properties props = mock(Properties.class);
    HoodieWriteClient mockWriteClient = PowerMockito.mock(HoodieWriteClient.class);
    PowerMockito.whenNew(HoodieWriteClient.class).withAnyArguments().thenReturn(mockWriteClient);
    HoodieReadClient mockReadClient = PowerMockito.mock(HoodieReadClient.class);
    PowerMockito.whenNew(HoodieReadClient.class).withAnyArguments().thenReturn(mockReadClient);
    HoodieDeltaStreamerWrapper mockDeltaWrapper = PowerMockito.mock(HoodieDeltaStreamerWrapper.class);
    PowerMockito.whenNew(HoodieDeltaStreamerWrapper.class).withArguments(mockCfg, mockSC).thenReturn(mockDeltaWrapper);

    String schema = "schema";
    String checkPoint = "DUMMY";
    String commitTime = "0";

    when(mockDeltaWrapper.fetchSource()).thenReturn(Pair.of(null, Pair.of(checkPoint, null)));
    when(mockWriteClient.startCommit()).thenReturn(commitTime);
    when(mockWriteClient.insert(any(), anyString())).thenReturn(null);
    when(mockWriteClient.upsert(any(), anyString())).thenReturn(null);
    when(mockWriteClient.bulkInsert(any(), anyString())).thenReturn(null);

    when(mockWriteClient.startCommit()).thenReturn(commitTime);
    DeltaWriter deltaWriter = new DeltaWriter(mockSC, props, mockCfg, schema);
    assertEquals(deltaWriter.fetchSource().getRight().getKey(), checkPoint);
    assertEquals(deltaWriter.fetchSource().getRight().getValue(), null);
    assertEquals(deltaWriter.startCommit().get(), commitTime);
    assertTrue(deltaWriter.bulkInsert(Option.of(commitTime)) == null);
    assertTrue(deltaWriter.insert(Option.of(commitTime)) == null);
    assertTrue(deltaWriter.upsert(Option.of(commitTime)) == null);
  }

}
