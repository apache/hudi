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

package org.apache.hudi.sink;

import org.apache.hudi.common.model.EventTimeAvroPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.utils.TestData;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

/**
 * Test cases for delta stream write.
 */
public class TestWriteMergeOnRead extends TestWriteCopyOnWrite {

  @Override
  protected void setUp(Configuration conf) {
    conf.setBoolean(FlinkOptions.COMPACTION_ASYNC_ENABLED, false);
  }

  @Test
  public void testIndexStateBootstrapWithMultiFilesInOneSlice() throws Exception {
    // open the function and ingest data
    preparePipeline(conf)
        .consume(TestData.filterOddRows(TestData.DATA_SET_INSERT))
        .assertEmptyDataFiles()
        .checkpoint(1)
        .assertNextEvent()
        .checkpointComplete(1)
        .consume(TestData.filterEvenRows(TestData.DATA_SET_INSERT))
        .checkpoint(2)
        .assertNextEvent()
        .checkpointComplete(2)
        .checkWrittenData(EXPECTED1, 4)
        // write another commit but does not complete it
        .consume(TestData.filterEvenRows(TestData.DATA_SET_INSERT))
        .checkpoint(3)
        .assertNextEvent()
        .end();

    // reset the config option
    conf.setBoolean(FlinkOptions.INDEX_BOOTSTRAP_ENABLED, true);
    validateIndexLoaded();
  }

  @Test
  public void testIndexStateBootstrapWithCompactionScheduled() throws Exception {
    // sets up the delta commits as 1 to generate a new compaction plan.
    conf.setInteger(FlinkOptions.COMPACTION_DELTA_COMMITS, 1);
    // open the function and ingest data
    preparePipeline(conf)
        .consume(TestData.DATA_SET_INSERT)
        .assertEmptyDataFiles()
        .checkpoint(1)
        .assertNextEvent()
        .checkpointComplete(1)
        .checkWrittenData(EXPECTED1, 4)
        .end();

    // reset config options
    conf.removeConfig(FlinkOptions.COMPACTION_DELTA_COMMITS);
    // sets up index bootstrap
    conf.setBoolean(FlinkOptions.INDEX_BOOTSTRAP_ENABLED, true);
    validateIndexLoaded();
  }

  @Test
  public void testEventTimeAvroPayloadMergeRead() throws Exception {
    conf.set(FlinkOptions.COMPACTION_ASYNC_ENABLED, true);
    conf.set(FlinkOptions.PATH, tempFile.getAbsolutePath());
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.MERGE_ON_READ.name());
    conf.set(FlinkOptions.OPERATION, "upsert");
    conf.set(FlinkOptions.CHANGELOG_ENABLED, false);
    conf.set(FlinkOptions.COMPACTION_DELTA_COMMITS, 2);
    conf.set(FlinkOptions.PRE_COMBINE, true);
    conf.set(FlinkOptions.PRECOMBINE_FIELD, "ts");
    conf.set(FlinkOptions.PAYLOAD_CLASS_NAME, EventTimeAvroPayload.class.getName());
    HashMap<String, String> mergedExpected = new HashMap<>(EXPECTED1);
    mergedExpected.put("par1", "[id1,par1,id1,Danny,22,4,par1, id2,par1,id2,Stephen,33,2,par1]");
    TestHarness.instance().preparePipeline(tempFile, conf)
        .consume(TestData.DATA_SET_INSERT)
        .emptyEventBuffer()
        .checkpoint(1)
        .assertNextEvent()
        .checkpointComplete(1)
        .checkWrittenData(EXPECTED1, 4)
        .consume(TestData.DATA_SET_DISORDER_INSERT)
        .emptyEventBuffer()
        .checkpoint(2)
        .assertNextEvent()
        .checkpointComplete(2)
        .checkWrittenData(mergedExpected, 4)
        .consume(TestData.DATA_SET_SINGLE_INSERT)
        .emptyEventBuffer()
        .checkpoint(3)
        .assertNextEvent()
        .checkpointComplete(3)
        .checkWrittenData(mergedExpected, 4)
        .end();
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  public void testOnlyBaseFileOrOnlyLogFileRead(int compactionDeltaCommits) throws Exception {
    conf.set(FlinkOptions.COMPACTION_ASYNC_ENABLED, true);
    conf.set(FlinkOptions.PATH, tempFile.getAbsolutePath());
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.MERGE_ON_READ.name());
    conf.set(FlinkOptions.OPERATION, "upsert");
    conf.set(FlinkOptions.CHANGELOG_ENABLED, false);
    conf.set(FlinkOptions.COMPACTION_DELTA_COMMITS, compactionDeltaCommits);
    TestHarness.instance().preparePipeline(tempFile, conf)
        .consume(TestData.DATA_SET_INSERT)
        .emptyEventBuffer()
        .checkpoint(1)
        .assertNextEvent()
        .checkpointComplete(1)
        .checkWrittenData(EXPECTED1, 4)
        .end();
  }

  @Override
  public void testInsertClustering() {
    // insert clustering is only valid for cow table.
  }

  @Override
  protected Map<String, String> getExpectedBeforeCheckpointComplete() {
    return EXPECTED1;
  }

  protected Map<String, String> getMiniBatchExpected() {
    Map<String, String> expected = new HashMap<>();
    // MOR mode merges the messages with the same key.
    expected.put("par1", "[id1,par1,id1,Danny,23,1,par1]");
    return expected;
  }

  @Override
  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }
}
