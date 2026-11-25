/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.table.action.commit;

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.ExceptionUtil;
import org.apache.hudi.exception.HoodieMultipleMatchesException;
import org.apache.hudi.index.HoodieIndexUtils;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.testutils.HoodieClientTestUtils;

import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link HoodieWriteHelper}
 */
public class TestSparkWriteHelper extends TestWriterHelperBase<HoodieData<HoodieRecord>> {
  JavaSparkContext jsc;

  public TestSparkWriteHelper() {
    super(HoodieWriteHelper.newInstance());
  }

  @BeforeEach
  public void setup() throws Exception {
    super.setUp();
    this.jsc = new JavaSparkContext(
        HoodieClientTestUtils.getSparkConfForTest(TestSparkWriteHelper.class.getName()));
    this.context = new HoodieSparkEngineContext(jsc);
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withEmbeddedTimelineServerEnabled(false)
        .build();
    this.table = HoodieSparkTable.create(config, context, metaClient);
  }

  @Override
  public HoodieData<HoodieRecord> getInputRecords(List<HoodieRecord> recordList, int numPartitions) {
    HoodieData<HoodieRecord> inputRecords = context.parallelize(recordList, numPartitions);
    assertEquals(numPartitions, inputRecords.getNumPartitions());
    return inputRecords;
  }

  @AfterEach
  public void tearDown() throws Exception {
    super.tearDown();
    if (this.jsc != null) {
      this.jsc.stop();
    }
    this.context = null;
  }

  @ParameterizedTest
  @CsvSource({"true,0", "true,50", "false,0", "false,50"})
  public void testCombineParallelism(boolean shouldCombine, int configuredShuffleParallelism) {
    int inputParallelism = 5;
    int expectDefaultParallelism = 4;
    inputRecords = getInputRecords(
        dataGen.generateInserts("20230915000000000", 10), inputParallelism);
    HoodieData<HoodieRecord> outputRecords = (HoodieData<HoodieRecord>) writeHelper.combineOnCondition(
        shouldCombine, inputRecords, configuredShuffleParallelism, table);

    if (shouldCombine) {
      if (configuredShuffleParallelism == 0) {
        assertEquals(expectDefaultParallelism, outputRecords.getNumPartitions());
      } else {
        assertEquals(configuredShuffleParallelism, outputRecords.getNumPartitions());
      }
    } else {
      assertEquals(inputParallelism, outputRecords.getNumPartitions());
    }
  }

  @Test
  void testFailOnMultipleMatches() throws IOException {
    List<HoodieRecord> inserts = dataGen.generateInserts("20230915000000000", 3);
    List<HoodieRecord> updates = dataGen.generateUpdates("20230915000000001", inserts);
    // One record per row it is located at: in two file groups, and at two rows of one base file
    List<HoodieRecord> tagged = Arrays.asList(
        located(inserts.get(0), "fg1", -1L), located(inserts.get(0), "fg2", -1L),
        located(inserts.get(1), "fg1", 3L), located(inserts.get(1), "fg1", 4L),
        inserts.get(2), updates.get(2));
    List<HoodieRecord> kept = ((HoodieData<HoodieRecord>) writeHelper.failOnMultipleMatches(
        getInputRecords(tagged, 2), false, table)).collectAsList();
    assertEquals(tagged.stream().map(TestSparkWriteHelper::recordAndRow).sorted().collect(Collectors.toList()),
        kept.stream().map(TestSparkWriteHelper::recordAndRow).sorted().collect(Collectors.toList()));
    // The two records of the key the table does not hold are combined into one
    List<HoodieRecord> combined = ((HoodieData<HoodieRecord>) writeHelper.failOnMultipleMatches(
        getInputRecords(tagged, 2), true, table)).collectAsList();
    assertEquals(tagged.subList(0, 5).stream().map(TestSparkWriteHelper::recordAndRow).sorted().collect(Collectors.toList()),
        combined.stream().map(TestSparkWriteHelper::recordAndRow).sorted().collect(Collectors.toList()));

    // Two records located at the same row
    for (long position : new long[] {-1L, 3L}) {
      List<HoodieRecord> multipleMatches = Arrays.asList(
          located(inserts.get(0), "fg1", position), located(updates.get(0), "fg1", position), inserts.get(1));
      Exception e = assertThrows(Exception.class, () -> ((HoodieData<HoodieRecord>) writeHelper.failOnMultipleMatches(
          getInputRecords(multipleMatches, 2), true, table)).collectAsList());
      assertTrue(ExceptionUtil.isCausedBy(e, HoodieMultipleMatchesException.class), e::toString);
    }
  }

  private static String recordAndRow(HoodieRecord record) {
    return record.getRecordKey() + "/" + record.getPartitionPath()
        + (record.isCurrentLocationKnown() ? "@" + record.getCurrentLocation().getFileId() + ":" + record.getCurrentPosition() : "");
  }

  private static HoodieRecord located(HoodieRecord record, String fileId, long position) {
    return HoodieIndexUtils.tagRecord(record.newInstance(), new HoodieRecordLocation("20230915000000000", fileId, position));
  }
}
