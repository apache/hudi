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
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.testutils.HoodieClientTestUtils;

import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
}
