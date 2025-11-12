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

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.util.List;

/**
 * Tests for write helpers
 */
public abstract class TestWriterHelperBase<I> extends HoodieCommonTestHarness {
  private static int runNo = 0;
  protected final BaseWriteHelper writeHelper;
  protected HoodieEngineContext context;
  protected HoodieTable table;
  protected I inputRecords;

  public TestWriterHelperBase(BaseWriteHelper writeHelper) {
    this.writeHelper = writeHelper;
  }

  public abstract I getInputRecords(List<HoodieRecord> recordList, int numPartitions);

  @BeforeEach
  public void setUp() throws Exception {
    initResources();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  private void initResources() throws IOException {
    initPath("dataset" + runNo);
    runNo++;
    initTestDataGenerator();
    initMetaClient();
  }

  private void cleanupResources() {
    cleanMetaClient();
    cleanupTestDataGenerator();
  }
}
