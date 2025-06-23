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

package org.apache.hudi.io;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.Iterator;
import java.util.List;

/**
 * Unit tests {@link HoodieCreateHandle}.
 */
public class BaseTestHandle extends HoodieSparkClientTestHarness {

  @BeforeEach
  public void setUp() throws Exception {
    initSparkContexts("BaseTestHandle");
    initPath();
    initHoodieStorage();
    initTestDataGenerator();
    initMetaClient();
    initTimelineService();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  Pair<WriteStatus, List<HoodieRecord>> createParquetFile(HoodieWriteConfig config, HoodieTable table, String partitionPath,
                                                          String fileId, String instantTime, HoodieTestDataGenerator dataGenerator) {
    List<HoodieRecord> records = dataGenerator.generateInserts(instantTime, 100);
    HoodieCreateHandle handle = new HoodieCreateHandle(config, instantTime, table, partitionPath, fileId, new LocalTaskContextSupplier(), false);
    for (int i = 0; i < records.size(); i++) {
      handle.write(records.get(i), handle.getWriterSchemaWithMetaFields(), handle.getConfig().getProps());
    }
    handle.close();
    return Pair.of(handle.writeStatus, records);
  }

  protected int generateDeleteRecords(List<HoodieRecord> existingRecords, HoodieTestDataGenerator dataGenerator, String instantTime) {
    List<HoodieRecord> deletes = dataGenerator.generateUniqueDeleteRecords(instantTime, 30);
    for (Iterator<HoodieRecord> it = deletes.iterator(); it.hasNext(); ) {
      HoodieRecord deleteRecord = it.next();
      for (HoodieRecord existingRecord : existingRecords) {
        if (deleteRecord.getKey().equals(existingRecord.getKey())) {
          it.remove();
        }
      }
    }
    existingRecords.addAll(deletes);
    return deletes.size();
  }
}
