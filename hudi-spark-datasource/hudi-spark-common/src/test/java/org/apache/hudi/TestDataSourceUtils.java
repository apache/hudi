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

package org.apache.hudi;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TestDataSourceUtils extends HoodieClientTestBase {

  @Test
  void testDeduplicationAgainstRecordsAlreadyInTable() {
    HoodieWriteConfig config = getConfig();
    config.getProps().setProperty("path", config.getBasePath());
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config)) {
      String newCommitTime = writeClient.startCommit();
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
      JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 2);
      List<WriteStatus> statusList = writeClient.bulkInsert(recordsRDD, newCommitTime).collect();
      writeClient.commit(newCommitTime, jsc.parallelize(statusList), Option.empty(), COMMIT_ACTION, Collections.emptyMap(), Option.empty());
      assertNoWriteErrors(statusList);

      Map<String, String> parameters = config.getProps().entrySet().stream().collect(Collectors.toMap(entry -> entry.getKey().toString(), entry -> entry.getValue().toString()));
      List<HoodieRecord> newRecords = dataGen.generateInserts(newCommitTime, 10);
      List<HoodieRecord> inputRecords = Stream.concat(records.subList(0, 10).stream(), newRecords.stream()).collect(Collectors.toList());
      List<HoodieRecord> output = DataSourceUtils.resolveDuplicates(jsc, jsc.parallelize(inputRecords, 1), parameters, false).collect();
      Set<String> expectedRecordKeys = newRecords.stream().map(HoodieRecord::getRecordKey).collect(Collectors.toSet());
      assertEquals(expectedRecordKeys, output.stream().map(HoodieRecord::getRecordKey).collect(Collectors.toSet()));
    }
  }
}
