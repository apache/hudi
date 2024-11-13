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

package org.apache.hudi.utilities;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.TimeGenerator;
import org.apache.hudi.common.table.timeline.TimeGenerators;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DataQualityTestDataGenerator  extends UtilitiesTestBase {

  @BeforeAll
  public static void initClass() throws Exception {
    UtilitiesTestBase.initTestServices(false, false, false);
  }

  @Test
  public void testGenerateData() throws IOException {
    int initialInserts = 10000;
    int recurrentInserts = 1000;
    int recurrentUpdates = 8000;
    int updatesWithLowerOrderingValue = 2000;
    int recurrentDeletes = 1000;
    int totalRounds = 10;
    int numMillsLess = 60000;

    String basePath = "/tmp/trial10";

    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath("/tmp/path").build();
    TimeGenerator timeGenerator = TimeGenerators.getTimeGenerator(writeConfig.getTimeGeneratorConfig(), HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration()));

    String commit1 = HoodieActiveTimeline.createNewInstantTime(true, timeGenerator);

    List<HoodieRecord> initialInsertRecords = dataGen.generateInserts(commit1, initialInserts);

    UtilitiesTestBase.Helpers.saveParquetToDFS(UtilitiesTestBase.Helpers.toGenericRecords(initialInsertRecords), new Path(basePath+"/0/inputdata.parquet"));

    for (int i = 1; i < totalRounds; i++) {
      String commit = HoodieActiveTimeline.createNewInstantTime(true, timeGenerator);
      List<HoodieRecord> allRecords = new ArrayList<>();
      List<GenericRecord> allGenRecs = new ArrayList<>();
      allGenRecs.addAll(dataGen.generateUniqueDeleteGenRecStream(commit, recurrentDeletes).collect(Collectors.toList()));
      List<HoodieRecord> updates = dataGen.generateUniqueUpdates(commit, recurrentUpdates, updatesWithLowerOrderingValue, numMillsLess);
      allRecords.addAll(updates);
      allRecords.addAll(dataGen.generateInserts(commit, recurrentInserts));
      allGenRecs.addAll(UtilitiesTestBase.Helpers.toGenericRecords(allRecords));
      UtilitiesTestBase.Helpers.saveParquetToDFS(allGenRecs, new Path(basePath+"/"+i+"/inputdata.parquet"));
    }
  }

}
