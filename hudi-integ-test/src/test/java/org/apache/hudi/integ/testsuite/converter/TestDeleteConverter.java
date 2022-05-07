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

package org.apache.hudi.integ.testsuite.converter;

import org.apache.hudi.integ.testsuite.utils.TestUtils;
import org.apache.hudi.utilities.UtilHelpers;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import scala.Tuple2;

import static junit.framework.TestCase.assertTrue;
import static org.apache.hudi.integ.testsuite.generator.GenericRecordFullPayloadGenerator.DEFAULT_HOODIE_IS_DELETED_COL;

/**
 * Tests DeleteConverter.
 */
public class TestDeleteConverter {

  private JavaSparkContext jsc;

  @BeforeEach
  public void setup() throws Exception {
    jsc = UtilHelpers.buildSparkContext(this.getClass().getName() + "-hoodie", "local[1]");

  }

  @AfterEach
  public void teardown() {
    jsc.stop();
  }

  /**
   * Test {@link UpdateConverter} by generates random deletes from existing records.
   */
  @Test
  public void testGenerateDeleteRecordsFromInputRecords() throws Exception {
    // 1. prepare input records
    JavaRDD<GenericRecord> inputRDD = TestUtils.makeRDD(jsc, 10);
    String schemaStr = inputRDD.take(1).get(0).getSchema().toString();
    int minPayloadSize = 1000;

    // 2. Converter reads existing records and generates deletes
    DeleteConverter deleteConverter = new DeleteConverter(schemaStr, minPayloadSize);
    List<String> insertRowKeys = inputRDD.map(r -> r.get("_row_key").toString()).collect();
    assertTrue(inputRDD.count() == 10);
    JavaRDD<GenericRecord> outputRDD = deleteConverter.convert(inputRDD);
    List<String> updateRowKeys = outputRDD.map(row -> row.get("_row_key").toString()).collect();
    // The insert row keys should be the same as delete row keys
    assertTrue(insertRowKeys.containsAll(updateRowKeys));
    Map<String, GenericRecord> inputRecords = inputRDD.mapToPair(r -> new Tuple2<>(r.get("_row_key").toString(), r))
        .collectAsMap();
    List<GenericRecord> deleteRecords = outputRDD.collect();
    deleteRecords.stream().forEach(updateRecord -> {
      assertTrue((boolean) updateRecord.get(DEFAULT_HOODIE_IS_DELETED_COL));
    });
  }
}
