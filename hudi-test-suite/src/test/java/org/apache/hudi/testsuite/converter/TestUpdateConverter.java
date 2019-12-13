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

package org.apache.hudi.testsuite.converter;

import org.apache.hudi.testsuite.utils.TestUtils;
import org.apache.hudi.utilities.UtilHelpers;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import scala.Tuple2;

import static junit.framework.TestCase.assertTrue;

public class TestUpdateConverter {

  private JavaSparkContext jsc;

  @Before
  public void setup() throws Exception {
    jsc = UtilHelpers.buildSparkContext(this.getClass().getName() + "-hoodie", "local[1]");

  }

  @After
  public void teardown() {
    jsc.stop();
  }

  @Test
  public void testGenerateUpdateRecordsFromInputRecords() throws Exception {
    JavaRDD<GenericRecord> inputRDD = TestUtils.makeRDD(jsc, 10);
    String schemaStr = inputRDD.take(1).get(0).getSchema().toString();
    int minPayloadSize = 1000;
    // 2. DFS converter reads existing records and generates random updates for the same row keys
    UpdateConverter updateConverter = new UpdateConverter(schemaStr, minPayloadSize,
        Arrays.asList("timestamp"), Arrays.asList("_row_key"));
    List<String> insertRowKeys = inputRDD.map(r -> r.get("_row_key").toString()).collect();
    Assert.assertTrue(inputRDD.count() == 10);
    JavaRDD<GenericRecord> outputRDD = updateConverter.convert(inputRDD);
    List<String> updateRowKeys = outputRDD.map(row -> row.get("_row_key").toString()).collect();
    // The insert row keys should be the same as update row keys
    Assert.assertTrue(insertRowKeys.containsAll(updateRowKeys));
    Map<String, GenericRecord> inputRecords = inputRDD.mapToPair(r -> new Tuple2<>(r.get("_row_key").toString(), r))
        .collectAsMap();
    List<GenericRecord> updateRecords = outputRDD.collect();
    updateRecords.stream().forEach(updateRecord -> {
      GenericRecord inputRecord = inputRecords.get(updateRecord.get("_row_key").toString());
      assertTrue(areRecordsDifferent(inputRecord, updateRecord));
    });

  }

  /**
   * Checks if even a single field in the 2 records is different (except the row key which is the same for an update).
   */
  private boolean areRecordsDifferent(GenericRecord in, GenericRecord up) {
    for (Field field : in.getSchema().getFields()) {
      if (field.name() == "_row_key") {
        continue;
      } else {
        // Just convert all types to string for now since all are primitive
        if (in.get(field.name()).toString() != up.get(field.name()).toString()) {
          return true;
        }
      }
    }
    return false;
  }
}
