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

package org.apache.hudi.utilities.schema;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.UtilitiesTestBase;
import org.apache.spark.streaming.kafka010.KafkaTestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests against {@link MongoSchemaProvider}.
 */
public class TestMongoSchemaProvider extends UtilitiesTestBase {

  private MongoSchemaProvider schemaProvider;
  private KafkaTestUtils testUtils;

  @BeforeClass
  public static void initClass() throws Exception {
    UtilitiesTestBase.initClass();
  }

  @AfterClass
  public static void cleanupClass() throws Exception {
    UtilitiesTestBase.cleanupClass();
  }

  @Before
  public void setup() throws Exception {
    super.setup();
    schemaProvider = new MongoSchemaProvider(Helpers.setupSchemaOnS3(), jsc);
  }

  @After
  public void teardown() throws Exception {
    super.teardown();
  }

  @Test
  public void testCombineSchemaFromS3() throws IOException {
    Pair<Schema, Boolean> schemaPair = schemaProvider.getLatestSourceSchema();
    Schema sourceSchema = schemaPair.getKey();

    HashMap<String, String> fieldNames = new HashMap<String, String>();
    fieldNames.put("_id", "string");
    fieldNames.put("_op", "string");
    fieldNames.put("_ts_ms", "long");
    fieldNames.put("_patch", "string");

    for (Schema.Field f : sourceSchema.getFields()) {
      if (fieldNames.containsKey(f.name())) {
        Schema oplogFieldSchema = f.schema();
        assertEquals(oplogFieldSchema.getType().toString().equals("UNION"), true);

        List<Schema> list = oplogFieldSchema.getTypes();
        for (Schema ff : list) {
          if (!ff.getName().equals("null")) {
            assertEquals(ff.getName().equals(fieldNames.get(f.name())), true);
          }
        }

        fieldNames.remove(f.name());
      }
    }

    assertEquals(fieldNames.isEmpty(), true);
  }
}
