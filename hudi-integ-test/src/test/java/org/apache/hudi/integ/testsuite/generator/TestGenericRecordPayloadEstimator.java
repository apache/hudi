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

package org.apache.hudi.integ.testsuite.generator;

import static junit.framework.TestCase.assertEquals;

import org.apache.avro.Schema;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;
import org.junit.jupiter.api.Test;

public class TestGenericRecordPayloadEstimator {

  private static final String SOURCE_SCHEMA_DOCKER_DEMO_RELATIVE_PATH = "/docker/demo/config/test-suite/source.avsc";
  private static final String COMPLEX_SOURCE_SCHEMA_DOCKER_DEMO_RELATIVE_PATH =
      "/docker/demo/config/test-suite/complex-source.avsc";

  @Test
  public void testSimpleSchemaSize() throws Exception {
    Schema schema = new Schema.Parser().parse(UtilitiesTestBase.Helpers
        .readFileFromAbsolutePath(System.getProperty("user.dir") + "/.." + SOURCE_SCHEMA_DOCKER_DEMO_RELATIVE_PATH));
    GenericRecordFullPayloadSizeEstimator estimator =
        new GenericRecordFullPayloadSizeEstimator(schema);
    Pair<Integer, Integer> estimateAndNumComplexFields = estimator.typeEstimateAndNumComplexFields();
    assertEquals(estimateAndNumComplexFields.getRight().intValue(), 0);
    assertEquals(estimateAndNumComplexFields.getLeft().intValue(), 156);
  }

  @Test
  public void testComplexSchemaSize() throws Exception {
    Schema schema = new Schema.Parser().parse(UtilitiesTestBase.Helpers.readFileFromAbsolutePath(
        System.getProperty("user.dir") + "/.." + COMPLEX_SOURCE_SCHEMA_DOCKER_DEMO_RELATIVE_PATH));
    GenericRecordFullPayloadSizeEstimator estimator =
        new GenericRecordFullPayloadSizeEstimator(schema);
    Pair<Integer, Integer> estimateAndNumComplexFields = estimator.typeEstimateAndNumComplexFields();
    assertEquals(estimateAndNumComplexFields.getRight().intValue(), 1);
    assertEquals(estimateAndNumComplexFields.getLeft().intValue(), 1278);
  }

}
