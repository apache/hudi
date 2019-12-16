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

package org.apache.hudi.testsuite.generator;

import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class TestGenericRecordPayloadEstimator {

  @Test
  public void testSimpleSchemaSize() throws Exception {
    Schema schema = new Schema.Parser().parse(getClass().getClassLoader()
        .getResourceAsStream("hudi-test-suite-config/source.avsc"));
    GenericRecordFullPayloadSizeEstimator estimator =
        new GenericRecordFullPayloadSizeEstimator(schema);
    Pair<Integer, Integer> estimateAndNumComplexFields = estimator.typeEstimateAndNumComplexFields();
    assertEquals(estimateAndNumComplexFields.getRight().intValue(), 0);
    assertEquals(estimateAndNumComplexFields.getLeft().intValue(), 156);
  }

  @Test
  public void testComplexSchemaSize() throws Exception {
    Schema schema = new Schema.Parser().parse(getClass().getClassLoader()
        .getResourceAsStream("hudi-test-suite-config/complex-source.avsc"));
    GenericRecordFullPayloadSizeEstimator estimator =
        new GenericRecordFullPayloadSizeEstimator(schema);
    Pair<Integer, Integer> estimateAndNumComplexFields = estimator.typeEstimateAndNumComplexFields();
    assertEquals(estimateAndNumComplexFields.getRight().intValue(), 1);
    assertEquals(estimateAndNumComplexFields.getLeft().intValue(), 1278);
  }

}
