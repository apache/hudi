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

package org.apache.spark.sql.hudi.execution;

import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.testutils.HoodieClientTestBase;
import org.apache.hudi.util.JavaScalaConverters;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

class TestRangeSampleSort extends HoodieClientTestBase {

  @Test
  void sortDataFrameBySampleSupportAllTypes() {
    Dataset<Row> df = this.context.getSqlContext().sql("select 1 as id, array(2) as content");
    for (int i = 0; i < 2; i++) {
      final int limit = i;
      Assertions.assertDoesNotThrow(() ->
          RangeSampleSort$.MODULE$.sortDataFrameBySampleSupportAllTypes(df.limit(limit),
              JavaScalaConverters.convertJavaListToScalaSeq(Arrays.asList("id", "content")), 1), "range sort shall not fail when 0 or 1 record incoming");
    }
  }

  @Test
  void sortDataFrameBySample() {
    HoodieClusteringConfig.LayoutOptimizationStrategy layoutOptStrategy = HoodieClusteringConfig.LayoutOptimizationStrategy.HILBERT;
    Dataset<Row> df = this.context.getSqlContext().sql("select 1 as id, 2 as content");
    for (int i = 0; i < 2; i++) {
      final int limit = i;
      Assertions.assertDoesNotThrow(() ->
          RangeSampleSort$.MODULE$.sortDataFrameBySample(df.limit(limit), layoutOptStrategy,
              JavaScalaConverters.convertJavaListToScalaSeq(Arrays.asList("id", "content")), 1), "range sort shall not fail when 0 or 1 record incoming");
    }
  }
}
