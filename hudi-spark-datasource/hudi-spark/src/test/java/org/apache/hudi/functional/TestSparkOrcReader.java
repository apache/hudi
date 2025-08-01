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

package org.apache.hudi.functional;

import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.util.JavaConversions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("functional")
public class TestSparkOrcReader extends TestBootstrapReadBase {

  @Test
  public void testReader() {
    dataGen = new HoodieTestDataGenerator(dashPartitionPaths);
    int n = 10;
    Dataset<Row> inserts = makeInsertDf("000", n);
    inserts.write().format("orc").save(bootstrapBasePath);
    Dataset<Row> orcReadRows = JavaConversions.createTestDataFrame(sparkSession, bootstrapBasePath, HoodieFileFormat.ORC);
    Dataset<Row> datasourceReadRows = sparkSession.read().format("orc").load(bootstrapBasePath);
    assertEquals(datasourceReadRows.count(), n);
    assertEquals(orcReadRows.count(), n);
    assertEquals(datasourceReadRows.except(orcReadRows).count(), 0);
    assertEquals(orcReadRows.except(datasourceReadRows).count(), 0);
  }
}
