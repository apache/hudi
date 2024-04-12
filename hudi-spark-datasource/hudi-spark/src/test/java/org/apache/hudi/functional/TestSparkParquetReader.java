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

import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.util.JavaConversions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("functional")
public class TestSparkParquetReader extends TestBootstrapReadBase {

  @Test
  public void testReader() {
    dataGen = new HoodieTestDataGenerator(dashPartitionPaths);
    int n = 10;
    Dataset<Row> inserts = makeInsertDf("000", n);
    inserts.write().format("parquet").save(bootstrapBasePath);
    Dataset<Row> parquetReadRows = JavaConversions.createTestDataFrame(sparkSession, bootstrapBasePath);
    Dataset<Row> datasourceReadRows = sparkSession.read().format("parquet").load(bootstrapBasePath);
    assertEquals(datasourceReadRows.count(), n);
    assertEquals(parquetReadRows.count(), n);
    assertEquals(datasourceReadRows.except(parquetReadRows).count(), 0);
    assertEquals(parquetReadRows.except(datasourceReadRows).count(), 0);
  }
}
