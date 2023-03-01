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

package org.apache.hudi.spark3.internal;

import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestLegacyParquetNanosLong {
  @Test
  public void testConfig() {
    // In HoodieSparkFileReaderFactory and Spark32PlusHoodieParquetFileFormat
    // We use string values instead of the SQLConf config,
    // So this test is to catch any changes to the config's values.
    assertEquals(SQLConf.LEGACY_PARQUET_NANOS_AS_LONG().key(), "spark.sql.legacy.parquet.nanosAsLong");
    assertEquals(SQLConf.LEGACY_PARQUET_NANOS_AS_LONG().defaultValueString(), "false");
  }
}
