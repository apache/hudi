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

package org.apache.spark.execution.datasources.parquet

import org.apache.hudi.SparkFileFormatInternalRowReaderContext
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness
import org.apache.spark.sql.sources.{EqualTo, GreaterThan, IsNotNull}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class TestHoodieFileGroupReaderBasedParquetFileFormat extends SparkClientFunctionalTestHarness {
  @Test
  def testGetRecordKeyRelatedFilters(): Unit = {
    val filters = Seq(
      IsNotNull("non_key_column"),
      EqualTo("non_key_column", 1)
    )
    val filtersWithoutKeyColumn = SparkFileFormatInternalRowReaderContext.getRecordKeyRelatedFilters(
      filters, "key_column");
    assertEquals(0, filtersWithoutKeyColumn.size)

    val filtersWithKeys = Seq(
      EqualTo("key_column", 1),
      GreaterThan("non_key_column", 2)
    )
    val filtersWithKeyColumn = SparkFileFormatInternalRowReaderContext.getRecordKeyRelatedFilters(
      filtersWithKeys, "key_column")
    assertEquals(1, filtersWithKeyColumn.size)
    assertEquals("key_column", filtersWithKeyColumn.head.references.head)
  }
}
