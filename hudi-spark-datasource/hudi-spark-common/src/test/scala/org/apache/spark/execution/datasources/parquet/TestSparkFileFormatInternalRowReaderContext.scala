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
import org.apache.hudi.SparkFileFormatInternalRowReaderContext.filterIsSafeForBootstrap
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.table.read.buffer.PositionBasedFileGroupRecordBuffer.ROW_INDEX_TEMPORARY_COLUMN_NAME
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness

import org.apache.spark.sql.execution.datasources.SparkColumnarFileReader
import org.apache.spark.sql.sources.{And, IsNotNull, Or}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.mockito.Mockito.when

class TestSparkFileFormatInternalRowReaderContext extends SparkClientFunctionalTestHarness {

  @Test
  def testBootstrapFilters(): Unit = {
    val recordKeyField = HoodieRecord.HoodieMetadataField.RECORD_KEY_METADATA_FIELD.getFieldName
    val commitTimeField = HoodieRecord.HoodieMetadataField.COMMIT_TIME_METADATA_FIELD.getFieldName

    val recordKeyFilter = IsNotNull(recordKeyField)
    assertTrue(filterIsSafeForBootstrap(recordKeyFilter))
    val commitTimeFilter = IsNotNull(commitTimeField)
    assertTrue(filterIsSafeForBootstrap(commitTimeFilter))

    val dataFieldFilter = IsNotNull("someotherfield")
    assertTrue(filterIsSafeForBootstrap(dataFieldFilter))

    val legalComplexFilter = Or(recordKeyFilter, commitTimeFilter)
    assertTrue(filterIsSafeForBootstrap(legalComplexFilter))

    val illegalComplexFilter = Or(recordKeyFilter, dataFieldFilter)
    assertFalse(filterIsSafeForBootstrap(illegalComplexFilter))

    val illegalNestedFilter = And(legalComplexFilter, illegalComplexFilter)
    assertFalse(filterIsSafeForBootstrap(illegalNestedFilter))

    val legalNestedFilter = And(legalComplexFilter, recordKeyFilter)
    assertTrue(filterIsSafeForBootstrap(legalNestedFilter))
  }

  @Test
  def testGetAppliedRequiredSchema(): Unit = {
    val fields = Array(
      StructField("column_a", LongType, nullable = false),
      StructField("column_b", StringType, nullable = false))
    val requiredSchema = StructType(fields)

    val appliedSchema: StructType = SparkFileFormatInternalRowReaderContext.getAppliedRequiredSchema(
      requiredSchema, true)
    assertEquals(3, appliedSchema.fields.length)
    assertTrue(appliedSchema.fields.map(f => f.name).contains(ROW_INDEX_TEMPORARY_COLUMN_NAME))
  }

  @Test
  def testConvertValueToEngineType(): Unit = {
    val reader = Mockito.mock(classOf[SparkColumnarFileReader])
    val stringValue = "string_value"
    val tableConfig = Mockito.mock(classOf[HoodieTableConfig])
    when(tableConfig.populateMetaFields()).thenReturn(true)
    val sparkReaderContext = new SparkFileFormatInternalRowReaderContext(reader, Seq.empty, Seq.empty, storageConf(), tableConfig)
    assertEquals(1, sparkReaderContext.getRecordContext().convertValueToEngineType(1))
    assertEquals(1L, sparkReaderContext.getRecordContext().convertValueToEngineType(1L))
    assertEquals(1.1f, sparkReaderContext.getRecordContext().convertValueToEngineType(1.1f))
    assertEquals(1.1d, sparkReaderContext.getRecordContext().convertValueToEngineType(1.1d))
    assertEquals(UTF8String.fromString(stringValue),
      sparkReaderContext.getRecordContext().convertPartitionValueToEngineType(stringValue))
  }
}
