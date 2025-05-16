/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi.command

import org.apache.hudi.common.model.HoodieCommitMetadata

import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.mockito.Mockito.{mock, when}

class TestHoodieCommandMetrics extends HoodieSparkSqlTestBase {

  override def beforeAll(): Unit = {
    spark.sparkContext
  }

  test("test HoodieCommandMetrics metrics") {
    val metrics = HoodieCommandMetrics.metrics
    assert(metrics != null)
    assert(metrics.size == 10)

    HoodieCommandMetrics.updateCommitMetrics(metrics, mockedCommitMetadata())
    assertResult(1L)(metrics(HoodieCommandMetrics.NUM_PARTITION_KEY).value)
    assertResult(2L)(metrics(HoodieCommandMetrics.NUM_INSERT_FILE_KEY).value)
    assertResult(3L)(metrics(HoodieCommandMetrics.NUM_UPDATE_FILE_KEY).value)
    assertResult(4L)(metrics(HoodieCommandMetrics.NUM_WRITE_ROWS_KEY).value)
    assertResult(5L)(metrics(HoodieCommandMetrics.NUM_UPDATE_ROWS_KEY).value)
    assertResult(6L)(metrics(HoodieCommandMetrics.NUM_INSERT_ROWS_KEY).value)
    assertResult(7L)(metrics(HoodieCommandMetrics.NUM_DELETE_ROWS_KEY).value)
    assertResult(8L)(metrics(HoodieCommandMetrics.NUM_OUTPUT_BYTES_KEY).value)
    assertResult(9L)(metrics(HoodieCommandMetrics.INSERT_TIME).value)
    assertResult(10L)(metrics(HoodieCommandMetrics.UPSERT_TIME).value)
  }


  private def mockedCommitMetadata(): HoodieCommitMetadata = {
    val metadata: HoodieCommitMetadata = mock(classOf[HoodieCommitMetadata])
    when(metadata.fetchTotalPartitionsWritten()).thenReturn(1L)
    when(metadata.fetchTotalFilesInsert()).thenReturn(2L)
    when(metadata.fetchTotalFilesUpdated()).thenReturn(3L)
    when(metadata.fetchTotalRecordsWritten()).thenReturn(4L)
    when(metadata.fetchTotalUpdateRecordsWritten()).thenReturn(5L)
    when(metadata.fetchTotalInsertRecordsWritten()).thenReturn(6L)
    when(metadata.getTotalRecordsDeleted()).thenReturn(7L)
    when(metadata.fetchTotalBytesWritten()).thenReturn(8L)
    when(metadata.getTotalCreateTime()).thenReturn(9L)
    when(metadata.getTotalUpsertTime()).thenReturn(10L)
    metadata
  }
}
