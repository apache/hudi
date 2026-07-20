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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi.analysis

import org.apache.hudi.metadata.HoodieTableMetadata

import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertSame}
import org.mockito.Mockito.{mock, times, verify}

class TestIvfRaBitQMdtSearchAlgorithm {

  @BeforeEach
  def resetCachesBeforeTest(): Unit = {
    IvfRaBitQMdtSearchAlgorithm.resetMetadataCaches()
  }

  @AfterEach
  def resetCachesAfterTest(): Unit = {
    IvfRaBitQMdtSearchAlgorithm.resetMetadataCaches()
  }

  @Test
  def testApproximateCandidateHeapUsesSmallSafetyFloor(): Unit = {
    assertEquals(32, IvfRaBitQMdtSearchAlgorithm.approximateCandidateHeapSize(10))
  }

  @Test
  def testApproximateCandidateHeapDoesNotCapLargeTopK(): Unit = {
    assertEquals(50, IvfRaBitQMdtSearchAlgorithm.approximateCandidateHeapSize(50))
  }

  @Test
  def testMetadataTableIsReusedForSameSnapshot(): Unit = {
    val metadataTable = mock(classOf[HoodieTableMetadata])
    var loads = 0

    val first = IvfRaBitQMdtSearchAlgorithm.getOrCreateMetadataTable(
      "table", "application", 1, "001") {
      loads += 1
      metadataTable
    }
    val second = IvfRaBitQMdtSearchAlgorithm.getOrCreateMetadataTable(
      "table", "application", 1, "001") {
      loads += 1
      mock(classOf[HoodieTableMetadata])
    }

    assertSame(metadataTable, first)
    assertSame(first, second)
    assertEquals(1, loads)
    assertEquals(1, IvfRaBitQMdtSearchAlgorithm.metadataTableCacheSize)
  }

  @Test
  def testMetadataTableIsReplacedWhenSnapshotAdvances(): Unit = {
    val oldMetadataTable = mock(classOf[HoodieTableMetadata])
    val newMetadataTable = mock(classOf[HoodieTableMetadata])
    IvfRaBitQMdtSearchAlgorithm.getOrCreateMetadataTable(
      "table", "application", 1, "001")(oldMetadataTable)

    val refreshed = IvfRaBitQMdtSearchAlgorithm.getOrCreateMetadataTable(
      "table", "application", 1, "002")(newMetadataTable)

    assertSame(newMetadataTable, refreshed)
    assertEquals(1, IvfRaBitQMdtSearchAlgorithm.metadataTableCacheSize)
    verify(oldMetadataTable, times(1)).close()
  }
}
