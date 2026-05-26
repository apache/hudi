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

package org.apache.hudi

import org.apache.hudi.common.index.vector.VectorDistanceMetric
import org.apache.hudi.common.model.{FileSlice, HoodieBaseFile}
import org.apache.hudi.metadata.HoodieMetadataPayload
import org.apache.hudi.storage.{StoragePath, StoragePathInfo}

import org.junit.jupiter.api.Assertions.{assertArrayEquals, assertEquals}
import org.junit.jupiter.api.Test

class TestVectorIndexSupport {

  @Test
  def testParseQueryVectorSupportsBracketSyntax(): Unit = {
    assertArrayEquals(Array(1.0f, 2.5f, -3.0f), VectorIndexSupport.parseQueryVector("[1.0, 2.5, -3.0]"), 1e-6f)
  }

  @Test
  def testNormalizeIndexNameAcceptsLogicalIndexName(): Unit = {
    assertEquals(
      "vector_index_products_embedding_idx",
      VectorIndexSupport.normalizeIndexName("products_embedding_idx"))
  }

  @Test
  def testCollectCandidateFileNamesMatchesFileGroups(): Unit = {
    val first = new FileSlice("p1", "001", "fg-1")
    first.setBaseFile(new HoodieBaseFile(new StoragePathInfo(new StoragePath("file:///tmp/fg-1-0.parquet"), 0L, false, 0, 0, 0)))

    val second = new FileSlice("p1", "001", "fg-2")
    second.setBaseFile(new HoodieBaseFile(new StoragePathInfo(new StoragePath("file:///tmp/fg-2-0.parquet"), 0L, false, 0, 0, 0)))

    val candidateFiles = VectorIndexSupport.collectCandidateFileNames(
      Set("fg-2"),
      Seq((None, Seq(first, second))),
      includeLogFiles = false)

    assertEquals(Set("fg-2-0.parquet"), candidateFiles)
  }

  @Test
  def testResolveCurrentGenerationIdPrefersManifestRecord(): Unit = {
    val indexPartition = "vector_index_embedding_idx"
    val currentManifest = HoodieMetadataPayload.createVectorIndexManifestRecord(
      "7", "IVF_RABITQ", 1, 42L, false, 123L, indexPartition)
    val olderGeneration = HoodieMetadataPayload.createVectorIndexGenerationManifestRecord(
      "6", "IVF_RABITQ", 1, 42L, false, 100L, indexPartition)
    val newerGeneration = HoodieMetadataPayload.createVectorIndexGenerationManifestRecord(
      "8", "IVF_RABITQ", 1, 42L, false, 200L, indexPartition)

    assertEquals(Some(7), VectorIndexSupport.resolveCurrentGenerationId(Seq(olderGeneration, newerGeneration, currentManifest)))
  }

  @Test
  def testBuildClusterMapFromClusterRecordsUsesExactClusterRows(): Unit = {
    val indexPartition = "vector_index_embedding_idx"
    val clusterThree = HoodieMetadataPayload.createVectorIndexClusterManifestRecord(
      "7", 3, 2, java.util.Arrays.asList("fg-1", "fg-2"), 100L, 123L, indexPartition)
    val clusterFive = HoodieMetadataPayload.createVectorIndexClusterManifestRecord(
      "7", 5, 1, java.util.Arrays.asList("fg-9"), 50L, 123L, indexPartition)

    val clusterMap = VectorIndexSupport.buildClusterMapFromClusterRecords(Seq(clusterThree, clusterFive), None)

    assertEquals(Set("fg-1", "fg-2"), clusterMap(3))
    assertEquals(Set("fg-9"), clusterMap(5))
  }

  @Test
  def testFindTopClustersMatchesQueryMetric(): Unit = {
    val centroids = Array(Array(0.0f, 0.0f), Array(5.0f, 0.0f), Array(9.0f, 0.0f))
    val topClusters = VectorIndexSupport.findTopClusters(
      centroids,
      Array(6.0f, 0.0f),
      2,
      VectorDistanceMetric.L2)

    assertEquals(Seq(1, 2), topClusters)
  }
}
