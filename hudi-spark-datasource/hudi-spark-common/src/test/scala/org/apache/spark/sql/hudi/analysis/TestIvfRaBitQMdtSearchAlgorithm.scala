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

package org.apache.spark.sql.hudi.analysis

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class TestIvfRaBitQMdtSearchAlgorithm {

  @Test
  def testBuildFileGroupFetchBatchesCreatesPositionLocalBatches(): Unit = {
    val candidates = Seq(
      Candidate("fg-b", 30L),
      Candidate("fg-a", 30L),
      Candidate("fg-b", 10L),
      Candidate("fg-a", 10L),
      Candidate("fg-b", 20L),
      Candidate("fg-a", 20L))

    val batches = IvfRaBitQMdtSearchAlgorithm
      .buildFileGroupFetchBatches(candidates, targetBatchCount = 4)(_.fileGroupId, _.position)

    assertEquals(Seq("fg-a", "fg-a", "fg-b", "fg-b"), batches.map(_._1))
    assertEquals(Seq(Seq(10L, 20L), Seq(30L), Seq(10L, 20L), Seq(30L)),
      batches.map(_._2.map(_.position)))
  }

  @Test
  def testBuildFileGroupFetchBatchesHandlesNoCandidates(): Unit = {
    val batches = IvfRaBitQMdtSearchAlgorithm
      .buildFileGroupFetchBatches(Seq.empty[Candidate], targetBatchCount = 4)(_.fileGroupId, _.position)

    assertEquals(Seq.empty, batches)
  }

  private case class Candidate(fileGroupId: String, position: Long)
}
