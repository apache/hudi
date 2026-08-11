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

package org.apache.hudi.common.index.vector.search;

import org.apache.hudi.avro.model.HoodieVectorIndexPostingBlock;
import org.apache.hudi.common.index.vector.MetricQueryState;
import org.apache.hudi.common.index.vector.PostingBlockBuilder;
import org.apache.hudi.common.index.vector.VectorDistanceMetric;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestPackedPostingBlockScorer {

  @Test
  void retainsBestRowsAndDecodesOnlyTheirLocators() {
    byte[] aligned = new byte[Long.BYTES];
    aligned[0] = 1;
    byte[] orthogonal = new byte[Long.BYTES];
    orthogonal[0] = 2;
    HoodieVectorIndexPostingBlock block = new PostingBlockBuilder(Long.BYTES, 0)
        .addRow("orthogonal", orthogonal, new byte[0],
            0f, 1f, 0f, 0f, 1f, 1f, "fg-old", "001", "p", 3)
        .addRow("aligned", aligned, new byte[0],
            0f, 1f, 0f, 0f, 1f, 1f, "fg-best", "002", "p", 7)
        .build();
    MetricQueryState query = MetricQueryState.create(
        VectorDistanceMetric.L2, vector -> vector, new float[] {1f, 0f}, false);

    List<VectorCandidate> candidates = PackedPostingBlockScorer.score(
        block, query, new float[] {0f, 0f}, 2, 1, 1,
        4, 2, 3, 9L);

    assertEquals(1, candidates.size());
    assertEquals("aligned", candidates.get(0).getRecordKey());
    assertEquals("fg-best", candidates.get(0).getPostingLocator().getFileId());
    assertEquals(7L, candidates.get(0).getPostingLocator().getRowPosition());
    assertEquals(9L, candidates.get(0).getPostingLocator().getBlockId());
  }
}
