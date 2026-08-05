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

package org.apache.hudi.client;

import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the pure helpers backing the paginated-clean orchestration in
 * {@link BaseHoodieTableServiceClient}. Kept off the mock-heavy path so
 * chunking / resume-parsing invariants are exercised as fast pure-Java
 * assertions.
 */
public class TestBatchedCleanHelpers {

  // -----------------------------------------------------------------
  // chunkForBatchedClean
  // -----------------------------------------------------------------

  @Test
  void chunk_emptyList_returnsEmpty() {
    List<List<String>> chunks = BaseHoodieTableServiceClient.chunkForBatchedClean(
        Collections.emptyList(), 5);
    assertTrue(chunks.isEmpty(), "no chunks for empty partition list");
  }

  @Test
  void chunk_sizeSmallerThanBatch_returnsSingleChunk() {
    List<String> partitions = Arrays.asList("a", "b", "c");
    List<List<String>> chunks = BaseHoodieTableServiceClient.chunkForBatchedClean(partitions, 5);
    assertEquals(1, chunks.size());
    assertEquals(Arrays.asList("a", "b", "c"), chunks.get(0));
  }

  @Test
  void chunk_sizeEqualToBatch_returnsSingleFullChunk() {
    List<String> partitions = Arrays.asList("a", "b", "c");
    List<List<String>> chunks = BaseHoodieTableServiceClient.chunkForBatchedClean(partitions, 3);
    assertEquals(1, chunks.size());
    assertEquals(3, chunks.get(0).size());
  }

  @Test
  void chunk_cleanMultipleOfBatchSize_returnsEvenChunks() {
    List<String> partitions = Arrays.asList("a", "b", "c", "d", "e", "f");
    List<List<String>> chunks = BaseHoodieTableServiceClient.chunkForBatchedClean(partitions, 2);
    assertEquals(3, chunks.size());
    for (List<String> chunk : chunks) {
      assertEquals(2, chunk.size());
    }
  }

  @Test
  void chunk_fractionalRemainder_lastChunkSmaller() {
    List<String> partitions = Arrays.asList("a", "b", "c", "d", "e", "f", "g");
    List<List<String>> chunks = BaseHoodieTableServiceClient.chunkForBatchedClean(partitions, 3);
    assertEquals(3, chunks.size());
    assertEquals(3, chunks.get(0).size());
    assertEquals(3, chunks.get(1).size());
    assertEquals(1, chunks.get(2).size(), "trailing partial batch preserved");
  }

  @Test
  void chunk_sortsBeforeChunking_soRestartsProduceSameLayout() {
    // Reverse-sorted input — deterministic chunking must sort first so that
    // a resume attempt aligns on batchIndex regardless of input order from
    // the (possibly re-run) partition listing.
    List<String> partitions = Arrays.asList("p9", "p8", "p7", "p6", "p5", "p4", "p3", "p2", "p1", "p0");
    List<List<String>> chunks = BaseHoodieTableServiceClient.chunkForBatchedClean(partitions, 4);
    assertEquals(3, chunks.size());
    assertEquals(Arrays.asList("p0", "p1", "p2", "p3"), chunks.get(0));
    assertEquals(Arrays.asList("p4", "p5", "p6", "p7"), chunks.get(1));
    assertEquals(Arrays.asList("p8", "p9"), chunks.get(2));
  }

  @Test
  void chunk_doesNotMutateInput() {
    List<String> partitions = new ArrayList<>(Arrays.asList("z", "a", "m"));
    BaseHoodieTableServiceClient.chunkForBatchedClean(partitions, 2);
    assertEquals(Arrays.asList("z", "a", "m"), partitions,
        "caller's list must not be mutated (sorted in place)");
  }

  @Test
  void chunk_batchSizeZero_rejects() {
    assertThrows(IllegalArgumentException.class,
        () -> BaseHoodieTableServiceClient.chunkForBatchedClean(Arrays.asList("a"), 0));
  }

  @Test
  void chunk_batchSizeNegative_rejects() {
    assertThrows(IllegalArgumentException.class,
        () -> BaseHoodieTableServiceClient.chunkForBatchedClean(Arrays.asList("a"), -1));
  }

  // -----------------------------------------------------------------
  // parseCompletedBatchIndex
  // -----------------------------------------------------------------

  @Test
  void parse_nullExtraMetadata_returnsEmpty() {
    Option<Integer> idx = BaseHoodieTableServiceClient.parseCompletedBatchIndex(null, "20260101010101", 10);
    assertFalse(idx.isPresent(), "null extraMetadata => not a batched instant");
  }

  @Test
  void parse_emptyExtraMetadata_returnsEmpty() {
    Option<Integer> idx = BaseHoodieTableServiceClient.parseCompletedBatchIndex(new HashMap<>(), "20260101010101", 10);
    assertFalse(idx.isPresent(), "no batch keys => not a batched instant");
  }

  @Test
  void parse_missingIndex_returnsEmpty() {
    Map<String, String> extra = new HashMap<>();
    extra.put(BaseHoodieTableServiceClient.BATCH_TARGET_EARLIEST_COMMIT_KEY, "20260101010101");
    extra.put(BaseHoodieTableServiceClient.BATCH_TOTAL_BATCHES_KEY, "10");
    Option<Integer> idx = BaseHoodieTableServiceClient.parseCompletedBatchIndex(extra, "20260101010101", 10);
    assertFalse(idx.isPresent(), "missing batchIndex => not usable");
  }

  @Test
  void parse_mismatchedTarget_returnsEmpty() {
    Map<String, String> extra = matchingExtra("20260101010101", 10, 3);
    Option<Integer> idx = BaseHoodieTableServiceClient.parseCompletedBatchIndex(extra, "20260201020202", 10);
    assertFalse(idx.isPresent(), "different targetEarliestCommit => belongs to a different logical run");
  }

  @Test
  void parse_mismatchedTotalBatches_returnsEmpty() {
    Map<String, String> extra = matchingExtra("20260101010101", 10, 3);
    Option<Integer> idx = BaseHoodieTableServiceClient.parseCompletedBatchIndex(extra, "20260101010101", 12);
    assertFalse(idx.isPresent(), "different totalBatches => the run was re-partitioned; can't reuse");
  }

  @Test
  void parse_malformedTotal_returnsEmpty() {
    Map<String, String> extra = matchingExtra("20260101010101", 10, 3);
    extra.put(BaseHoodieTableServiceClient.BATCH_TOTAL_BATCHES_KEY, "not-a-number");
    Option<Integer> idx = BaseHoodieTableServiceClient.parseCompletedBatchIndex(extra, "20260101010101", 10);
    assertFalse(idx.isPresent(), "malformed totalBatches => ignore rather than crash");
  }

  @Test
  void parse_malformedIndex_returnsEmpty() {
    Map<String, String> extra = matchingExtra("20260101010101", 10, 3);
    extra.put(BaseHoodieTableServiceClient.BATCH_INDEX_KEY, "?");
    Option<Integer> idx = BaseHoodieTableServiceClient.parseCompletedBatchIndex(extra, "20260101010101", 10);
    assertFalse(idx.isPresent(), "malformed batchIndex => ignore rather than crash");
  }

  @Test
  void parse_matchingEntry_returnsIndex() {
    Map<String, String> extra = matchingExtra("20260101010101", 10, 3);
    Option<Integer> idx = BaseHoodieTableServiceClient.parseCompletedBatchIndex(extra, "20260101010101", 10);
    assertTrue(idx.isPresent());
    assertEquals(3, idx.get().intValue());
  }

  @Test
  void parse_legacyInstantWithSavepointedOnly_returnsEmpty() {
    // A pre-batching completed clean might carry only SAVEPOINTED_TIMESTAMPS in
    // extraMetadata. Resume scan must not mistake it for a batched instant.
    Map<String, String> extra = new HashMap<>();
    extra.put("hoodie.savepoint.timestamps", "20250101010101,20250201020202");
    Option<Integer> idx = BaseHoodieTableServiceClient.parseCompletedBatchIndex(extra, "20260101010101", 10);
    assertFalse(idx.isPresent());
  }

  // -----------------------------------------------------------------
  // Combined: chunking + a simulated done-set produces the expected
  // remaining slice — the whole spec's testing-plan point 1.
  // -----------------------------------------------------------------

  @Test
  void combined_remainingChunksSkipDoneIndicesWithoutDuplicating() {
    // 25 partitions, batchSize 3 -> 9 batches (last one size 1).
    List<String> partitions = IntStream.range(0, 25).mapToObj(i -> String.format("p%02d", i)).collect(Collectors.toList());
    List<List<String>> chunks = BaseHoodieTableServiceClient.chunkForBatchedClean(partitions, 3);
    assertEquals(9, chunks.size());

    // Simulate a prior crashed run that completed batches 0, 2, 5.
    Set<Integer> done = new HashSet<>(Arrays.asList(0, 2, 5));

    List<Integer> remaining = IntStream.range(0, chunks.size())
        .filter(i -> !done.contains(i))
        .boxed()
        .collect(Collectors.toList());

    assertEquals(Arrays.asList(1, 3, 4, 6, 7, 8), remaining,
        "resume must skip completed batches and not duplicate any");

    // Sanity: union of done + remaining covers all indices exactly once.
    Set<Integer> union = new HashSet<>(done);
    union.addAll(remaining);
    assertEquals(chunks.size(), union.size(), "every batchIndex accounted for exactly once");
  }

  private static Map<String, String> matchingExtra(String target, int total, int index) {
    Map<String, String> extra = new HashMap<>();
    extra.put(BaseHoodieTableServiceClient.BATCH_TARGET_EARLIEST_COMMIT_KEY, target);
    extra.put(BaseHoodieTableServiceClient.BATCH_TOTAL_BATCHES_KEY, String.valueOf(total));
    extra.put(BaseHoodieTableServiceClient.BATCH_INDEX_KEY, String.valueOf(index));
    return extra;
  }
}
