/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the
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

package org.apache.hudi.common.index.vector;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.util.collection.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/** Performs distributed bounded candidate reduction for vector search. */
final class VectorCandidateReducer {

  private static final Logger LOG = LoggerFactory.getLogger(VectorIndexMdtSearchUtils.class);
  private static final int TOP_K_REDUCER_KEY = 0;
  private static final Comparator<ScoredVectorPostingMatch> BEST_FIRST = Comparator
      .comparingDouble(ScoredVectorPostingMatch::getApproxDistance)
      .thenComparing(ScoredVectorPostingMatch::getRecordKey);

  private VectorCandidateReducer() {
  }

  static HoodieData<ScoredVectorPostingMatch> selectTopK(HoodieData<ScoredVectorPostingMatch> candidates, int topK) {
    if (topK <= 0) {
      return HoodieListData.eager(Collections.emptyList());
    }

    HoodiePairData<Integer, List<ScoredVectorPostingMatch>> partialTopK = candidates
        .mapPartitions(iterator -> {
          long startMs = System.currentTimeMillis();
          PriorityQueue<ScoredVectorPostingMatch> localTopK = new PriorityQueue<>(topK, BEST_FIRST.reversed());
          int inputCount = 0;
          while (iterator.hasNext()) {
            inputCount++;
            offerTopK(localTopK, iterator.next(), topK);
          }
          List<ScoredVectorPostingMatch> kept = new ArrayList<>(localTopK);
          trimTopK(kept, topK);
          LOG.info("[vector_search][stage][select_topk_local] input={} kept={} topK={} elapsedMs={}",
              inputCount,
              kept.size(),
              topK,
              System.currentTimeMillis() - startMs);
          if (kept.isEmpty()) {
            return Collections.<Pair<Integer, List<ScoredVectorPostingMatch>>>emptyIterator();
          }
          return Collections.singletonList(Pair.of(TOP_K_REDUCER_KEY, kept)).iterator();
        }, true)
        .mapToPair(pair -> Pair.of(pair.getLeft(), pair.getRight()));

    return partialTopK
        .reduceByKey((left, right) -> mergeTopK(left, right, topK), 1)
        .values()
        .flatMap(List::iterator)
        .mapPartitions(iterator -> {
          long startMs = System.currentTimeMillis();
          List<ScoredVectorPostingMatch> finalTopK = new ArrayList<>();
          while (iterator.hasNext()) {
            finalTopK.add(iterator.next());
          }
          LOG.info("[vector_search][stage][select_topk_final] kept={} topK={} elapsedMs={}",
              finalTopK.size(),
              topK,
              System.currentTimeMillis() - startMs);
          return finalTopK.iterator();
        }, true);
  }

  static List<ScoredVectorPostingMatch> mergeTopK(List<ScoredVectorPostingMatch> left,
                                            List<ScoredVectorPostingMatch> right,
                                            int topK) {
    List<ScoredVectorPostingMatch> merged = new ArrayList<>(left.size() + right.size());
    merged.addAll(left);
    merged.addAll(right);
    trimTopK(merged, topK);
    return merged;
  }

  private static void trimTopK(List<ScoredVectorPostingMatch> candidates, int topK) {
    candidates.sort(BEST_FIRST);
    if (candidates.size() > topK) {
      candidates.subList(topK, candidates.size()).clear();
    }
  }

  private static void offerTopK(PriorityQueue<ScoredVectorPostingMatch> heap,
                                ScoredVectorPostingMatch candidate,
                                int topK) {
    if (heap.size() < topK) {
      heap.offer(candidate);
    } else if (BEST_FIRST.compare(candidate, heap.peek()) < 0) {
      heap.poll();
      heap.offer(candidate);
    }
  }

  static int compare(ScoredVectorPostingMatch left, ScoredVectorPostingMatch right) {
    return BEST_FIRST.compare(left, right);
  }

}
