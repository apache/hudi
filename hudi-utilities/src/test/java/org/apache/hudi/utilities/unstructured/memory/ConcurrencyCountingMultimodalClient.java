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

package org.apache.hudi.utilities.unstructured.memory;

import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.compute.DummyMultimodalEmbeddingClient;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingInput;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingResult;

import org.apache.spark.TaskContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The dummy multimodal client (each image input decoded and resized in memory, as any multimodal client's), embedding one input
 * at a time and counting, per compute task (its partition id), the image inputs it embedded and the ones that failed, and the
 * most image preparations in flight at once across the tasks of the JVM. Local Spark runs every task in the memory cell's child
 * JVM, which prints the counts after its rounds.
 */
public class ConcurrencyCountingMultimodalClient extends DummyMultimodalEmbeddingClient {

  private static final Map<Integer, AtomicLong> IMAGES_OK = new ConcurrentHashMap<>();
  private static final AtomicLong IMAGES_FAILED = new AtomicLong();
  private static final AtomicInteger IN_FLIGHT = new AtomicInteger();
  private static final AtomicInteger MAX_IN_FLIGHT = new AtomicInteger();
  /** Start and end (System.nanoTime) of every image input that came back OK, to count only overlapping successful decodes. */
  private static final List<long[]> OK_INTERVALS = Collections.synchronizedList(new ArrayList<>());

  @Override
  public List<EmbeddingResult> embed(List<EmbeddingInput> inputs) {
    TaskContext task = TaskContext.get();
    int partition = task == null ? -1 : task.partitionId();
    List<EmbeddingResult> results = new ArrayList<>(inputs.size());
    for (EmbeddingInput input : inputs) {
      if (!EmbeddingTableSchema.MODALITY_IMAGE.equals(input.getModality())) {
        results.addAll(super.embed(Collections.singletonList(input)));
        continue;
      }
      int inFlight = IN_FLIGHT.incrementAndGet();
      MAX_IN_FLIGHT.accumulateAndGet(inFlight, Math::max);
      long start = System.nanoTime();
      try {
        EmbeddingResult result = super.embed(Collections.singletonList(input)).get(0);
        if (result.getStatus() == EmbeddingResult.Status.OK) {
          OK_INTERVALS.add(new long[] {start, System.nanoTime()});
          IMAGES_OK.computeIfAbsent(partition, p -> new AtomicLong()).incrementAndGet();
        } else {
          IMAGES_FAILED.incrementAndGet();
        }
        results.add(result);
      } finally {
        IN_FLIGHT.decrementAndGet();
      }
    }
    return results;
  }

  /** Image inputs embedded per compute task so far, by partition id. */
  public static Map<Integer, Long> imagesOk() {
    Map<Integer, Long> counts = new TreeMap<>();
    IMAGES_OK.forEach((partition, n) -> counts.put(partition, n.get()));
    return counts;
  }

  /** Image inputs whose preparation or embedding failed so far. */
  public static long imagesFailed() {
    return IMAGES_FAILED.get();
  }

  /** The most image inputs prepared at once so far, refused ones included. */
  public static int maxInFlight() {
    return MAX_IN_FLIGHT.get();
  }

  /** The most image inputs that came back OK whose preparation and embedding overlapped in time: refused inputs do not count. */
  public static int maxOkInFlight() {
    List<long[]> events = new ArrayList<>();
    synchronized (OK_INTERVALS) {
      for (long[] interval : OK_INTERVALS) {
        events.add(new long[] {interval[0], 1});
        events.add(new long[] {interval[1], -1});
      }
    }
    // an end sorts before a start at the same instant, so touching intervals do not count as overlapping
    events.sort((a, b) -> a[0] != b[0] ? Long.compare(a[0], b[0]) : Long.compare(a[1], b[1]));
    int current = 0;
    int max = 0;
    for (long[] event : events) {
      current += (int) event[1];
      max = Math.max(max, current);
    }
    return max;
  }
}
