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

package org.apache.hudi.utilities.unstructured.embedding.compute.openai;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Packs the inputs of one embed() call, in order, into provider requests that respect both the provider's per-request
 * input count and its summed token budget (OpenAI: 2048 inputs and 300,000 tokens per request).
 */
final class RequestPlanner {

  private RequestPlanner() {
  }

  /**
   * @param tokenCounts token count (or upper bound) of each input, in call order
   * @param maxInputs   most inputs in one request, &gt; 0
   * @param maxTokens   most summed tokens in one request, &gt; 0; an input above it is sent alone
   * @return consecutive {@code [from, to)} index ranges covering every input once, in order
   */
  static List<int[]> plan(int[] tokenCounts, int maxInputs, long maxTokens) {
    if (tokenCounts.length == 0) {
      return Collections.emptyList();
    }
    List<int[]> requests = new ArrayList<>();
    int from = 0;
    long tokens = 0;
    for (int i = 0; i < tokenCounts.length; i++) {
      boolean full = i - from == maxInputs || (i > from && tokens + tokenCounts[i] > maxTokens);
      if (full) {
        requests.add(new int[] {from, i});
        from = i;
        tokens = 0;
      }
      tokens += tokenCounts[i];
    }
    requests.add(new int[] {from, tokenCounts.length});
    return requests;
  }
}
