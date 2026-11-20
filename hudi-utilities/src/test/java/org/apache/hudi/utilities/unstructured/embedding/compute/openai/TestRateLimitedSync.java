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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.unstructured.embedding.ComputeRoundCounters;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingClientConfigException;
import org.apache.hudi.utilities.unstructured.embedding.compute.RequestRateLimiter;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static org.apache.hudi.utilities.unstructured.embedding.compute.openai.StubEmbeddingServer.embeddings;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Rate-limit sync cases through one prep_and_compute writer, with exact fake-clock waits. */
@Tag("functional")
class TestRateLimitedSync extends OpenAiRoundTestBase {

  @Test
  void testLimitIsPerModelBindingAndCountersShowItsWait() throws Exception {
    announce(pdf("one"), pdf("two"));
    List<Long> limitedArrivals = new ArrayList<>();
    List<Long> freeArrivals = new ArrayList<>();
    List<String> limitedInputs = new ArrayList<>();
    List<String> freeInputs = new ArrayList<>();
    server.otherwise(PATH, request -> {
      boolean limited = "text-embedding-3-small".equals(request.json().path("model").asText());
      (limited ? limitedArrivals : freeArrivals).add(FakeClockOpenAiClient.CLOCK.get());
      (limited ? limitedInputs : freeInputs).addAll(request.inputs());
      return embeddings(DIM).apply(request);
    });
    // The writer fills one active model. Independent model bindings use independent target tables.
    ComputeRoundCounters.Counted limited = ComputeRoundCounters.round(spark(), paced(table("limited"), "5"));
    TypedProperties freeProps = paced(table("free"), null);
    freeProps.setProperty(PREFIX + EmbeddingTableConfig.MODEL_NAME_SUFFIX, "text-embedding-3-large");
    ComputeRoundCounters.Counted free = ComputeRoundCounters.round(spark(), freeProps);
    assertEquals(6, limited.counters.rowsEmbedded());
    assertEquals(6, free.counters.rowsEmbedded());
    assertEquals(1000, limited.counters.rateLimitWaitMillis());
    assertEquals(0, free.counters.rateLimitWaitMillis());
    assertEquals(6, new HashSet<>(limitedArrivals).size());
    assertEquals(1, new HashSet<>(freeArrivals).size());
    assertEquals(limitedInputs, freeInputs);
    assertEquals(1, limited.round.getCompleted().size());
    assertEquals(1, free.round.getCompleted().size());
  }

  @Test
  void testRateChangesApplyToNewRoundsWithoutReembeddingEarlierRows() throws Exception {
    server.otherwise(PATH, embeddings(DIM));
    announce(pdf("one"));
    assertEquals(0, ComputeRoundCounters.round(spark(), paced(table(), null)).counters.rateLimitWaitMillis());
    announce(pdf("two"));
    assertEquals(400, ComputeRoundCounters.round(spark(), paced(table(), "5")).counters.rateLimitWaitMillis());
    announce(pdf("three"));
    assertEquals(800, ComputeRoundCounters.round(spark(), paced(table(), "2.5")).counters.rateLimitWaitMillis());
    announce(pdf("four"));
    assertEquals(0, ComputeRoundCounters.round(spark(), paced(table(), null)).counters.rateLimitWaitMillis());
    assertEquals(12, server.posts().size(), "only the three new chunks of each round are sent");
    assertEquals(12, rows(table()).size());
    assertEquals(4, EmbeddingStreamers.commits(table(), storage()).size());
    rows(table()).values().forEach(row -> assertNotNull(vector(row)));
  }

  @Test
  void testBadRateFailsTheSyncAtStart() throws Exception {
    announce(pdf("bad-rate"));
    Exception failure = assertThrows(Exception.class, () -> round(paced(table(), "0")));
    assertTrue(EmbeddingStreamers.causeOf(failure, EmbeddingClientConfigException.class).isPresent(), EmbeddingStreamers.causes(failure));
    assertTrue(EmbeddingStreamers.causes(failure).contains(PREFIX + RequestRateLimiter.RATE_LIMIT_REQUESTS_PER_SECOND_SUFFIX));
    assertTrue(EmbeddingStreamers.commits(table(), storage()).isEmpty());
    assertEquals(0, server.requests().size());
  }

  private TypedProperties paced(String target, String rate) {
    TypedProperties props = providerProps(target,
        PREFIX + EmbeddingTableConfig.MODEL_CLIENT_CLASS_SUFFIX, FakeClockOpenAiClient.class.getName(),
        EmbeddingTableConfig.COMPUTE_BATCH_SIZE.key(), "1");
    if (rate != null) {
      props.setProperty(PREFIX + RequestRateLimiter.RATE_LIMIT_REQUESTS_PER_SECOND_SUFFIX, rate);
    }
    return props;
  }
}
