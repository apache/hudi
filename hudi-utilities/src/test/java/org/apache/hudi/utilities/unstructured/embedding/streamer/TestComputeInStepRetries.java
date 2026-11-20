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

package org.apache.hudi.utilities.unstructured.embedding.streamer;

import org.apache.hudi.utilities.unstructured.embedding.EmbeddingRoundTestBase;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers.Round;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.compute.ScriptedEmbeddingClient;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Provider retries inside the compute step ({@code compute.max.attempts}, {@code compute.retry.backoff.ms}): a
 * retryable answer is tried again and the round commits; an input still retryable after its last try fails the round, which then
 * commits nothing.
 */
@Tag("functional")
public class TestComputeInStepRetries extends EmbeddingRoundTestBase {

  @Test
  public void retryableAnswerIsRetriedInsideTheStepAndTheRoundCommits() throws Exception {
    announce(text("a", "alpha", "beta"));
    ScriptedEmbeddingClient.RETRYABLE.set(1);

    Round round = round(props(table(), EmbeddingTableConfig.MODE_PREP_AND_COMPUTE,
        EmbeddingTableConfig.COMPUTE_MAX_ATTEMPTS.key(), "3", EmbeddingTableConfig.COMPUTE_PARALLELISM.key(), "1"));

    assertEquals(2, round.getRowsWritten(), round.toString());
    assertTrue(rows(table()).values().stream().allMatch(row -> vector(row) != null));
  }

  @Test
  public void anInputStillRetryableAfterItsLastTryFailsTheRound() throws Exception {
    announce(text("a", "alpha", "beta"));
    ScriptedEmbeddingClient.RETRYABLE.set(Integer.MAX_VALUE);

    Exception failure = assertThrows(Exception.class, () -> round(props(table(), EmbeddingTableConfig.MODE_PREP_AND_COMPUTE,
        EmbeddingTableConfig.COMPUTE_MAX_ATTEMPTS.key(), "2")));

    assertTrue(EmbeddingStreamers.causes(failure).contains("EMBED-COMPUTE-RETRIES-EXHAUSTED"), EmbeddingStreamers.causes(failure));
    assertTrue(EmbeddingStreamers.commits(table(), storage()).isEmpty(), "no commit");
  }
}
