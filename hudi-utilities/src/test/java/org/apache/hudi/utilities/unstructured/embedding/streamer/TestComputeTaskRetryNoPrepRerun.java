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
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers.Round;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.FixtureFile;
import org.apache.hudi.utilities.unstructured.embedding.compute.ScriptedEmbeddingClient;
import org.apache.hudi.utilities.unstructured.embedding.prep.TextPipePreprocessor;

import org.apache.spark.SparkConf;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A failed compute task is retried by Spark alone: the round's output equals a run without the failure, has no duplicate key,
 * and prep runs once per file (its output is persisted and counted before the shuffle, whose map side reads that cache).
 * Spark runs {@code local[4,2]} here, so a task gets a second attempt.
 */
@Tag("functional")
public class TestComputeTaskRetryNoPrepRerun extends EmbeddingRoundTestBase {

  private static final int FILES = 6;

  @Override
  public SparkConf conf() {
    return conf(Collections.singletonMap("spark.master", "local[4,2]"));
  }

  @Test
  public void failedComputeTaskIsRetriedAloneWithTheSameOutputAndNoPrepRerun() throws Exception {
    FixtureFile[] files = new FixtureFile[FILES];
    for (int i = 0; i < FILES; i++) {
      files[i] = text("f" + i, "one " + i, "two " + i, "three " + i);
    }
    announce(files);

    Round clean = round(props(table("clean"), EmbeddingTableConfig.MODE_PREP_AND_COMPUTE,
        EmbeddingTableConfig.COMPUTE_PARALLELISM.key(), "4"));
    assertEquals(FILES, TextPipePreprocessor.OPENED.get());
    assertEquals(0, ScriptedEmbeddingClient.TASK_FAILURES.get());

    int cleanCalls = ScriptedEmbeddingClient.CALLS.getAndSet(0);
    TextPipePreprocessor.OPENED.set(0);
    ScriptedEmbeddingClient.FAIL_FIRST_ATTEMPT_OF_PARTITION.set(0);
    Round faulted = round(props(table("faulted"), EmbeddingTableConfig.MODE_PREP_AND_COMPUTE,
        EmbeddingTableConfig.COMPUTE_PARALLELISM.key(), "4"));

    assertEquals(1, ScriptedEmbeddingClient.TASK_FAILURES.get(), "the first attempt of compute task 0 failed once");
    assertEquals(FILES, TextPipePreprocessor.OPENED.get(), "prep ran once per file: the compute retry did not re-run it");
    assertEquals(cleanCalls + 1, ScriptedEmbeddingClient.CALLS.get(),
        "only the failed task ran again: one more model call than the clean round (the call that failed)");
    assertTrue(faulted.getCommit().isPresent(), faulted.toString());
    assertEquals(clean.getRowsWritten(), faulted.getRowsWritten());
    assertEquals(3 * FILES, rows(table("faulted")).size(), "no duplicate key");
    assertEquals(describe(rows(table("clean"))), describe(rows(table("faulted"))), "identical output");
  }
}
