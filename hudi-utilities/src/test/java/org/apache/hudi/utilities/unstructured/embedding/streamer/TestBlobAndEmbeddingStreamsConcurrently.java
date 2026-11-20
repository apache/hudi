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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.sources.blob.BlobJobHarness;
import org.apache.hudi.utilities.sources.blob.BlobTableSchema;
import org.apache.hudi.utilities.streamer.HoodieStreamer;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingRoundTestBase;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.FixtureFile;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingProviderUnavailableException;
import org.apache.hudi.utilities.unstructured.embedding.compute.ScriptedEmbeddingClient;

import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A blob stream and an embedding stream on ONE S3 events table, run at the same time: two streamer jobs, two tables, each with
 * its own stock checkpoint over the events table. Both tables end complete, and neither stream affects the other: the embedding
 * stream reads no blob table, and while its provider is down the blob stream keeps committing; the embedding stream then
 * catches up from its own checkpoint.
 */
@Tag("functional")
public class TestBlobAndEmbeddingStreamsConcurrently extends EmbeddingRoundTestBase {

  /** Objects above this many bytes are out of line, in both jobs. */
  private static final long INLINE_CEILING = 40;

  @Override
  protected Map<String, String> sourceEventsWriteOptions() {
    // The two readers share one events table; avoid Spark column-stat cache lock inversion.
    return Collections.singletonMap("hoodie.metadata.index.column.stats.enable", "false");
  }

  private String blobTable() {
    return table("blob_table");
  }

  private Callable<Void> blobRound() throws Exception {
    TypedProperties props = objects().jobProperties(INLINE_CEILING);
    return () -> {
      HoodieStreamer streamer = new HoodieStreamer(BlobJobHarness.streamerConfig(blobTable()), jsc(), Option.of(props));
      try {
        streamer.sync();
      } finally {
        streamer.shutdownGracefully();
      }
      return null;
    };
  }

  private Callable<Void> embeddingRound() {
    TypedProperties props = props(table(), EmbeddingTableConfig.MODE_PREP_AND_COMPUTE, INLINE_CEILING);
    // the shared Spark catalog cache is cleared once both rounds are done, not while the blob round still runs
    return () -> {
      EmbeddingStreamers.round(spark(), props, cfg -> { }, false);
      return null;
    };
  }

  /** Runs both rounds at once; returns their outcomes, blob first. */
  private List<Future<Void>> together(Callable<Void> blob, Callable<Void> embedding) throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      List<Future<Void>> outcomes = pool.invokeAll(Arrays.asList(blob, embedding), 10, TimeUnit.MINUTES);
      return outcomes;
    } finally {
      pool.shutdownNow();
      spark().catalog().clearCache();
    }
  }

  private Set<String> blobObjects() {
    return spark().read().format("hudi").load(blobTable()).collectAsList().stream()
        .map(row -> row.<String>getAs(BlobTableSchema.SOURCE_FILE_NAME)).collect(Collectors.toCollection(TreeSet::new));
  }

  private Set<String> embeddedObjects() {
    return rows(table()).values().stream().map(row -> row.<String>getAs(EmbeddingTableSchema.SOURCE_FILE_NAME))
        .collect(Collectors.toCollection(TreeSet::new));
  }

  private static Set<String> names(FixtureFile... files) {
    return Arrays.stream(files).map(FixtureFile::fileName).collect(Collectors.toCollection(TreeSet::new));
  }

  private String lastCheckpoint(String table) {
    List<HoodieInstant> commits = EmbeddingStreamers.commits(table, storage());
    return EmbeddingStreamers.checkpoint(table, storage(), commits.get(commits.size() - 1));
  }

  @Test
  public void bothStreamsCompleteWithTheirOwnCheckpointsAndNeitherAffectsTheOther() throws Exception {
    FixtureFile a = text("a", "alpha");
    FixtureFile b = text("b", "a long first chunk of text", "a long second chunk of text");
    announce(a, b);
    for (Future<Void> outcome : together(blobRound(), embeddingRound())) {
      outcome.get();
    }
    assertEquals(names(a, b), blobObjects());
    assertEquals(names(a, b), embeddedObjects());
    assertEquals(latestEventsCheckpoint(), lastCheckpoint(blobTable()));
    assertEquals(latestEventsCheckpoint(), lastCheckpoint(table()));

    // the embedding provider is down: the embedding round fails, the blob round commits
    FixtureFile c = text("c", "gamma");
    announce(c);
    ScriptedEmbeddingClient.OUTAGE.set(Integer.MAX_VALUE);
    String embeddingCheckpoint = lastCheckpoint(table());
    List<Future<Void>> outcomes = together(blobRound(), embeddingRound());
    outcomes.get(0).get();
    Exception failure = assertThrows(Exception.class, () -> outcomes.get(1).get(), "the embedding round fails while its provider is down");
    assertTrue(EmbeddingStreamers.causeOf(failure, EmbeddingProviderUnavailableException.class).isPresent(),
        "the round failed for the provider outage, not for the other stream: " + EmbeddingStreamers.causes(failure));
    assertEquals(names(a, b, c), blobObjects(), "the blob stream keeps committing");
    assertEquals(latestEventsCheckpoint(), lastCheckpoint(blobTable()));
    assertEquals(embeddingCheckpoint, lastCheckpoint(table()), "the embedding checkpoint holds");
    assertEquals(names(a, b), embeddedObjects());

    // the provider is back: the embedding stream catches up from its own checkpoint, alongside an idle blob round
    ScriptedEmbeddingClient.OUTAGE.set(0);
    for (Future<Void> outcome : together(blobRound(), embeddingRound())) {
      outcome.get();
    }
    assertEquals(names(a, b, c), embeddedObjects());
    assertEquals(latestEventsCheckpoint(), lastCheckpoint(table()));
    assertEquals(2, EmbeddingStreamers.commits(blobTable(), storage()).size(), "the idle blob round commits nothing");
    assertEquals(2, EmbeddingStreamers.commits(table(), storage()).size());
    for (Row row : rows(table()).values()) {
      assertTrue(vector(row) != null, "every embedding row has its embedding: " + row);
    }
  }
}
