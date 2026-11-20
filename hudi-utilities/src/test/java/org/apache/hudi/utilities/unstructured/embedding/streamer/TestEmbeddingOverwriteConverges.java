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
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingRoundTestBase;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers.Round;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.FixtureFile;
import org.apache.hudi.utilities.unstructured.embedding.compute.DummyEmbeddingClient;
import org.apache.hudi.utilities.unstructured.embedding.prep.TextPipePreprocessor;

import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Objects are version-keyed by the status observed at read time; every historical version's chunks stay. */
@Tag("functional")
public class TestEmbeddingOverwriteConverges extends EmbeddingRoundTestBase {

  /** Objects above this many bytes are out of line. */
  private static final long INLINE_CEILING = 40;

  private TypedProperties prepAndCompute() {
    return props(table(), EmbeddingTableConfig.MODE_PREP_AND_COMPUTE, INLINE_CEILING);
  }

  /** The prepared text of each row of {@code file}, by chunk id. */
  private Map<Long, String> texts(FixtureFile file) {
    return texts(file, versionOf(file));
  }

  private Map<Long, String> texts(FixtureFile file, Version version) {
    Map<Long, String> texts = new TreeMap<>();
    for (Row row : rows(table()).values()) {
      if (file.sha.equals(row.getAs(EmbeddingTableSchema.SOURCE_PATH_SHA))
          && version.modificationTime == row.<Long>getAs(EmbeddingTableSchema.SOURCE_MODIFICATION_TIME)
          && version.size == row.<Long>getAs(EmbeddingTableSchema.SOURCE_FILE_SIZE)) {
        texts.put(row.getAs(EmbeddingTableSchema.CHUNK_ID), row.getAs(EmbeddingTableSchema.PREPARED_TEXT_CHUNK));
        long chunk = row.getAs(EmbeddingTableSchema.CHUNK_ID);
        assertArrayEquals(DummyEmbeddingClient.vectorFor(file.sha, chunk, EmbeddingStreamers.MODEL_DIMENSIONS), vector(row));
      }
    }
    return texts;
  }

  private static Map<Long, String> chunks(String... texts) {
    Map<Long, String> chunks = new TreeMap<>();
    for (int i = 0; i < texts.length; i++) {
      chunks.put((long) i, texts[i]);
    }
    return chunks;
  }

  private void overwriteInPlace(FixtureFile file, String... parts) throws Exception {
    Files.write(objects().file(keyOf(file)), String.join("|", parts).getBytes(StandardCharsets.UTF_8));
  }

  /**
   * The overwrite has another length than the event announced, and lands on the other side of the inline ceiling: an inline-size
   * event whose object is now out-of-line size, and the reverse. Only a source that reads the object's current size and bytes
   * ingests the current content whole.
   */
  @ParameterizedTest(name = "announced out of line: {0}")
  @ValueSource(booleans = {false, true})
  public void anOverwriteBeforeTheReadIsIngestedUnderTheVersionRead(boolean announcedOutOfLine) throws Exception {
    String longPart = " with enough words to pass the inline ceiling";
    String[] announced = announcedOutOfLine ? new String[] {"first" + longPart, "second" + longPart} : new String[] {"first", "second"};
    String[] current = announcedOutOfLine ? new String[] {"third", "fourth", "fifth"}
        : new String[] {"third" + longPart, "fourth" + longPart, "fifth" + longPart};
    FixtureFile a = text("a", announced);
    byte[] currentBytes = String.join("|", current).getBytes(StandardCharsets.UTF_8);
    assertEquals(announcedOutOfLine, a.bytes.length > INLINE_CEILING);
    assertEquals(!announcedOutOfLine, currentBytes.length > INLINE_CEILING, "the overwrite crosses the inline ceiling");
    announce(a);
    // overwritten after its event and before any round reads it, with another length
    overwriteInPlace(a, current);

    Version observed = versionOf(a);
    Round first = round(prepAndCompute());

    assertEquals(chunks(current), texts(a), "the current content is ingested whole");
    assertEquals(3, first.getRowsWritten());

    // the overwrite's own event: the same keys, the same content
    announce(object(keyOf(a), currentBytes));
    touch(a, Instant.ofEpochMilli(observed.modificationTime));
    Round second = round(prepAndCompute());

    assertEquals(3, second.getRowsWritten(), second.toString());
    assertEquals(chunks(current), texts(a), "the later event upserts the same version");
    assertEquals(3, rows(table()).size(), "an unchanged version has the same keys");
    assertEquals(2, EmbeddingStreamers.commits(table(), storage()).size());
    assertEquals(latestEventsCheckpoint(), EmbeddingStreamers.checkpoint(table(), storage(), second.getCommit().get()));
  }

  /**
   * An events table that keeps every notification (this fixture's, keyed by event id) announces one object written twice in a
   * batch, with two sizes. The source builds one row for the path, so the round prepares and embeds the current content once.
   */
  @Test
  public void anObjectAnnouncedTwiceInOneBatchIsPreparedOnce() throws Exception {
    FixtureFile first = text("a", "alpha", "beta");
    FixtureFile overwrite = text("a", "gamma", "delta", "epsilon");
    assertTrue(first.bytes.length != overwrite.bytes.length, "two announced sizes survive the fetcher's selection as two entries");
    announce(first, overwrite);

    Round round = round(prepAndCompute());

    assertEquals(1, TextPipePreprocessor.OPENED.get(), "the object is prepared once, not once per announcement");
    assertEquals(3, round.getRowsWritten(), round.toString());
    assertEquals(chunks("gamma", "delta", "epsilon"), texts(overwrite), "the current content, once");
    assertEquals(3, rows(table()).size(), "one source version of the path");
    assertEquals(1, EmbeddingStreamers.commits(table(), storage()).size());
    assertEquals(latestEventsCheckpoint(), EmbeddingStreamers.checkpoint(table(), storage(), round.getCommit().get()));
  }

  @Test
  public void anOverwriteInALaterBatchAddsVersionRowsAndKeepsTheOld() throws Exception {
    FixtureFile a = text("a", "alpha", "beta", "gamma");
    announce(a);
    touch(a, Instant.parse("2026-01-02T10:00:00Z"));
    Version old = versionOf(a);
    round(prepAndCompute());

    announce(object(keyOf(a), "delta".getBytes(StandardCharsets.UTF_8)));
    touch(a, Instant.parse("2026-01-02T10:00:01Z"));
    Round next = round(prepAndCompute());

    assertEquals(1, next.getRowsWritten(), next.toString());
    assertEquals(4, rows(table()).size());
    assertEquals(chunks("alpha", "beta", "gamma"), texts(a, old));
    assertEquals(chunks("delta"), texts(a));
    Map<String, Row> rows = rows(table());
    for (long chunk = 0; chunk < 3; chunk++) {
      assertTrue(rows.containsKey(key(a, old, "text", chunk)), rows.keySet().toString());
    }
    assertEquals("delta", rows.get(key(a, "text", 0)).getAs(EmbeddingTableSchema.PREPARED_TEXT_CHUNK));
    assertEquals(2, EmbeddingStreamers.commits(table(), storage()).size());
    assertEquals(latestEventsCheckpoint(), EmbeddingStreamers.checkpoint(table(), storage(), next.getCommit().get()));
  }

  @Test
  public void anUnchangedObjectInALaterBatchIsIdempotent() throws Exception {
    FixtureFile a = text("a", "alpha", "beta", "gamma");
    announce(a);
    Version version = versionOf(a);
    round(prepAndCompute());
    announce(a);
    touch(a, Instant.ofEpochMilli(version.modificationTime));

    Round next = round(prepAndCompute());

    assertEquals(3, rows(table()).size());
    assertEquals(chunks("alpha", "beta", "gamma"), texts(a, version));
    assertEquals(3, next.getRowsWritten());
    assertEquals(2, EmbeddingStreamers.commits(table(), storage()).size());
  }

  @Test
  public void newVersionThatFailsToPrepareLeavesTheOldRowsAndRecordsTheFailure() throws Exception {
    FixtureFile a = text("a", "alpha", "beta");
    announce(a);
    Version old = versionOf(a);
    round(prepAndCompute());

    FixtureFile b = text("b", "delta");
    announce(object(keyOf(a), ("gamma|" + TextPipePreprocessor.BROKEN).getBytes(StandardCharsets.UTF_8)), b);
    Round round = round(prepAndCompute());

    assertEquals(1, round.getRowsWritten(), "only the other object's row: " + round);
    assertEquals(chunks("delta"), texts(b));
    assertEquals(1, round.getErrors().size(), round.toString());
    Map<String, Object> error = round.getErrors().get(0);
    // The prep error record identifies the failed source version.
    assertEquals(new TreeSet<>(Arrays.asList("source_path_sha", "source_modification_time", "source_file_size", "source_file_path",
        "modality", "chunk_id", "stage", "error", "instant")), new TreeSet<>(error.keySet()));
    assertEquals(a.sha, error.get("source_path_sha"));
    assertEquals(a.sourcePath, error.get("source_file_path"));
    assertEquals("prep", error.get("stage"));
    assertTrue(String.valueOf(error.get("error")).contains("TEST_BROKEN"), error.toString());
    assertEquals(chunks("alpha", "beta"), texts(a, old), "the old version rows are unchanged");
    assertEquals(latestEventsCheckpoint(), EmbeddingStreamers.checkpoint(table(), storage(), round.getCommit().get()),
        "the round commits past the failed version");
  }
}
