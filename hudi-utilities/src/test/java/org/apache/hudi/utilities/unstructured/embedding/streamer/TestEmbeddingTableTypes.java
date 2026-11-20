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
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingRoundTestBase;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers.Round;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.FixtureFile;
import org.apache.hudi.utilities.unstructured.embedding.compute.DummyEmbeddingClient;
import org.apache.hudi.utilities.unstructured.embedding.prep.TextPipePreprocessor;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Real embedding rounds support the streamer's table type in both processing modes. */
@Tag("functional")
class TestEmbeddingTableTypes extends EmbeddingRoundTestBase {

  @ParameterizedTest(name = "{0}, {1}")
  @CsvSource({"COPY_ON_WRITE, prep_only", "COPY_ON_WRITE, prep_and_compute",
      "MERGE_ON_READ, prep_only", "MERGE_ON_READ, prep_and_compute"})
  void roundsCommitOnceRouteErrorsAndRerunIdempotently(HoodieTableType tableType, String mode) throws Exception {
    Instant firstTime = Instant.parse("2024-01-01T00:00:00Z");
    Instant nextTime = Instant.parse("2024-01-02T00:00:00Z");
    FixtureFile first = text("first", "alpha", "beta");
    announce(first);
    touch(first, firstTime);
    Version firstVersion = versionOf(first);
    TypedProperties props = props(table(), mode);

    Round initial = EmbeddingStreamers.round(spark(), props, cfg -> cfg.tableType = tableType.name());

    assertFirstRound(initial, tableType);
    Map<String, Row> expected = new TreeMap<>();
    expectChunks(expected, first, firstVersion, "alpha", "beta");
    assertRows(mode, expected);

    // Same path and size, another mtime: both versions' complete chunks must remain.
    FixtureFile changedTime = text("first", "gamma", "zeta");
    assertEquals(first.bytes.length, changedTime.bytes.length);
    announce(changedTime);
    touch(changedTime, nextTime);
    Version timeVersion = versionOf(changedTime);
    assertOnlyTimeChanged(firstVersion, firstTime, timeVersion, nextTime);

    Round timeRound = EmbeddingStreamers.round(spark(), props, cfg -> cfg.tableType = tableType.name());

    assertOneRoundCommitted(timeRound, 2, 2);
    assertTrue(timeRound.getErrors().isEmpty(), timeRound.toString());
    expectChunks(expected, changedTime, timeVersion, "gamma", "zeta");
    assertRows(mode, expected);

    // Same path and mtime, another size: a shorter version adds its chunk without replacing history.
    FixtureFile changedSize = text("first", "delta");
    FixtureFile second = text("second", "theta");
    FixtureFile broken = text("broken", TextPipePreprocessor.BROKEN);
    announce(changedSize, second, broken);
    touch(changedSize, nextTime);
    touch(second, nextTime);
    touch(broken, nextTime);
    Version sizeVersion = versionOf(changedSize);
    Version secondVersion = versionOf(second);
    Version brokenVersion = versionOf(broken);
    assertOnlySizeChanged(timeVersion, sizeVersion);

    Round next = EmbeddingStreamers.round(spark(), props, cfg -> cfg.tableType = tableType.name());

    assertOneRoundCommitted(next, 2, 3);
    assertEquals(latestEventsCheckpoint(), EmbeddingStreamers.checkpoint(table(), storage(), next.getCommit().get()));
    assertEquals(1, next.getErrors().size(), next.toString());
    assertBrokenFileError(next.getErrors().get(0), broken, brokenVersion);
    expectChunks(expected, changedSize, sizeVersion, "delta");
    expectChunks(expected, second, secondVersion, "theta");
    assertRows(mode, expected);
    Map<String, Row> snapshot = rows(table());
    assertEquals(6, snapshot.size(), "three versions of first and one version of second");

    announce(changedSize, second);
    touch(changedSize, nextTime);
    touch(second, nextTime);
    Round repeated = EmbeddingStreamers.round(spark(), props, cfg -> cfg.tableType = tableType.name());

    assertUnchangedRerun(repeated, tableType, snapshot);
    assertRows(mode, expected);
    assertTrue(repeated.getErrors().isEmpty(), repeated.toString());

    Round empty = EmbeddingStreamers.round(spark(), props, cfg -> cfg.tableType = tableType.name());

    assertEmptyRound(empty, snapshot);
  }

  /** The first round: one batch, two rows, no error, and the table created with the requested type. */
  private void assertFirstRound(Round initial, HoodieTableType tableType) {
    assertEquals(1, initial.getCompleted().size(), initial.toString());
    assertEquals(2, initial.getRowsWritten());
    assertTrue(initial.getErrors().isEmpty(), initial.toString());
    assertEquals(tableType, EmbeddingStreamers.metaClient(table(), storage()).getTableType());
  }

  /** A round that completed one batch, wrote {@code rows} rows and made the table's {@code commits}-th commit. */
  private void assertOneRoundCommitted(Round round, int rows, int commits) {
    assertEquals(1, round.getCompleted().size(), round.toString());
    assertEquals(rows, round.getRowsWritten());
    assertEquals(commits, EmbeddingStreamers.commits(table(), storage()).size());
  }

  /** The second version differs from the first in its modification time only. */
  private static void assertOnlyTimeChanged(Version firstVersion, Instant firstTime, Version timeVersion, Instant nextTime) {
    assertEquals(firstTime.toEpochMilli(), firstVersion.modificationTime);
    assertEquals(nextTime.toEpochMilli(), timeVersion.modificationTime);
    assertEquals(firstVersion.size, timeVersion.size);
  }

  /** The third version differs from the second in its size only (5 bytes against 10). */
  private static void assertOnlySizeChanged(Version timeVersion, Version sizeVersion) {
    assertEquals(timeVersion.modificationTime, sizeVersion.modificationTime);
    assertEquals(5, sizeVersion.size);
    assertEquals(10, timeVersion.size);
  }

  /** The quarantine entry of the broken file: its key, version and stage, and the preprocessor's failure. */
  private static void assertBrokenFileError(Map<String, Object> error, FixtureFile broken, Version brokenVersion) {
    assertEquals(broken.sha, error.get("source_path_sha"));
    assertEquals(broken.sourcePath, error.get("source_file_path"));
    assertEquals(brokenVersion.modificationTime, ((Number) error.get("source_modification_time")).longValue());
    assertEquals(brokenVersion.size, ((Number) error.get("source_file_size")).longValue());
    assertEquals("prep", error.get("stage"));
    assertTrue(String.valueOf(error.get("error")).contains("TEST_BROKEN"), error.toString());
  }

  /** A rerun of unchanged versions: two updates of existing keys in a 4th commit, the snapshot unchanged. */
  private void assertUnchangedRerun(Round repeated, HoodieTableType tableType, Map<String, Row> snapshot) {
    assertEquals(1, repeated.getCompleted().size(), repeated.toString());
    assertEquals(2, repeated.getRowsWritten(), repeated.toString());
    // the two unchanged versions are updates of existing keys: log files on MERGE_ON_READ, rewritten base files on COPY_ON_WRITE
    List<HoodieWriteStat> repeatedWrites = EmbeddingStreamers.metadata(table(), storage(), repeated.getCommit().get()).getWriteStats();
    assertEquals(2L, repeatedWrites.stream().mapToLong(HoodieWriteStat::getNumUpdateWrites).sum(), repeatedWrites.toString());
    for (HoodieWriteStat write : repeatedWrites) {
      assertEquals(tableType == HoodieTableType.MERGE_ON_READ, FSUtils.isLogFile(write.getPath()), write.getPath());
    }
    assertEquals(4, EmbeddingStreamers.commits(table(), storage()).size());
    assertEquals(latestEventsCheckpoint(), EmbeddingStreamers.checkpoint(table(), storage(), repeated.getCommit().get()));
    assertEquals(describe(snapshot), describe(rows(table())), "unchanged versions add no rows or change their values");
  }

  /** A round with nothing announced: nothing completed, no commit, no error, the snapshot unchanged. */
  private void assertEmptyRound(Round empty, Map<String, Row> snapshot) {
    assertTrue(empty.getCompleted().isEmpty(), empty.toString());
    assertEquals(4, EmbeddingStreamers.commits(table(), storage()).size());
    assertEquals(describe(snapshot), describe(rows(table())));
    assertTrue(empty.getErrors().isEmpty(), empty.toString());
  }

  private static void expectChunks(Map<String, Row> expected, FixtureFile file, Version version, String... chunks) {
    for (int i = 0; i < chunks.length; i++) {
      expected.put(key(file, version, "text", i), RowFactory.create(file.sha, version.modificationTime, version.size, "text",
          (long) i, file.sourcePath, file.fileName(), file.extension, chunks[i], 1));
    }
  }

  private void assertRows(String mode, Map<String, Row> expected) {
    Map<String, Row> snapshot = rows(table());
    assertEquals(expected.keySet(), snapshot.keySet());
    for (Map.Entry<String, Row> entry : snapshot.entrySet()) {
      Row row = entry.getValue();
      assertEquals(expected.get(entry.getKey()), RowFactory.create(row.getAs(EmbeddingTableSchema.SOURCE_PATH_SHA),
          row.getAs(EmbeddingTableSchema.SOURCE_MODIFICATION_TIME), row.getAs(EmbeddingTableSchema.SOURCE_FILE_SIZE),
          row.getAs(EmbeddingTableSchema.MODALITY), row.getAs(EmbeddingTableSchema.CHUNK_ID),
          row.getAs(EmbeddingTableSchema.SOURCE_FILE_PATH), row.getAs(EmbeddingTableSchema.SOURCE_FILE_NAME),
          row.getAs(EmbeddingTableSchema.SOURCE_FILE_EXTENSION), row.getAs(EmbeddingTableSchema.PREPARED_TEXT_CHUNK),
          row.getAs(EmbeddingTableSchema.CHUNK_TOKEN_COUNT)), entry.getKey());
      if ("prep_only".equals(mode)) {
        assertFalse(Arrays.asList(row.schema().fieldNames()).contains(EmbeddingStreamers.MODEL_COLUMN),
            "prep_only writes no model column: " + row.schema().simpleString());
      } else {
        assertArrayEquals(DummyEmbeddingClient.vectorFor(row.getAs(EmbeddingTableSchema.SOURCE_PATH_SHA),
            row.getAs(EmbeddingTableSchema.CHUNK_ID), EmbeddingStreamers.MODEL_DIMENSIONS), vector(row));
      }
    }
  }
}
