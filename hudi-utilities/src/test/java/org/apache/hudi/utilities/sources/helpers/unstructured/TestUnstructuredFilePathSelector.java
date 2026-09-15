/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
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

package org.apache.hudi.utilities.sources.helpers.unstructured;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.config.UnstructuredFileSourceConfig;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.checkpoint.CheckpointUtils.createCheckpoint;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Batch selection for the unstructured source: paths survive intact, batches are bounded by file
 * count, and a group of files sharing one modification time can be split and resumed.
 */
public class TestUnstructuredFilePathSelector {

  @TempDir
  Path root;

  private TypedProperties props(int maxFiles) {
    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.streamer.source.dfs.root", root.toString());
    props.setProperty(UnstructuredFileSourceConfig.MAX_FILES_PER_BATCH.key(), String.valueOf(maxFiles));
    return props;
  }

  private UnstructuredFilePathSelector selector(int maxFiles) {
    return new UnstructuredFilePathSelector(props(maxFiles), new Configuration());
  }

  private void write(String name, long modificationTime) throws IOException {
    Path file = root.resolve(name);
    Files.createDirectories(file.getParent());
    Files.write(file, ("content of " + name).getBytes(StandardCharsets.UTF_8));
    assertTrue(file.toFile().setLastModified(modificationTime), "could not set mtime on " + name);
  }

  /**
   * Paths were previously joined into one comma-delimited string and split back apart, so a single
   * file whose name contains a comma was torn into fragments and failed the whole batch. Commas in
   * filenames are ordinary in real document corpora.
   */
  @Test
  void testPathsContainingCommasSurviveSelection() throws IOException {
    write("Q3,2024 report.txt", 1_000L);
    write("Smith, John - resume.txt", 2_000L);
    write("ordinary.txt", 3_000L);

    List<UnstructuredFilePathSelector.FileEntry> files =
        selector(100).selectNextBatch(Option.empty(), Long.MAX_VALUE).files;

    assertEquals(3, files.size(), "one entry per file, regardless of commas in the name");
    Set<String> names = files.stream().map(f -> new java.io.File(f.path).getName())
        .collect(Collectors.toSet());
    assertTrue(names.contains("Q3,2024 report.txt"), "got: " + names);
    assertTrue(names.contains("Smith, John - resume.txt"), "got: " + names);
  }

  /** Each entry carries what the driver already learned, so executors need not stat again. */
  @Test
  void testEntriesCarrySizeAndModificationTime() throws IOException {
    write("doc.txt", 5_000L);

    UnstructuredFilePathSelector.FileEntry entry =
        selector(100).selectNextBatch(Option.empty(), Long.MAX_VALUE).files.get(0);

    assertEquals(Files.size(root.resolve("doc.txt")), entry.size);
    assertEquals(5_000L, entry.modificationTime);
  }

  /**
   * A byte budget alone never bounds file count, which is what drives driver memory and task
   * count. A folder of small files could therefore pull an entire corpus into one batch.
   */
  @Test
  void testBatchIsBoundedByFileCount() throws IOException {
    for (int i = 0; i < 50; i++) {
      write(String.format("doc-%03d.txt", i), 1_000L + i);
    }

    assertEquals(10, selector(10).selectNextBatch(Option.empty(), Long.MAX_VALUE).files.size());
  }

  /**
   * The core fix. Every file shares one modification time, which is what a bulk upload produces
   * and what object stores report at second granularity. Previously the batch could not be split,
   * because the next sync's {@code mtime > checkpoint} filter would strand the remainder - so the
   * limit was ignored and the whole group was ingested at once. With the path recorded alongside
   * the timestamp the group splits safely and resumes.
   */
  @Test
  void testSameModificationTimeGroupSplitsAndResumes() throws IOException {
    for (int i = 0; i < 50; i++) {
      write(String.format("doc-%03d.txt", i), 7_777L);
    }

    UnstructuredFilePathSelector selector = selector(10);
    List<String> ingested = new ArrayList<>();
    Option<Checkpoint> checkpoint = Option.empty();

    for (int sync = 0; sync < 5; sync++) {
      UnstructuredFilePathSelector.Batch batch = selector.selectNextBatch(checkpoint, Long.MAX_VALUE);
      assertEquals(10, batch.files.size(), "sync " + sync + " must honour the file cap");
      batch.files.forEach(f -> ingested.add(f.path));
      checkpoint = Option.of(batch.checkpoint);
    }

    assertEquals(50, ingested.size());
    assertEquals(50, new HashSet<>(ingested).size(), "no file may be ingested twice");
    assertTrue(selector.selectNextBatch(checkpoint, Long.MAX_VALUE).files.isEmpty(),
        "the group must be exhausted, not left stranded");
  }

  /**
   * A table written before this selector carries a bare timestamp. Upgrading must not re-ingest
   * everything at that timestamp, so a legacy checkpoint keeps the old "strictly newer" meaning.
   */
  @Test
  void testLegacyTimestampCheckpointIsHonoured() throws IOException {
    write("old.txt", 1_000L);
    write("new.txt", 9_000L);

    List<UnstructuredFilePathSelector.FileEntry> files = selector(100)
        .selectNextBatch(Option.of(createCheckpoint("1000")), Long.MAX_VALUE).files;

    assertEquals(1, files.size());
    assertTrue(files.get(0).path.endsWith("new.txt"));
  }

  /**
   * A non-positive cap selects nothing and leaves the checkpoint untouched, so every later sync
   * would stall silently rather than fail. Reject it where the message can still name the config.
   */
  @Test
  void testNonPositiveFileCapIsRejected() {
    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, () -> selector(0));
    assertTrue(thrown.getMessage().contains(UnstructuredFileSourceConfig.MAX_FILES_PER_BATCH.key()),
        "the error must name the offending config key, got: " + thrown.getMessage());
  }

  /** The byte budget still applies, but a single oversized file must not stall the sync forever. */
  @Test
  void testByteLimitStillAppliesAndNeverStalls() throws IOException {
    write("a.txt", 1_000L);
    write("b.txt", 2_000L);
    write("c.txt", 3_000L);

    assertEquals(1, selector(100).selectNextBatch(Option.empty(), 1L).files.size(),
        "a limit smaller than the first file must still make progress");

    UnstructuredFilePathSelector.Batch batch =
        selector(100).selectNextBatch(Option.empty(), Long.MAX_VALUE);
    assertEquals(3, batch.files.size());
    assertFalse(batch.files.isEmpty());
  }
}
