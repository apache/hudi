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

package org.apache.hudi.utilities.util;

import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.HoodieLogFormatWriter;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieCommandBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.common.testutils.SchemaTestUtil.getSimpleSchema;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The failure mapping a dangling-file adjudication relies on to tell "no instant" from "we could
 * not find out" — see {@link FileSliceMismatchAnalyzer}'s original doc for why the two must never
 * be confused. Every case here asserts the exact {@link MismatchRecord.InstantUnresolvedReason},
 * never merely that one was set: two different failures both being "not null" is not the same as
 * either one being the reason a caller actually needs.
 *
 * <p>No schema is resolved before the reader is opened, so a log file with no data block (a
 * rollback marker's, above all) is not {@code UNREADABLE} for lack of one. {@code NO_LOG_BLOCK} is
 * the ordinary answer for an empty file and has a case of its own below.
 */
class TestLogFileInstantReader {

  @TempDir
  Path tempDir;

  private HoodieStorage storage;
  private HoodieTableMetaClient metaClient;

  @BeforeEach
  void setUp() {
    storage = new HoodieHadoopStorage(tempDir.toUri().toString(), new Configuration());
    metaClient = mock(HoodieTableMetaClient.class);
    when(metaClient.getStorage()).thenReturn(storage);
  }

  @Test
  void theFileThatIsNotThereIsUnreadableNotAbsent() {
    LogFileInstantReader.Result result = LogFileInstantReader.read(
        metaClient, new StoragePath(tempDir.toString(), ".fg1_1000.log.1_0-1-0"));

    assertFalse(result.isResolved());
    assertEquals(MismatchRecord.InstantUnresolvedReason.UNREADABLE, result.getReason(),
        "a file we could not open tells us nothing about whether anything references it");
    assertNull(result.getInstantTime());
    assertNull(result.getBlockType(),
        "and nothing about what kind of block it starts with; a null type is 'unknown', never "
            + "'not a command block'");
  }

  /**
   * A file that exists but holds zero bytes. The reader's very first {@code hasNext()} call hits an
   * {@code EOFException} reading the 6-byte magic header, catches it internally, and reports no next
   * block — so this is {@code NO_LOG_BLOCK}, distinct from the {@code UNREADABLE} a missing file
   * gets. Asserting only {@code assertNotNull(getReason())} would pass regardless of which of the
   * four reasons came back, so it would not catch this file taking a different branch than its name
   * claims.
   *
   * <p>Not {@code UNREADABLE}: resolving a schema first would give up on a file with no data block
   * one step earlier, and the same step would make a command-block-only rollback marker unreadable.
   * <b>{@code NO_LOG_BLOCK} is not a licence to delete either</b> — an empty
   * log file is as likely to be one a writer has created and not yet flushed as one that is junk.
   */
  @Test
  void theEmptyLogFileHasNoBlockRatherThanBeingUnreadable() throws Exception {
    StoragePath path = new StoragePath(tempDir.toString(), ".fg1_1000.log.1_0-2-0");
    try (OutputStream out = storage.create(path, true)) {
      out.write(new byte[0]);
    }

    LogFileInstantReader.Result result = LogFileInstantReader.read(metaClient, path);

    assertFalse(result.isResolved());
    assertEquals(MismatchRecord.InstantUnresolvedReason.NO_LOG_BLOCK, result.getReason(),
        "an existing-but-empty file was opened successfully and held no block; that is a different "
            + "fact from a file that could not be opened, and a consumer must be able to tell them "
            + "apart");
    assertNull(result.getInstantTime());
    assertNull(result.getBlockType(), "no block was read, so nothing is known about its type");
  }

  /**
   * A block that made it past the schema scan (so it is not {@code UNREADABLE}) and was found by
   * the second read (so it is not {@code NO_LOG_BLOCK}) but carries no {@code INSTANT_TIME} in its
   * header. This is the one failure reason that can only be produced by a genuine, readable block
   * — the other two reasons stop before a block is ever parsed.
   */
  @Test
  void theLogBlockWithNoInstantHeaderIsUnresolvedNotUnreadable() throws Exception {
    HoodieLogFile logFile;
    try (HoodieLogFormat.Writer writer = HoodieLogFormatWriter.builder()
        .withParentPath(new StoragePath(tempDir.toString()))
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
        .withLogFileId("fg2")
        .withInstantTime("1000")
        .withStorage(storage)
        .withSizeThreshold(Long.MAX_VALUE)
        .build()) {
      Map<HoodieLogBlock.HeaderMetadataType, String> header =
          new EnumMap<>(HoodieLogBlock.HeaderMetadataType.class);
      header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
      writer.appendBlock(new HoodieAvroDataBlock(
          Collections.emptyList(), header, HoodieRecord.RECORD_KEY_METADATA_FIELD));
      logFile = writer.getLogFile();
    }

    LogFileInstantReader.Result result = LogFileInstantReader.read(metaClient, logFile.getPath());

    assertFalse(result.isResolved());
    assertEquals(MismatchRecord.InstantUnresolvedReason.NO_INSTANT_HEADER, result.getReason(),
        "a readable block without an INSTANT_TIME header is a different failure than a file we "
            + "could not read at all, and a consumer must be able to tell them apart");
    assertNull(result.getInstantTime());
  }

  /**
   * A real block with an {@code INSTANT_TIME} header is the only way anything is ever resolved;
   * the cases above are the reachable failure vocabulary, but without this one nothing proves the
   * happy path still returns the instant rather than merely failing safely.
   */
  @Test
  void theLogBlockWithAnInstantHeaderResolvesTheInstant() throws Exception {
    String instantTime = "1005";
    HoodieLogFile logFile;
    try (HoodieLogFormat.Writer writer = HoodieLogFormatWriter.builder()
        .withParentPath(new StoragePath(tempDir.toString()))
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
        .withLogFileId("fg1")
        .withInstantTime("1000")
        .withStorage(storage)
        .withSizeThreshold(Long.MAX_VALUE)
        .build()) {
      Map<HoodieLogBlock.HeaderMetadataType, String> header =
          new EnumMap<>(HoodieLogBlock.HeaderMetadataType.class);
      header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, instantTime);
      header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, getSimpleSchema().toString());
      writer.appendBlock(new HoodieAvroDataBlock(
          Collections.emptyList(), header, HoodieRecord.RECORD_KEY_METADATA_FIELD));
      logFile = writer.getLogFile();
    }

    LogFileInstantReader.Result result = LogFileInstantReader.read(metaClient, logFile.getPath());

    assertTrue(result.isResolved());
    assertEquals(instantTime, result.getInstantTime());
    assertNull(result.getReason());
    assertEquals(HoodieLogBlock.HoodieLogBlockType.AVRO_DATA_BLOCK, result.getBlockType(),
        "the data-block half of the pair below: a reader that hard-coded COMMAND_BLOCK, or that "
            + "reported the type of some block other than the first, fails here");
  }

  // ---------------------------------------------------------------------------------------------
  //  The block type, which for a rollback marker is the only thing that can save it
  // ---------------------------------------------------------------------------------------------

  /**
   * A rollback marker, written the way {@code RollbackHelper} writes one. Its {@code INSTANT_TIME}
   * header carries {@code activeTimeline.lastInstant().requestedTime()} — a commit that under a
   * concurrent writer has genuinely <em>completed</em> and has no reason to mention the marker —
   * while the instant actually being rolled back sits in {@code TARGET_INSTANT_TIME}. So the
   * instant read out of this file is indistinguishable from an orphan's, and the block type is the
   * only thing that says otherwise. The fixture deliberately puts a plausible completed instant in
   * {@code INSTANT_TIME} and a different one in {@code TARGET_INSTANT_TIME}, and asserts the reader
   * returns the first: a reader that read the rolled-back instant instead would hand the adjudicator
   * an instant that never completed, which is a different bug with the same file at the end of it.
   */
  @Test
  void theCommandBlockReportsItsTypeAlongsideItsHeaderInstant() throws Exception {
    String lastInstantOnTimeline = "1010";
    String instantBeingRolledBack = "1006";
    HoodieLogFile logFile;
    try (HoodieLogFormat.Writer writer = HoodieLogFormatWriter.builder()
        .withParentPath(new StoragePath(tempDir.toString()))
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
        .withLogFileId("fg3")
        .withInstantTime("1000")
        .withStorage(storage)
        .withSizeThreshold(Long.MAX_VALUE)
        .build()) {
      Map<HoodieLogBlock.HeaderMetadataType, String> header =
          new EnumMap<>(HoodieLogBlock.HeaderMetadataType.class);
      header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, lastInstantOnTimeline);
      header.put(HoodieLogBlock.HeaderMetadataType.TARGET_INSTANT_TIME, instantBeingRolledBack);
      header.put(HoodieLogBlock.HeaderMetadataType.COMMAND_BLOCK_TYPE, String.valueOf(
          HoodieCommandBlock.HoodieCommandBlockTypeEnum.ROLLBACK_BLOCK.ordinal()));
      writer.appendBlock(new HoodieCommandBlock(header));
      logFile = writer.getLogFile();
    }

    LogFileInstantReader.Result result = LogFileInstantReader.read(metaClient, logFile.getPath());

    assertEquals(HoodieLogBlock.HoodieLogBlockType.COMMAND_BLOCK, result.getBlockType(),
        "without the type nothing downstream can tell this file from an orphan, and deleting it "
            + "resurrects the rolled-back data");
    assertTrue(result.isResolved());
    assertEquals(lastInstantOnTimeline, result.getInstantTime(),
        "INSTANT_TIME carries whatever was last on the timeline; TARGET_INSTANT_TIME carries the "
            + "instant being rolled back, and reading the wrong one of the two is its own bug");
  }

  /**
   * A block whose header carries no instant still reports its type. The two halves of a
   * {@link LogFileInstantReader.Result} are independent: the type is what refuses a delete, so it is
   * reported whenever a block was actually read, and an implementation that returned early on the
   * missing instant would drop it. Nothing today reaches a dangling verdict on an instant-less
   * record, so this is defence in depth rather than a live hole — but it is the half that costs data
   * if the other half ever changes.
   */
  @Test
  void theBlockTypeSurvivesAMissingInstantHeader() throws Exception {
    HoodieLogFile logFile;
    try (HoodieLogFormat.Writer writer = HoodieLogFormatWriter.builder()
        .withParentPath(new StoragePath(tempDir.toString()))
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
        .withLogFileId("fg4")
        .withInstantTime("1000")
        .withStorage(storage)
        .withSizeThreshold(Long.MAX_VALUE)
        .build()) {
      Map<HoodieLogBlock.HeaderMetadataType, String> header =
          new EnumMap<>(HoodieLogBlock.HeaderMetadataType.class);
      header.put(HoodieLogBlock.HeaderMetadataType.TARGET_INSTANT_TIME, "1006");
      header.put(HoodieLogBlock.HeaderMetadataType.COMMAND_BLOCK_TYPE, String.valueOf(
          HoodieCommandBlock.HoodieCommandBlockTypeEnum.ROLLBACK_BLOCK.ordinal()));
      writer.appendBlock(new HoodieCommandBlock(header));
      logFile = writer.getLogFile();
    }

    LogFileInstantReader.Result result = LogFileInstantReader.read(metaClient, logFile.getPath());

    assertFalse(result.isResolved());
    assertEquals(MismatchRecord.InstantUnresolvedReason.NO_INSTANT_HEADER, result.getReason());
    assertEquals(HoodieLogBlock.HoodieLogBlockType.COMMAND_BLOCK, result.getBlockType());
  }

  // ---------------------------------------------------------------------------------------------
  //  The header field, tested independently of the block type (COMMAND_BLOCK_TYPE header)
  // ---------------------------------------------------------------------------------------------

  @Test
  void headerCarryingCommandBlockTypeIsRefusedEvenWhenTheBlockTypeSaysOtherwise() {
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "20260815120000");
    header.put(HoodieLogBlock.HeaderMetadataType.COMMAND_BLOCK_TYPE, "0");

    LogFileInstantReader.Result result = LogFileInstantReader.Result.fromHeader(
        header, HoodieLogBlock.HoodieLogBlockType.AVRO_DATA_BLOCK);

    assertTrue(result.isCommandBlock());
  }

  @Test
  void anOrdinaryDataBlockHeaderIsNotACommandBlock() {
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "20260815120000");
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, "{}");

    LogFileInstantReader.Result result = LogFileInstantReader.Result.fromHeader(
        header, HoodieLogBlock.HoodieLogBlockType.AVRO_DATA_BLOCK);

    assertFalse(result.isCommandBlock());
    assertEquals("20260815120000", result.getInstantTime());
    assertEquals(HoodieLogBlock.HoodieLogBlockType.AVRO_DATA_BLOCK, result.getBlockType());
  }

  @Test
  void commandBlockTypeStillReportsItselfThroughTheExistingRoute() {
    LogFileInstantReader.Result result = LogFileInstantReader.Result.of(
        "20260815120000", HoodieLogBlock.HoodieLogBlockType.COMMAND_BLOCK);

    assertTrue(result.isCommandBlock());
    assertEquals(HoodieLogBlock.HoodieLogBlockType.COMMAND_BLOCK, result.getBlockType());
  }

}
