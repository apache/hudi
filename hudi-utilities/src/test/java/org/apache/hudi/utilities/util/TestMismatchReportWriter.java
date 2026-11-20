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

import org.apache.hudi.common.util.JsonUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;
import org.apache.hudi.utilities.HoodieMetadataTableValidator;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The document schema and the write mechanics every one of a run's reports goes through, read back
 * off storage rather than asserted from the object that wrote it.
 */
class TestMismatchReportWriter {

  @TempDir
  Path tempDir;

  private static final String TIMESTAMP = "20260812153000";
  private static final String OUTPUT_DIR =
      HoodieMetadataTableValidator.Config.DEFAULT_VALIDATION_OUTPUT_DIR;
  private static final String LEAF_DIR = "findings";
  private static final String FILE_NAME = "findings.json";

  @Test
  void documentAndMarkerAreBothWritten() throws IOException {
    HoodieStorage storage = storage();
    String root = tempDir.toUri().toString();
    MismatchReportWriter.Document document = MismatchReportWriter.documentOf(
        root + "/table", TIMESTAMP, "exhaustive", 3, 42L,
        Arrays.asList(record(MismatchPattern.FS_EXCLUSIVE_LOG_FILE, false),
            record(MismatchPattern.MDT_EXCLUSIVE_FILE_GROUP, true)));

    String resultDir = MismatchReportWriter.writeDocument(
        storage, root, OUTPUT_DIR, TIMESTAMP, LEAF_DIR, FILE_NAME, document);

    assertTrue(storage.exists(
            new StoragePath(new StoragePath(resultDir), MismatchReportWriter.COMPLETE_MARKER)),
        "without the marker a consumer cannot tell a finished run from a killed one");

    JsonNode written = read(storage, new StoragePath(new StoragePath(resultDir), FILE_NAME));
    assertEquals(2, written.get("recordCount").asLong());
    assertEquals(1, written.get("failingRecordCount").asLong(),
        "an uncommitted storage-side log file is recorded without failing the validation");
    assertEquals(3, written.get("partitionsValidated").asInt());
    assertEquals(42L, written.get("filesSeen").asLong());
    assertEquals(1, written.get("counts").get("FS_EXCLUSIVE_LOG_FILE").asLong());
    assertEquals(2, written.get("records").size());

    JsonNode first = written.get("records").get(0);
    assertEquals("2026/08/12", first.get("partition").asText());
    assertEquals("FS", first.get("side").asText());
    assertEquals("LOG_BLOCK_HEADER", first.get("instantSource").asText());
    assertEquals("1005", first.get("instantTime").asText());
  }

  /** Every record reaches the document, and the counts describe all of them. */
  @Test
  void everyRecordIsCarriedAndCounted() {
    MismatchReportWriter.Document document = MismatchReportWriter.documentOf(
        "/t", "run-1", "exhaustive", 2, 0L,
        Arrays.asList(someRecord("p1", "f1.parquet"), someRecord("p1", "f2.parquet"),
            someRecord("p2", "f3.parquet")));

    assertEquals(3, document.getRecords().size());
    assertEquals(3, document.getRecordCount());
    assertEquals(3L, document.getCounts().get(MismatchPattern.FS_EXCLUSIVE_BASE_FILE.name()));
  }

  /**
   * {@code documentOf} cannot derive {@code filesSeen} from the records it is handed — unlike
   * {@code recordCount} and {@code counts}, nothing about a record says how big the run's storage
   * listing was — so the caller supplies it, and it describes the table rather than the record list.
   */
  @Test
  void filesSeenIsTheFigureTheCallerGaveAndNotOneDerivedFromTheRecords() {
    MismatchReportWriter.Document document = MismatchReportWriter.documentOf(
        "/t", "run-1", "exhaustive", 1, 500L,
        Arrays.asList(someRecord("p1", "f1.parquet"), someRecord("p1", "f2.parquet")));

    assertEquals(2, document.getRecordCount());
    assertEquals(500L, document.getFilesSeen(),
        "filesSeen describes the table, not the carried record list");
  }

  /**
   * The property that stands in for "a consumer checks {@code _COMPLETE}", which no consumer does.
   *
   * <p>Nothing in either repository reads the marker: every consumer takes its report directory
   * from {@code ValidationRunReports}, which the validator fills in only with what the write
   * <em>returned</em>. That makes "a directory the handle names carries a marker" true by
   * construction — but only for as long as a marker that could not be written stops the write.
   * Swallow the marker's failure and the handle starts naming directories holding a findings file
   * and nothing to say the run reached the end of it: the exact state the marker exists to rule
   * out, and one the handle then cannot describe either.
   *
   * <p>Recorded as a characterisation test. The code already behaves this way, so this did not fail
   * before it was written; it is here so that making the marker optional fails something.
   */
  @Test
  void markerThatCannotBeWrittenFailsTheWholeWrite() {
    HoodieStorage storage = storageRefusingToCreate(MismatchReportWriter.COMPLETE_MARKER);
    String root = tempDir.toUri().toString();
    MismatchReportWriter.Document document = MismatchReportWriter.documentOf(
        root + "/table", TIMESTAMP, "exhaustive", 1, 1L,
        Collections.singletonList(record(MismatchPattern.FS_EXCLUSIVE_FILE_GROUP, true)));

    assertThrows(IOException.class,
        () -> MismatchReportWriter.writeDocument(storage, root, OUTPUT_DIR, TIMESTAMP, LEAF_DIR,
            FILE_NAME, document),
        "the caller records a directory only when the write returned one, so a marker that could "
            + "not be written must not leave a directory behind that reads as finished");
  }

  private HoodieStorage storage() {
    return new HoodieHadoopStorage(tempDir.toUri().toString(), new Configuration());
  }

  /** A storage that writes everything except one file name, which it fails on. */
  private HoodieStorage storageRefusingToCreate(String fileName) {
    return new HoodieHadoopStorage(tempDir.toUri().toString(), new Configuration()) {
      @Override
      public OutputStream create(StoragePath path, boolean overwrite) throws IOException {
        if (fileName.equals(path.getName())) {
          throw new IOException("cannot write " + path);
        }
        return super.create(path, overwrite);
      }
    };
  }

  private static JsonNode read(HoodieStorage storage, StoragePath path) throws IOException {
    try (InputStream in = storage.open(path)) {
      return JsonUtils.getObjectMapper().readTree(in);
    }
  }

  private static MismatchRecord record(MismatchPattern pattern, boolean failsValidation) {
    return MismatchRecord.newBuilder()
        .withPartition("2026/08/12")
        .withPattern(pattern)
        .withFileGroupId("fg1")
        .withBaseInstantTime("1000")
        .withFileName(".fg1_1000.log.1_0-9-0")
        .withFileType(MismatchRecord.FileType.LOG)
        .withSizeBytes(4096L)
        .withInstant("1005", MismatchRecord.InstantSource.LOG_BLOCK_HEADER)
        .withFailsValidation(failsValidation)
        .withDetail("log file is on storage and not in the metadata table's slice")
        .build();
  }

  private static MismatchRecord someRecord(String partition, String fileName) {
    return MismatchRecord.newBuilder()
        .withPartition(partition)
        .withPattern(MismatchPattern.FS_EXCLUSIVE_BASE_FILE)
        .withFileName(fileName)
        .withFileType(MismatchRecord.FileType.BASE)
        .withFailsValidation(true)
        .build();
  }
}
