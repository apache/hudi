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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Writes the FS-MDT diff dimension's two reports, on the driver, as JSON documents — the sibling of
 * {@link MismatchReportWriter} and {@link DanglingReportWriter} for the dimension Task 1-9 built:
 * one {@link FileDiffRecord} per disagreeing file, adjudicated by {@link FileDiffAdjudicator} to a
 * {@link Verdict}.
 *
 * <p>Layout, under the same {@code <root>/<outputDir>/<timestamp>} every one of a run's reports
 * shares:
 *
 * <pre>
 * &lt;root&gt;/&lt;outputDir&gt;/&lt;timestamp&gt;/fs_mdt_diff/diff/          diff.json         full population
 * &lt;root&gt;/&lt;outputDir&gt;/&lt;timestamp&gt;/fs_mdt_diff/adjudicated/   adjudicated.json  the finding
 * </pre>
 *
 * <p><b>Both are built from the same adjudicated records; only the population differs.</b> {@code
 * diff/} carries every record phase 1 emitted, verdict and all — {@link Verdict#SANCTIONED} included
 * — because a consumer auditing the run needs to see what was explained, not only what was not.
 * {@code adjudicated/} excludes {@code SANCTIONED} and keeps {@link Verdict#NOT_SETTLED}: "nobody
 * could tell" is itself the finding this directory exists to report, never silently dropped as if it
 * were an explained agreement.
 *
 * <p>Same {@code _COMPLETE} marker, same "a run that found nothing writes nothing" rule, same
 * "the marker is written last and a failure to write it fails the whole write" contract as {@link
 * MismatchReportWriter#writeDocument} — see that class's javadoc for why, and for why {@link #read}
 * deliberately does not check for the marker itself.
 *
 * <p><b>The header carries {@code filesSeen}, {@code partitionsValidated}, {@code dropCounts},
 * {@code notSettledCount} and {@code unmatchedStateCount}.</b> {@code filesSeen} is the mitigation
 * job's blast-radius denominator. {@code dropCounts} is what phase 1 excluded from adjudication
 * entirely (an unattributable instant, an unreadable log file), carried over unchanged. {@code
 * unmatchedStateCount} is {@link FileDiffAdjudicator}'s own counter of records no adjudication row
 * named — a design gap in that table, not a fact about the table being validated — and is not
 * recoverable from the records, so callers pass it in directly, as they do the other two counters.
 */
public final class FileDiffReportWriter {

  private static final Logger LOG = LoggerFactory.getLogger(FileDiffReportWriter.class);

  public static final String REPORT_DIR = "fs_mdt_diff";
  public static final String DIFF_DIR = "diff";
  public static final String DIFF_FILE = "diff.json";
  public static final String ADJUDICATED_DIR = "adjudicated";
  public static final String ADJUDICATED_FILE = "adjudicated.json";
  /** Same marker, same contract, as {@link MismatchReportWriter#COMPLETE_MARKER}. */
  public static final String COMPLETE_MARKER = MismatchReportWriter.COMPLETE_MARKER;

  private FileDiffReportWriter() {
  }

  /**
   * The full population: every {@link FileDiffRecord} phase 1 emitted, each carrying the verdict
   * phase 2 decided, {@link Verdict#SANCTIONED} included.
   *
   * @return the directory written to, or empty when there was nothing to write
   */
  public static Option<String> writeDiff(HoodieStorage storage, String outputRoot, String outputDir,
                                         String timestamp, String tablePath, String mode,
                                         int partitionsValidated, List<FileDiffRecord> records,
                                         long filesSeen, Map<String, Long> dropCounts,
                                         long unmatchedStateCount)
      throws IOException {
    Document document = aggregate(tablePath, timestamp, mode, partitionsValidated, records,
        filesSeen, dropCounts, unmatchedStateCount);
    if (nothingToReport(document)) {
      LOG.info("FS-MDT diff report for {}: nothing to write", tablePath);
      return Option.empty();
    }
    String resultDir =
        writeDocument(storage, outputRoot, outputDir, timestamp, DIFF_DIR, DIFF_FILE, document);
    LOG.warn("Metadata table validation (exhaustive) recorded {} FS-MDT diff record(s) for {} at "
        + "{}", document.recordCount, tablePath, resultDir);
    return Option.of(resultDir);
  }

  /**
   * The finding: the same records as {@link #writeDiff}, minus {@link Verdict#SANCTIONED}.
   *
   * @return the directory written to, or empty when nothing survived exclusion of {@code SANCTIONED}
   */
  public static Option<String> writeAdjudicated(HoodieStorage storage, String outputRoot,
                                                String outputDir, String timestamp, String tablePath,
                                                String mode, int partitionsValidated,
                                                List<FileDiffRecord> records, long filesSeen,
                                                Map<String, Long> dropCounts,
                                                long unmatchedStateCount) throws IOException {
    Document document = adjudicatedDocument(tablePath, timestamp, mode, partitionsValidated, records,
        filesSeen, dropCounts, unmatchedStateCount);
    if (nothingToReport(document)) {
      LOG.info("FS-MDT adjudicated report for {}: nothing survived exclusion of SANCTIONED, "
          + "nothing to write", tablePath);
      return Option.empty();
    }
    String resultDir = writeDocument(storage, outputRoot, outputDir, timestamp, ADJUDICATED_DIR,
        ADJUDICATED_FILE, document);
    LOG.warn("Metadata table validation (exhaustive) adjudicated {} FS-MDT diff record(s) for {} at "
            + "{}, {} of them NOT_SETTLED", document.recordCount, tablePath, resultDir,
        document.notSettledCount);
    return Option.of(resultDir);
  }

  /**
   * A document is genuinely empty only when it has nothing to say on <b>any</b> of the four signals
   * a consumer reads: no records, no drops, no unmatched-state signal and no files seen. Every one of
   * these can be non-zero while {@code recordCount == 0}, and each is a route by which "nothing was
   * found" and "we do not know what happened" would otherwise look identical on disk — the exact
   * failure this class's header exists to prevent:
   *
   * <ul>
   *   <li>{@code dropCounts} — a drop is counted and never carried forward as a record (Task 10's
   *       design: an unattributable record is excluded from adjudication and counted), so a run
   *       where every file was dropped in phase 1 has {@code recordCount == 0} with a non-empty
   *       {@code dropCounts}.
   *   <li>{@code unmatchedStateCount} — a whole-run signal threaded in by the caller, entirely
   *       independent of any report's record count.
   *   <li>{@code filesSeen} — independent of both the records and the drops: a run that listed a
   *       large population and found nothing still has a {@code filesSeen} tally the blast-radius
   *       ceiling needs, and "we examined N files and found nothing" must not read the same as "we
   *       do not know whether anything ran".
   * </ul>
   *
   * <p>Gating on {@code recordCount} alone — or on any strict subset of these four — would silently
   * drop one of them for exactly the run this class exists to report on.
   */
  private static boolean nothingToReport(Document document) {
    return document.recordCount == 0 && document.dropCounts.isEmpty()
        && document.unmatchedStateCount == 0 && document.filesSeen == 0;
  }

  /**
   * The write mechanics {@code diff/} and {@code adjudicated/} share, mirroring {@link
   * MismatchReportWriter#writeDocument} with one extra path segment: both leaf directories sit under
   * a common {@link #REPORT_DIR}, so a reader finds the whole dimension by listing one directory.
   *
   * @return the directory written to
   */
  private static String writeDocument(HoodieStorage storage, String outputRoot, String outputDir,
                                      String timestamp, String leafDir, String fileName,
                                      Document document) throws IOException {
    StoragePath resultDir = new StoragePath(new StoragePath(new StoragePath(
        new StoragePath(outputRoot, outputDir), timestamp), REPORT_DIR), leafDir);
    storage.createDirectory(resultDir);
    try (OutputStream out = storage.create(new StoragePath(resultDir, fileName), true)) {
      out.write(JsonUtils.toString(document).getBytes(StandardCharsets.UTF_8));
      out.write('\n');
    }
    try (OutputStream marker = storage.create(new StoragePath(resultDir, COMPLETE_MARKER), true)) {
      marker.write(String.valueOf(document.recordCount).getBytes(StandardCharsets.UTF_8));
    }
    return resultDir.toString();
  }

  /**
   * Reads a document back off storage, dispatching on the leaf directory's name: {@link #DIFF_DIR}
   * reads {@link #DIFF_FILE}, anything else reads {@link #ADJUDICATED_FILE}. Unlike {@link
   * MismatchReportWriter}, this dimension's two directories carry different file names, so one
   * {@code read} covers both only by looking at which directory it was handed.
   *
   * @param resultDir a directory a write returned
   */
  public static Document read(HoodieStorage storage, String resultDir) throws IOException {
    String fileName = DIFF_DIR.equals(new StoragePath(resultDir).getName()) ? DIFF_FILE : ADJUDICATED_FILE;
    return read(storage, resultDir, fileName);
  }

  static Document read(HoodieStorage storage, String resultDir, String fileName) throws IOException {
    try (InputStream in = storage.open(new StoragePath(new StoragePath(resultDir), fileName))) {
      return JsonUtils.getObjectMapper().readValue(in, Document.class);
    }
  }

  /** The full-population document. */
  static Document aggregate(String tablePath, String timestamp, String mode, int partitionsValidated,
                            Collection<FileDiffRecord> records, long filesSeen,
                            Map<String, Long> dropCounts, long unmatchedStateCount) {
    return buildDocument(tablePath, timestamp, mode, partitionsValidated, records, filesSeen,
        dropCounts, unmatchedStateCount, null);
  }

  /** The same records, filtered to exclude {@link Verdict#SANCTIONED}. */
  static Document adjudicatedDocument(String tablePath, String timestamp, String mode,
                                      int partitionsValidated, Collection<FileDiffRecord> records,
                                      long filesSeen, Map<String, Long> dropCounts,
                                      long unmatchedStateCount) {
    return buildDocument(tablePath, timestamp, mode, partitionsValidated, records, filesSeen,
        dropCounts, unmatchedStateCount, Verdict.SANCTIONED);
  }

  /**
   * The fold {@link #aggregate} and {@link #adjudicatedDocument} share: both carry the run's
   * {@code filesSeen} and {@code dropCounts} through unchanged, and differ only in which records
   * count toward {@code recordCount} and the carried {@code records} list.
   *
   * <p>The two counters are parameters rather than something derived from the records because
   * neither can be: a dropped file is counted and never carried forward as a record, and a
   * partition with nothing wrong contributes files seen and no record at all.
   */
  private static Document buildDocument(String tablePath, String timestamp, String mode,
                                        int partitionsValidated,
                                        Collection<FileDiffRecord> records, long filesSeen,
                                        Map<String, Long> dropCounts, long unmatchedStateCount,
                                        Verdict excludedVerdict) {
    Document document = new Document();
    document.tablePath = tablePath;
    document.timestamp = timestamp;
    document.mode = mode;
    document.partitionsValidated = partitionsValidated;
    document.unmatchedStateCount = unmatchedStateCount;
    document.filesSeen = filesSeen;
    if (dropCounts != null) {
      document.dropCounts.putAll(dropCounts);
    }
    for (FileDiffRecord record : records) {
      if (record == null) {
        continue;
      }
      if (record.getVerdict() == excludedVerdict && excludedVerdict != null) {
        continue;
      }
      document.recordCount++;
      if (record.getVerdict() == Verdict.NOT_SETTLED) {
        document.notSettledCount++;
      }
      document.records.add(record);
    }
    return document;
  }

  /**
   * The written contract for {@code fs_mdt_diff/}. Serialized field by field by {@code JsonUtils},
   * so fields may be added freely, must not be renamed, and may be removed only once no consumer of
   * the documents reads them.
   */
  public static class Document implements Serializable {

    private static final long serialVersionUID = 1L;

    private String tablePath;
    private String timestamp;
    private String mode;
    private int partitionsValidated;
    /** Every file either side's listing considered across every partition. */
    private long filesSeen;
    private long recordCount;
    /** How many carried records could not be settled either way. */
    private long notSettledCount;
    /**
     * {@link FileDiffAdjudicator#getUnmatchedStateCount()}, threaded through by the caller. Zero on
     * a healthy run; anything else is a gap in the adjudication table, not a fact about the table
     * being validated — surfaced here rather than silently folded into {@code notSettledCount} so it
     * cannot be mistaken for one.
     */
    private long unmatchedStateCount;
    private final Map<String, Long> dropCounts = new LinkedHashMap<>();
    private final List<FileDiffRecord> records = new ArrayList<>();

    public String getTablePath() {
      return tablePath;
    }

    public String getTimestamp() {
      return timestamp;
    }

    public String getMode() {
      return mode;
    }

    public int getPartitionsValidated() {
      return partitionsValidated;
    }

    public long getFilesSeen() {
      return filesSeen;
    }

    public long getRecordCount() {
      return recordCount;
    }

    public long getNotSettledCount() {
      return notSettledCount;
    }

    public long getUnmatchedStateCount() {
      return unmatchedStateCount;
    }

    public Map<String, Long> getDropCounts() {
      return dropCounts;
    }

    public List<FileDiffRecord> getRecords() {
      return records;
    }
  }
}
