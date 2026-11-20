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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.HoodieStorage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Writes the two duplicate-base-file dimension reports, beside the FS-MDT dimension's own pair:
 *
 * <pre>
 * &lt;root&gt;/&lt;outputDir&gt;/&lt;timestamp&gt;/dangling_files/  dangling.json     suspects, unadjudicated
 * &lt;root&gt;/&lt;outputDir&gt;/&lt;timestamp&gt;/adjudicated/    adjudicated.json  the answer
 * </pre>
 *
 * <p>Same {@code Document} schema, same {@code _COMPLETE} marker, same "a run that found nothing
 * writes nothing" rule, through the same {@link MismatchReportWriter#writeDocument} body. One
 * reader handles every report a run writes.
 *
 * <p><b>Both are exhaustive-only</b>. Fail-fast persists nothing at all: it detects, logs
 * each suspect on the executor that found it, and fails the run. That is not merely a policy — with
 * {@code --ignore-failed} off a fail-fast run throws at the FIRST surviving suspect, so a report
 * written then would be partial in a way that depends on Spark's scheduling rather than on the
 * table.
 */
public final class DanglingReportWriter {

  private static final Logger LOG = LoggerFactory.getLogger(DanglingReportWriter.class);

  public static final String SUSPECTS_DIR = "dangling_files";
  public static final String SUSPECTS_FILE = "dangling.json";
  public static final String ADJUDICATED_DIR = "adjudicated";
  public static final String ADJUDICATED_FILE = "adjudicated.json";

  private DanglingReportWriter() {
  }

  /**
   * The raw suspects, exactly as the executors found them and with no timeline consulted.
   *
   * @param suspects   every suspect that survived classification, sorted by the caller
   * @param filesSeen  every file the run's storage listings considered, which is the denominator a
   *                   mitigation job sizes a delete list against; nothing about a record says it, so
   *                   it is passed in
   * @return the directory written to, or empty when the run found no suspects
   */
  public static Option<String> writeSuspects(HoodieStorage storage, String outputRoot,
                                             String outputDir, String timestamp, String tablePath,
                                             String mode, int partitionsValidated,
                                             List<MismatchRecord> suspects, long filesSeen)
      throws IOException {
    MismatchReportWriter.Document document = MismatchReportWriter.documentOf(
        tablePath, timestamp, mode, partitionsValidated, filesSeen, suspects);
    if (document.getRecordCount() == 0) {
      LOG.info("Dangling-file suspect report for {}: nothing to write", tablePath);
      return Option.empty();
    }
    String resultDir = MismatchReportWriter.writeDocument(
        storage, outputRoot, outputDir, timestamp, SUSPECTS_DIR, SUSPECTS_FILE, document);
    LOG.warn("Metadata table validation (exhaustive) recorded {} dangling-file suspect(s) for {} "
            + "at {}. Nothing here is proven: the adjudicated report is what a consumer acts on",
        document.getRecordCount(), tablePath, resultDir);
    return Option.of(resultDir);
  }

  /**
   * The reconciled answer. Takes a finished {@code Document} rather than reports to fold, because
   * the reconciliation pass has already chosen what survives and recomputed every summary field
   * from it.
   *
   * @return the directory written to, or empty when nothing survived reconciliation
   */
  public static Option<String> writeAdjudicated(HoodieStorage storage, String outputRoot,
                                                String outputDir, String timestamp,
                                                MismatchReportWriter.Document document)
      throws IOException {
    if (document == null || document.getRecordCount() == 0) {
      LOG.info("Adjudicated report for {}: nothing survived reconciliation, nothing to write",
          document == null ? "<unknown>" : document.getTablePath());
      return Option.empty();
    }
    String resultDir = MismatchReportWriter.writeDocument(
        storage, outputRoot, outputDir, timestamp, ADJUDICATED_DIR, ADJUDICATED_FILE, document);
    LOG.warn("Metadata table validation (exhaustive) adjudicated {} record(s) for {} at {}, {} of "
            + "them proven dangling files", document.getRecordCount(), document.getTablePath(),
        resultDir, document.getCounts().getOrDefault(MismatchPattern.DANGLING_FILES.name(), 0L));
    return Option.of(resultDir);
  }

  /** The {@code dangling_files/} document one run wrote. */
  public static MismatchReportWriter.Document readSuspects(HoodieStorage storage, String resultDir)
      throws IOException {
    return MismatchReportWriter.read(storage, resultDir, SUSPECTS_FILE);
  }

  /** The {@code adjudicated/} document one run wrote. */
  public static MismatchReportWriter.Document readAdjudicated(HoodieStorage storage,
                                                              String resultDir) throws IOException {
    return MismatchReportWriter.read(storage, resultDir, ADJUDICATED_FILE);
  }
}
