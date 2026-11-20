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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The JSON document schema and write mechanics shared by every findings report of a run. Root
 * defaults to the base path, outputDir to {@code .hoodie_validation/table_validation_job}:
 *
 * <pre>
 * &lt;root&gt;/&lt;outputDir&gt;/&lt;timestamp&gt;/&lt;leafDir&gt;/
 *     &lt;fileName&gt;.json
 *     _COMPLETE
 * </pre>
 *
 * <p>{@link DanglingReportWriter} and {@link FileDiffReportWriter} write their leaf directories under
 * the same timestamp. A directory without {@code .hoodie_partition_metadata} is never taken for a
 * partition, so the validator cannot read its own output as table data. Nothing is written when a
 * run finds nothing.
 *
 * <p>{@code _COMPLETE} is written last and is read by nothing in this repository: in-process
 * consumers get directories from {@link ValidationRunReports}, which only names complete ones. A
 * reader that resolves a report directory by path must check for the marker before parsing it.
 */
public final class MismatchReportWriter {

  /** Written after the findings file is closed; its content is the record count. */
  public static final String COMPLETE_MARKER = "_COMPLETE";

  private MismatchReportWriter() {
  }

  /**
   * Writes one JSON document, then the marker. A marker failure must propagate: that is what makes
   * every directory named by {@link ValidationRunReports} complete.
   *
   * @return the directory written to
   */
  static String writeDocument(HoodieStorage storage, String outputRoot, String outputDir,
                              String timestamp, String leafDir, String fileName, Document document)
      throws IOException {
    StoragePath resultDir = new StoragePath(
        new StoragePath(new StoragePath(outputRoot, outputDir), timestamp), leafDir);
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
   * Reads a document back off storage with the same schema {@link #writeDocument} used, so the
   * reconciliation pass can be derived from, and re-run against, what was persisted. Does not check
   * the marker.
   *
   * @param resultDir the directory a write returned
   */
  public static Document read(HoodieStorage storage, String resultDir, String fileName)
      throws IOException {
    try (InputStream in = storage.open(new StoragePath(new StoragePath(resultDir), fileName))) {
      return JsonUtils.getObjectMapper().readValue(in, Document.class);
    }
  }

  /**
   * Builds a document whose summary counts describe exactly {@code records}, the final decided list.
   * {@code filesSeen} cannot be derived from records, so the caller passes it in.
   */
  public static Document documentOf(String tablePath, String timestamp, String mode,
                                    int partitionsValidated, long filesSeen,
                                    List<MismatchRecord> records) {
    Document document = new Document();
    document.tablePath = tablePath;
    document.timestamp = timestamp;
    document.mode = mode;
    document.partitionsValidated = partitionsValidated;
    document.filesSeen = filesSeen;
    for (MismatchRecord record : records) {
      document.records.add(record);
      document.recordCount++;
      if (record.isFailsValidation()) {
        document.failingRecordCount++;
      }
      if (record.getPattern() != null) {
        document.counts.merge(record.getPattern().name(), 1L, Long::sum);
      }
    }
    return document;
  }

  /**
   * The written contract. Serialized field by field by {@code JsonUtils}, so fields may be added
   * but must not be renamed.
   */
  public static class Document implements Serializable {

    private static final long serialVersionUID = 1L;

    private String tablePath;
    private String timestamp;
    private String mode;
    private int partitionsValidated;
    /** Every file the storage listing considered across every partition. */
    private long filesSeen;
    private long recordCount;
    /** Findings that make a partition fail; the rest are recorded and tolerated. */
    private long failingRecordCount;
    private final Map<String, Long> counts = new LinkedHashMap<>();
    private final List<MismatchRecord> records = new ArrayList<>();

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

    public long getFailingRecordCount() {
      return failingRecordCount;
    }

    public Map<String, Long> getCounts() {
      return counts;
    }

    public List<MismatchRecord> getRecords() {
      return records;
    }
  }
}
