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

package org.apache.hudi.tools;

import org.apache.hudi.avro.model.HoodieArchivedMetaEntry;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanPartitionMetadata;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieInstantInfo;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRestorePlan;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPartitionMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.avro.model.HoodieRollbackRequest;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.JsonUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.conf.Configuration;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Standalone Java tool to inspect a Hudi table's active and archived timeline.
 *
 * <p>Modes:
 * <ul>
 *   <li>{@code --show-instant <instantTime> [--action <action>]} — deserialize the matching
 *       instant's content and print all metadata as pretty JSON.</li>
 *   <li>{@code --find-file-id <fileIdOrFileName>} — print a sorted table of timeline events
 *       that mention the given file ID or filename (write stats, replace-commit replaced
 *       file IDs, clustering input slices, clean delete patterns, rollback delete files).</li>
 *   <li>{@code --commit-stats} — per-ingestion-commit records/files/bytes breakdown for a
 *       time range, with a totals + per-commit-average footer.</li>
 *   <li>{@code --phase-timings} — per-ingestion-instant wall-clock split (source read +
 *       indexing + small-file handling, data write, MDT prep, MDT write, MDT-tail) derived
 *       from {@code .hoodie/} state-marker file mtimes on the active timeline, with
 *       per-action mean/p50/p95/max aggregates.</li>
 *   <li>{@code --parse-filename <name>} — decode a Hudi parquet/log filename.</li>
 *   <li>{@code --raw-archive <ts>} — dump the full HoodieArchivedMetaEntry for an instant.</li>
 * </ul>
 *
 * <p>Common options:
 * <pre>
 *   --base-path &lt;path&gt;          (required) table base path (directory containing .hoodie/)
 *   --include-archived          include archived timeline (default true)
 *   --start-instant &lt;instant&gt;   only consider instants &gt;= this instant time
 *   --end-instant &lt;instant&gt;     only consider instants &lt;= this instant time
 *   --actions &lt;csv&gt;             restrict to these actions (commit,deltacommit,replacecommit,
 *                               clean,rollback,compaction,savepoint)
 *   --limit &lt;N&gt;                 cap rows in find-file-id output (default 100)
 * </pre>
 *
 * <p>Example invocations:
 * <pre>
 *   java -cp hudi-common-0.14.1-rc2.jar:hudi-hadoop-common-...jar:... \
 *     org.apache.hudi.tools.TimelineInspector \
 *     --base-path /tmp/apna_table/tbl_path \
 *     --show-instant 20260530092852891
 *
 *   java -cp ... org.apache.hudi.tools.TimelineInspector \
 *     --base-path /tmp/apna_table/tbl_path \
 *     --find-file-id 5d85210a-fc17-411e-a01f-3bb44d9a12cc-0 \
 *     --actions commit,deltacommit,replacecommit,clean \
 *     --start-instant 20260101000000000 \
 *     --limit 500
 * </pre>
 */
public class TimelineInspector {

  private static final ObjectMapper JSON_MAPPER =
      JsonUtils.getObjectMapper().copy().enable(SerializationFeature.INDENT_OUTPUT);
  private static final int DEFAULT_LIMIT = 100;

  static class ExitException extends Exception {
    final int status;

    ExitException(int status, String message) {
      super(message);
      this.status = status;
    }
  }

  public static void main(String[] args) {
    try {
      run(args);
    } catch (ExitException e) {
      System.exit(e.status);
    }
  }

  static void run(String[] args) throws ExitException {
    try {
      runInternal(args);
    } catch (IllegalArgumentException e) {
      System.err.println("ERROR: " + e.getMessage());
      System.err.println();
      Args.printUsage();
      throw new ExitException(2, e.getMessage());
    } catch (IOException | RuntimeException e) {
      System.err.println("ERROR: " + e.getClass().getSimpleName() + ": " + e.getMessage());
      e.printStackTrace(System.err);
      throw new ExitException(1, e.getMessage());
    }
  }

  private static void runInternal(String[] args) throws ExitException, IOException {
    Args parsed = Args.parse(args);

    if (parsed.helpRequested) {
      return;
    }

    // parse-filename is a pure decode utility -- no metaclient needed.
    if (parsed.mode == Mode.PARSE_FILENAME) {
      new TimelineInspector().parseFilename(parsed);
      return;
    }

    Configuration hadoopConf = new Configuration();
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setConf(HadoopFSUtils.getStorageConfWithCopy(hadoopConf))
        .setBasePath(parsed.basePath)
        .build();

    TimelineInspector inspector = new TimelineInspector();
    switch (parsed.mode) {
      case SHOW_INSTANT:
        inspector.showInstant(metaClient, parsed);
        break;
      case FIND_FILE_ID:
        inspector.findFileId(metaClient, parsed);
        break;
      case RAW_ARCHIVE:
        inspector.rawArchive(metaClient, parsed);
        break;
      case COMMIT_STATS:
        inspector.commitStats(metaClient, parsed);
        break;
      case PHASE_TIMINGS:
        inspector.phaseTimings(metaClient, parsed);
        break;
      default:
        throw new IllegalStateException("unknown mode " + parsed.mode);
    }
  }

  // ---- parse-filename -------------------------------------------------------

  /**
   * Decodes a Hudi-conventional parquet or log filename into its constituent parts.
   * Handles three flavors:
   * <ul>
   *   <li>Base file (parquet/hfile/orc): {@code <fileId>_<writeToken>_<instantTime>.<ext>}</li>
   *   <li>Log file: {@code .<fileId>_<baseInstant>.log.<version>_<writeToken>}</li>
   *   <li>Bare file ID: {@code <fileId>} on its own</li>
   * </ul>
   */
  private void parseFilename(Args parsed) throws IOException {
    String input = parsed.filenameToParse;
    String name = input.contains("/") ? input.substring(input.lastIndexOf('/') + 1) : input;
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("input", input);
    out.put("filename", name);

    try {
      org.apache.hudi.storage.StoragePath sp = new org.apache.hudi.storage.StoragePath(name);
      if (FSUtils.isLogFile(sp)) {
        out.put("kind", "log");
        out.put("fileId", FSUtils.getFileIdFromLogPath(sp));
        out.put("baseInstantTime", FSUtils.getDeltaCommitTimeFromLogPath(sp));
        out.put("logVersion", FSUtils.getFileVersionFromLog(name));
        out.put("writeToken", FSUtils.getWriteTokenFromLogPath(sp));
        out.put("logFileExtension", FSUtils.getFileExtensionFromLog(sp));
      } else if (name.contains("_") && name.contains(".")) {
        // Base file convention: <fileId>_<writeToken>_<commitTime>.<ext>
        out.put("kind", "base");
        out.put("fileId", FSUtils.getFileId(name));
        out.put("commitTime", FSUtils.getCommitTime(name));
        out.put("fileExtension", FSUtils.getFileExtension(name));
        // writeToken is the token between fileId and commitTime; FSUtils doesn't expose
        // a public getter, so reconstruct it.
        String stem = name.substring(0, name.lastIndexOf('.'));
        String[] parts = stem.split("_");
        if (parts.length >= 3) {
          // fileId may itself contain underscores (it doesn't with default key gen but
          // be defensive). Take everything between the first '_' and the last '_'.
          int firstUs = name.indexOf('_');
          int lastUs = stem.lastIndexOf('_');
          if (firstUs > 0 && lastUs > firstUs) {
            out.put("writeToken", name.substring(firstUs + 1, lastUs));
          }
        }
      } else {
        // Looks like just a file ID with no commit/token.
        out.put("kind", "fileIdOnly");
        out.put("fileId", name);
      }
    } catch (Exception e) {
      out.put("kind", "unrecognized");
      out.put("error", e.getClass().getSimpleName() + ": " + e.getMessage());
    }

    if (parsed.output == OutputFormat.JSON) {
      System.out.println(JSON_MAPPER.writeValueAsString(out));
    } else {
      for (Map.Entry<String, Object> e : out.entrySet()) {
        System.out.printf("%-20s : %s%n", e.getKey(), e.getValue());
      }
    }
  }

  // ---- raw-archive ----------------------------------------------------------

  /**
   * Walks the archive log files under {@code .hoodie/archived/} and prints the full
   * {@link HoodieArchivedMetaEntry} for the requested instant. Bypasses the in-memory
   * cache built by {@link HoodieArchivedTimeline} so we can see every sibling field --
   * useful when {@code --show-instant} reports "(no body)" but you want to confirm
   * whether the archive actually carried the metadata under a different field name.
   */
  private void rawArchive(HoodieTableMetaClient metaClient, Args parsed) throws IOException, ExitException {
    String needle = parsed.rawArchiveInstant;
    org.apache.hudi.storage.StoragePath archiveDir =
        new org.apache.hudi.storage.StoragePath(metaClient.getArchivePath().toString());
    org.apache.hudi.storage.HoodieStorage storage = metaClient.getStorage();
    List<org.apache.hudi.storage.StoragePathInfo> files;
    try {
      files = storage.listDirectEntries(archiveDir);
    } catch (java.io.FileNotFoundException nf) {
      throw new ExitException(3, "archive dir not found: " + archiveDir);
    }
    if (files == null || files.isEmpty()) {
      throw new ExitException(3, "archive dir is empty: " + archiveDir);
    }
    // Filter to archive log files.
    List<org.apache.hudi.storage.StoragePathInfo> archiveFiles = new ArrayList<>();
    for (org.apache.hudi.storage.StoragePathInfo spi : files) {
      if (spi.getPath().getName().startsWith("commits")) {
        archiveFiles.add(spi);
      }
    }
    if (archiveFiles.isEmpty()) {
      throw new ExitException(3, "no archive log files found in " + archiveDir);
    }

    List<Map<String, Object>> hits = new ArrayList<>();
    for (org.apache.hudi.storage.StoragePathInfo spi : archiveFiles) {
      if (!parsed.quiet) {
        System.err.println("scanning " + spi.getPath().getName());
      }
      try (HoodieLogFormat.Reader reader = HoodieLogFormat.newReader(
          metaClient,
          new HoodieLogFile(spi.getPath().toString()),
          org.apache.hudi.common.schema.HoodieSchema.fromAvroSchema(
              HoodieArchivedMetaEntry.getClassSchema()))) {
        while (reader.hasNext()) {
          HoodieLogBlock block = reader.next();
          if (!(block instanceof HoodieAvroDataBlock)) {
            continue;
          }
          HoodieAvroDataBlock avroBlock = (HoodieAvroDataBlock) block;
          try (ClosableIterator<HoodieRecord<IndexedRecord>> itr =
                   avroBlock.getRecordIterator(HoodieRecordType.AVRO)) {
            while (itr.hasNext()) {
              GenericRecord rec = (GenericRecord) itr.next().getData();
              Object ts = rec.get("commitTime");
              if (ts != null && ts.toString().equals(needle)) {
                Map<String, Object> entry = new LinkedHashMap<>();
                entry.put("archiveFile", spi.getPath().getName());
                Map<String, Object> populatedFields = new LinkedHashMap<>();
                Map<String, Object> nullFields = new LinkedHashMap<>();
                for (Schema.Field f : rec.getSchema().getFields()) {
                  Object v = rec.get(f.name());
                  if (v == null) {
                    nullFields.put(f.name(), null);
                  } else {
                    // Avro GenericRecord's toString() emits Avro-JSON. Re-parse via Jackson
                    // so the output is uniform JSON rather than Avro's quirky variant.
                    try {
                      populatedFields.put(f.name(), JSON_MAPPER.readTree(v.toString()));
                    } catch (Exception parseFail) {
                      populatedFields.put(f.name(), v.toString());
                    }
                  }
                }
                entry.put("populatedFields", populatedFields);
                entry.put("nullFields", nullFields.keySet());
                hits.add(entry);
              }
            }
          }
        }
      } catch (Exception e) {
        if (!parsed.quiet) {
          System.err.println("WARN: failed to read " + spi.getPath().getName() + ": " + e.getMessage());
        }
      }
    }

    if (hits.isEmpty()) {
      System.out.println("(no archive entries with commitTime=" + needle + ")");
      throw new ExitException(3, "no archive entries with commitTime=" + needle);
    }

    Map<String, Object> root = new LinkedHashMap<>();
    root.put("instant", needle);
    root.put("hitCount", hits.size());
    root.put("entries", hits);
    System.out.println(JSON_MAPPER.writeValueAsString(root));
  }

  // ---- commit-stats ---------------------------------------------------------

  /**
   * Lists per-ingestion-commit stats (records + files + bytes) across the active timeline
   * and, when not suppressed, the archived timeline, restricted by the optional time range
   * and action filter. Considers only completed commit/deltacommit/replacecommit instants.
   * Prints a footer with totals and per-commit averages for inserts/updates/deletes.
   *
   * <p>"Files deleted" semantics: for a replacecommit, the count of file ids in
   * {@code partitionToReplaceFileIds} (logical replacement). For plain commit/deltacommit,
   * this is always 0 — physical deletions live on clean instants, not on commit metadata.
   */
  private void commitStats(HoodieTableMetaClient metaClient, Args parsed) throws IOException {
    Set<String> actions = parsed.actionFilter.isEmpty()
        ? new HashSet<>(Arrays.asList(HoodieTimeline.COMMIT_ACTION,
            HoodieTimeline.DELTA_COMMIT_ACTION, HoodieTimeline.REPLACE_COMMIT_ACTION))
        : parsed.actionFilter.stream()
            .filter(a -> a.equals(HoodieTimeline.COMMIT_ACTION)
                || a.equals(HoodieTimeline.DELTA_COMMIT_ACTION)
                || a.equals(HoodieTimeline.REPLACE_COMMIT_ACTION))
            .collect(Collectors.toCollection(HashSet::new));
    if (actions.isEmpty()) {
      throw new IllegalArgumentException("--commit-stats: --actions filter must include at least"
          + " one of commit/deltacommit/replacecommit");
    }

    List<CommitStatsRow> rows = new ArrayList<>();
    HoodieActiveTimeline active = metaClient.reloadActiveTimeline();
    collectCommitStats(active, "ACTIVE", actions, parsed, rows);

    if (parsed.includeArchived) {
      HoodieArchivedTimeline archived = metaClient.getArchivedTimeline();
      if (parsed.startInstant != null && parsed.endInstant != null) {
        archived.loadInstantDetailsInMemory(parsed.startInstant, parsed.endInstant);
      } else {
        archived.loadCompletedInstantDetailsInMemory();
      }
      collectCommitStats(archived, "ARCHIVED", actions, parsed, rows);
    }

    Comparator<CommitStatsRow> byInstant = Comparator.comparing(r -> r.instant);
    if (parsed.sortDescending) {
      byInstant = byInstant.reversed();
    }
    rows.sort(byInstant);
    List<CommitStatsRow> capped = rows.size() > parsed.limit ? rows.subList(0, parsed.limit) : rows;

    CommitStatsTotals totals = CommitStatsTotals.from(rows);

    if (parsed.output == OutputFormat.JSON) {
      Map<String, Object> root = new LinkedHashMap<>();
      root.put("startInstant", parsed.startInstant);
      root.put("endInstant", parsed.endInstant);
      root.put("actions", new ArrayList<>(actions));
      root.put("totalCommits", rows.size());
      root.put("returnedCommits", capped.size());
      List<Map<String, Object>> commits = new ArrayList<>(capped.size());
      for (CommitStatsRow r : capped) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("instant", r.instant);
        m.put("timeline", r.timelineType);
        m.put("action", r.action);
        m.put("operation", r.operation);
        m.put("numInserts", r.numInserts);
        m.put("numUpdates", r.numUpdates);
        m.put("numDeletes", r.numDeletes);
        m.put("filesInserted", r.filesInserted);
        m.put("filesUpdated", r.filesUpdated);
        m.put("filesDeleted", r.filesDeleted);
        m.put("bytesWritten", r.bytesWritten);
        m.put("partitions", r.partitions);
        commits.add(m);
      }
      root.put("commits", commits);
      Map<String, Object> footer = new LinkedHashMap<>();
      footer.put("totalInserts", totals.totalInserts);
      footer.put("totalUpdates", totals.totalUpdates);
      footer.put("totalDeletes", totals.totalDeletes);
      footer.put("avgInsertsPerCommit", totals.avgInserts());
      footer.put("avgUpdatesPerCommit", totals.avgUpdates());
      footer.put("avgDeletesPerCommit", totals.avgDeletes());
      root.put("totals", footer);
      System.out.println(JSON_MAPPER.writeValueAsString(root));
      return;
    }

    if (capped.isEmpty()) {
      System.out.println("(no ingestion commits in range)");
      return;
    }
    String[] headers = {"instant", "timeline", "action", "operation",
        "numInserts", "numUpdates", "numDeletes",
        "filesInserted", "filesUpdated", "filesDeleted",
        "bytesWritten", "partitions"};
    List<String[]> tableRows = new ArrayList<>(capped.size());
    for (CommitStatsRow r : capped) {
      tableRows.add(new String[] {
          r.instant, r.timelineType, r.action, r.operation,
          Long.toString(r.numInserts), Long.toString(r.numUpdates), Long.toString(r.numDeletes),
          Long.toString(r.filesInserted), Long.toString(r.filesUpdated), Long.toString(r.filesDeleted),
          Long.toString(r.bytesWritten), Long.toString(r.partitions)
      });
    }
    printRowsWithHeaders(headers, tableRows);
    if (rows.size() > parsed.limit) {
      System.out.println();
      System.out.println("(showing first " + parsed.limit + " of " + rows.size()
          + " commits; raise with --limit to see more)");
    }
    System.out.println();
    System.out.printf("totals (over %d commits): inserts=%d updates=%d deletes=%d%n",
        rows.size(), totals.totalInserts, totals.totalUpdates, totals.totalDeletes);
    System.out.printf("avg per commit:           inserts=%.2f updates=%.2f deletes=%.2f%n",
        totals.avgInserts(), totals.avgUpdates(), totals.avgDeletes());
  }

  private void collectCommitStats(HoodieTimeline timeline, String timelineType,
                                  Set<String> actions, Args parsed, List<CommitStatsRow> out) {
    timeline.getInstantsAsStream()
        .filter(HoodieInstant::isCompleted)
        .filter(i -> actions.contains(i.getAction()))
        .filter(i -> parsed.startInstant == null || i.requestedTime().compareTo(parsed.startInstant) >= 0)
        .filter(i -> parsed.endInstant == null || i.requestedTime().compareTo(parsed.endInstant) <= 0)
        .forEach(instant -> {
          try {
            out.add(buildCommitStatsRow(timeline, timelineType, instant));
          } catch (Exception e) {
            if (!parsed.quiet) {
              System.err.println("WARN: failed to read " + timelineType + " " + instant
                  + ": " + e.getMessage());
            }
          }
        });
  }

  /**
   * Builds a stats row by first trying the POJO path (works for the active timeline) and,
   * if that yields an empty shell (which happens on the archived timeline where the bytes
   * are an Avro-flavored JSON string Jackson can't bind cleanly), falling back to parsing
   * the JSON tree and aggregating write-stat fields directly.
   */
  private CommitStatsRow buildCommitStatsRow(HoodieTimeline timeline,
                                             String timelineType,
                                             HoodieInstant instant) throws IOException {
    boolean isReplace = instant.getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION);

    Option<byte[]> rawOpt = timeline.getInstantDetails(instant);
    byte[] raw = rawOpt.isPresent() ? rawOpt.get() : new byte[0];

    // First try the POJO path.
    HoodieCommitMetadata pojo = null;
    try {
      if (raw.length > 0) {
        pojo = isReplace
            ? timeline.readInstantContent(instant, HoodieReplaceCommitMetadata.class)
            : timeline.readInstantContent(instant, HoodieCommitMetadata.class);
      }
    } catch (Exception ignored) {
      // fall through to JSON-tree path
    }

    if (pojo != null && pojo.getPartitionToWriteStats() != null
        && !pojo.getPartitionToWriteStats().isEmpty()) {
      return CommitStatsRow.fromPojo(instant, timelineType, pojo, isReplace);
    }

    // Archived-shell or empty body: aggregate from JSON.
    if (raw.length == 0) {
      return CommitStatsRow.empty(instant, timelineType);
    }
    return CommitStatsRow.fromJson(instant, timelineType, JSON_MAPPER.readTree(raw), isReplace);
  }

  // ---- phase-timings --------------------------------------------------------

  /**
   * Lists per-ingestion-instant wall-clock phase splits derived from the {@code .hoodie/}
   * state-marker file modification times on the active timeline. The six phase boundaries
   * follow the typical write sequence:
   * <pre>
   *   T0 = mtime of  &lt;basePath&gt;/.hoodie/&lt;ts&gt;.&lt;action&gt;.requested
   *   T1 = mtime of  &lt;basePath&gt;/.hoodie/&lt;ts&gt;.&lt;action&gt;.inflight
   *   T2 = mtime of  &lt;basePath&gt;/.hoodie/metadata/.hoodie/&lt;ts&gt;.deltacommit.requested  (MDT)
   *   T3 = mtime of  &lt;basePath&gt;/.hoodie/metadata/.hoodie/&lt;ts&gt;.deltacommit.inflight    (MDT)
   *   T4 = mtime of  &lt;basePath&gt;/.hoodie/metadata/.hoodie/&lt;ts&gt;.deltacommit              (MDT)
   *   T5 = mtime of  &lt;basePath&gt;/.hoodie/&lt;ts&gt;.&lt;action&gt; (or .commit for compaction)
   * </pre>
   * Phase columns:
   * <ul>
   *   <li>sourceReadIndexSfh_ms = T1 - T0 (data table: source read + indexing + small-file handling)</li>
   *   <li>dataWrite_ms = T2 - T1 (data table writes; when MDT is absent, T5 - T1)</li>
   *   <li>mdtPrep_ms = T3 - T2 (metadata record preparation)</li>
   *   <li>mdtWrite_ms = T4 - T3 (metadata writes)</li>
   *   <li>mdtTail_ms = T5 - T4 (gap between MDT completion and data-table completion)</li>
   *   <li>total_ms = T5 - T0</li>
   * </ul>
   *
   * <p>Per-instant MDT detection: if the MDT timeline directory does not exist on the
   * table at all, all rows fall back to the 3-phase view (sourceReadIndexSfh, dataWrite,
   * total). Otherwise, MDT phases are populated when the three MDT state files exist for
   * that instant, and left blank for instants that predate MDT enablement.
   *
   * <p>Rows where any expected data-table state file is missing (e.g. partial/rolled-back
   * writes) are dropped from the output and counted in the footer's {@code skipped} block.
   * Archived instants are skipped — their state-marker files no longer exist on disk.
   *
   * <p>S3/GCS caveat: object-store mtimes reflect server-side write-completion time, not
   * the producer's wall clock. The relative phase durations are still accurate; clock-skew
   * artifacts are bounded by the storage layer's commit granularity.
   */
  private void phaseTimings(HoodieTableMetaClient metaClient, Args parsed) throws IOException {
    // Validate / build the action set. Default: deltacommit + commit (MOR + COW ingest).
    Set<String> baseActions = new HashSet<>(Arrays.asList(
        HoodieTimeline.COMMIT_ACTION, HoodieTimeline.DELTA_COMMIT_ACTION));
    if (parsed.includeReplacecommit) {
      baseActions.add(HoodieTimeline.REPLACE_COMMIT_ACTION);
    }
    Set<String> actions;
    if (parsed.actionFilter.isEmpty()) {
      actions = baseActions;
    } else {
      // Explicit --actions overrides the default but is still restricted to ingest-style
      // actions; phase semantics don't apply to clean/rollback/savepoint.
      Set<String> validForPhaseTimings = new HashSet<>(Arrays.asList(
          HoodieTimeline.COMMIT_ACTION, HoodieTimeline.DELTA_COMMIT_ACTION,
          HoodieTimeline.REPLACE_COMMIT_ACTION));
      actions = parsed.actionFilter.stream()
          .filter(validForPhaseTimings::contains)
          .collect(Collectors.toCollection(HashSet::new));
      if (actions.isEmpty()) {
        throw new IllegalArgumentException("--phase-timings: --actions filter must include at"
            + " least one of commit/deltacommit/replacecommit");
      }
    }

    org.apache.hudi.storage.HoodieStorage storage = metaClient.getStorage();
    org.apache.hudi.storage.StoragePath dataMetaDir =
        new org.apache.hudi.storage.StoragePath(metaClient.getTimelinePath().toString());
    // Resolve the MDT timeline directory. Try building an MDT metaClient first (handles
    // both V1 and V2 layout correctly); if that fails (e.g. no hoodie.properties in the
    // MDT directory), probe known paths directly.
    String mdtBasePath = metaClient.getBasePath()
        + "/" + HoodieTableMetaClient.METADATA_TABLE_FOLDER_PATH;
    HoodieTableMetaClient mdtMetaClient = null;
    org.apache.hudi.storage.StoragePath mdtMetaDir = null;
    boolean mdtPresent = false;
    try {
      mdtMetaClient = HoodieTableMetaClient.builder()
          .setConf(metaClient.getStorageConf())
          .setBasePath(mdtBasePath)
          .build();
      mdtMetaDir = new org.apache.hudi.storage.StoragePath(
          mdtMetaClient.getTimelinePath().toString());
      mdtPresent = storage.exists(mdtMetaDir);
    } catch (Exception e) {
      // MDT hoodie.properties missing — probe known paths directly.
      // Try V2 path (<base>/.hoodie/metadata/.hoodie/timeline/) first, then V1 (<base>/.hoodie/metadata/.hoodie/).
      org.apache.hudi.storage.StoragePath v2 = new org.apache.hudi.storage.StoragePath(
          mdtBasePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME
              + "/" + HoodieTableMetaClient.TIMELINEFOLDER_NAME);
      org.apache.hudi.storage.StoragePath v1 = new org.apache.hudi.storage.StoragePath(
          mdtBasePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME);
      try {
        if (storage.exists(v2)) {
          mdtMetaDir = v2;
          mdtPresent = true;
        } else if (storage.exists(v1)) {
          mdtMetaDir = v1;
          mdtPresent = true;
        }
      } catch (IOException ioe) {
        // leave mdtPresent = false
      }
    }

    // Collect candidate completed instants from the active timeline.
    HoodieActiveTimeline active = metaClient.reloadActiveTimeline();
    List<HoodieInstant> candidates = active.getInstantsAsStream()
        .filter(HoodieInstant::isCompleted)
        .filter(i -> actions.contains(i.getAction()))
        .filter(i -> parsed.startInstant == null || i.requestedTime().compareTo(parsed.startInstant) >= 0)
        .filter(i -> parsed.endInstant == null || i.requestedTime().compareTo(parsed.endInstant) <= 0)
        .collect(Collectors.toList());

    List<PhaseTimingRow> rows = new ArrayList<>();
    Map<String, Integer> skipReasons = new LinkedHashMap<>();
    int skipped = 0;

    for (HoodieInstant instant : candidates) {
      PhaseTimingRow row = buildPhaseTimingRow(metaClient, mdtMetaClient, storage, dataMetaDir,
          mdtMetaDir, mdtPresent, instant, skipReasons);
      if (row == null) {
        skipped++;
        continue;
      }
      rows.add(row);
    }

    // Sort + cap.
    Comparator<PhaseTimingRow> byInstant = Comparator.comparing(r -> r.instant);
    if (parsed.sortDescending) {
      byInstant = byInstant.reversed();
    }
    rows.sort(byInstant);
    List<PhaseTimingRow> capped = rows.size() > parsed.limit
        ? rows.subList(0, parsed.limit) : rows;

    Map<String, PhaseTimingAggregate> perAction = new LinkedHashMap<>();
    for (PhaseTimingRow r : capped) {
      perAction.computeIfAbsent(r.action, k -> new PhaseTimingAggregate(k)).add(r);
    }

    if (parsed.output == OutputFormat.JSON) {
      emitPhaseTimingsJson(parsed, mdtPresent, rows, capped, perAction, skipReasons, skipped);
      return;
    }
    emitPhaseTimingsTable(parsed, mdtPresent, rows, capped, perAction, skipReasons, skipped);
  }

  /**
   * Builds one row by stat'ing the six expected state-marker files. Returns {@code null}
   * if any data-table state file is missing; in that case, increments {@code skipReasons}
   * accordingly so the caller can report a breakdown. MDT files being absent is fine — the
   * row falls back to the 3-phase view.
   */
  private PhaseTimingRow buildPhaseTimingRow(HoodieTableMetaClient metaClient,
                                             HoodieTableMetaClient mdtMetaClient,
                                             org.apache.hudi.storage.HoodieStorage storage,
                                             org.apache.hudi.storage.StoragePath dataMetaDir,
                                             org.apache.hudi.storage.StoragePath mdtMetaDir,
                                             boolean mdtPresent,
                                             HoodieInstant completed,
                                             Map<String, Integer> skipReasons) {
    String ts = completed.requestedTime();
    String action = completed.getAction();
    org.apache.hudi.common.table.timeline.InstantGenerator ig = metaClient.getInstantGenerator();
    org.apache.hudi.common.table.timeline.InstantFileNameGenerator fng = metaClient.getInstantFileNameGenerator();
    HoodieInstant requested = ig.createNewInstant(HoodieInstant.State.REQUESTED, action, ts);
    HoodieInstant inflight = ig.createNewInstant(HoodieInstant.State.INFLIGHT, action, ts);

    Long t0 = mtimeOrNull(storage, new org.apache.hudi.storage.StoragePath(dataMetaDir, fng.getFileName(requested)));
    Long t1 = mtimeOrNull(storage, new org.apache.hudi.storage.StoragePath(dataMetaDir, fng.getFileName(inflight)));
    Long t5 = mtimeOrNull(storage, new org.apache.hudi.storage.StoragePath(dataMetaDir, fng.getFileName(completed)));

    if (t0 == null || t1 == null || t5 == null) {
      String reason = (t0 == null ? "requested-missing"
          : t1 == null ? "inflight-missing" : "completed-missing");
      skipReasons.merge(reason, 1, Integer::sum);
      return null;
    }

    Long t2 = null;
    Long t3 = null;
    Long t4 = null;
    if (mdtPresent) {
      // Use MDT metaClient's generators when available; fall back to data table's generators
      // (safe because MDT always shares the same table version as the data table).
      org.apache.hudi.common.table.timeline.InstantGenerator mdtIg =
          mdtMetaClient != null ? mdtMetaClient.getInstantGenerator() : ig;
      org.apache.hudi.common.table.timeline.InstantFileNameGenerator mdtFng =
          mdtMetaClient != null ? mdtMetaClient.getInstantFileNameGenerator() : fng;
      // MDT timeline always uses DELTA_COMMIT_ACTION regardless of data table type.
      String mdtAction = HoodieTimeline.DELTA_COMMIT_ACTION;
      HoodieInstant mdtReq = mdtIg.createNewInstant(HoodieInstant.State.REQUESTED, mdtAction, ts);
      HoodieInstant mdtInf = mdtIg.createNewInstant(HoodieInstant.State.INFLIGHT, mdtAction, ts);
      t2 = mtimeOrNull(storage, new org.apache.hudi.storage.StoragePath(mdtMetaDir, mdtFng.getFileName(mdtReq)));
      t3 = mtimeOrNull(storage, new org.apache.hudi.storage.StoragePath(mdtMetaDir, mdtFng.getFileName(mdtInf)));
      // The completed MDT file carries a completion-time suffix in V2 layout
      // (<ts>_<completionTime>.deltacommit), which we don't know a priori. Match by
      // prefix + extension in the MDT timeline dir instead of synthesizing the full name.
      t4 = mtimeByPrefixOrNull(storage, mdtMetaDir, ts, "." + mdtAction);
      // Partial MDT state for this instant → fall back to the 3-phase view (treat as if
      // MDT wasn't present for this row). Don't skip the row.
      if (t2 == null || t3 == null || t4 == null) {
        t2 = null;
        t3 = null;
        t4 = null;
      }
    }

    return new PhaseTimingRow(ts, action, t0, t1, t2, t3, t4, t5);
  }

  private static Long mtimeOrNull(org.apache.hudi.storage.HoodieStorage storage,
                                   org.apache.hudi.storage.StoragePath p) {
    try {
      return storage.getPathInfo(p).getModificationTime();
    } catch (java.io.FileNotFoundException nf) {
      return null;
    } catch (IOException ioe) {
      return null;
    }
  }

  /**
   * Returns the mtime of the file under {@code dir} whose name starts with
   * {@code namePrefix} and ends with {@code nameSuffix}. Used to locate completed instant
   * files in V2 layout, where the on-disk name embeds a completion-time we don't know
   * a priori (e.g., "20260601000000000_20260601000000234.deltacommit").
   */
  private static Long mtimeByPrefixOrNull(org.apache.hudi.storage.HoodieStorage storage,
                                          org.apache.hudi.storage.StoragePath dir,
                                          String namePrefix, String nameSuffix) {
    try {
      List<org.apache.hudi.storage.StoragePathInfo> entries = storage.listDirectEntries(dir);
      if (entries == null) {
        return null;
      }
      for (org.apache.hudi.storage.StoragePathInfo e : entries) {
        String n = e.getPath().getName();
        if (n.startsWith(namePrefix) && n.endsWith(nameSuffix)) {
          return e.getModificationTime();
        }
      }
      return null;
    } catch (IOException ioe) {
      return null;
    }
  }

  private void emitPhaseTimingsTable(Args parsed, boolean mdtPresent,
                                     List<PhaseTimingRow> allRows,
                                     List<PhaseTimingRow> capped,
                                     Map<String, PhaseTimingAggregate> perAction,
                                     Map<String, Integer> skipReasons,
                                     int skipped) {
    if (!mdtPresent) {
      System.out.println("(metadata table directory not found — MDT phases will be blank)");
      System.out.println();
    }
    if (capped.isEmpty()) {
      System.out.println("(no ingestion instants with complete phase timings in range)");
      if (skipped > 0) {
        System.out.println("skipped " + skipped + " instant(s): "
            + formatSkipReasons(skipReasons));
      }
      return;
    }
    String[] headers = {"instant", "action",
        "sourceReadIndexSfh_ms", "dataWrite_ms", "mdtPrep_ms", "mdtWrite_ms",
        "mdtTail_ms", "total_ms"};
    List<String[]> tableRows = new ArrayList<>(capped.size());
    for (PhaseTimingRow r : capped) {
      tableRows.add(new String[]{
          r.instant, r.action,
          formatMs(r.sourceReadIndexSfhMs()),
          formatMs(r.dataWriteMs()),
          formatMs(r.mdtPrepMs()),
          formatMs(r.mdtWriteMs()),
          formatMs(r.mdtTailMs()),
          formatMs(r.totalMs())
      });
    }
    printRowsWithHeaders(headers, tableRows);

    if (allRows.size() > parsed.limit) {
      System.out.println();
      System.out.println("(showing first " + parsed.limit + " of " + allRows.size()
          + " instants; raise with --limit to see more)");
    }
    System.out.println();
    System.out.println("aggregates (per action, ms):");
    for (PhaseTimingAggregate agg : perAction.values()) {
      System.out.println("  " + agg.action + " (n=" + agg.count() + "):");
      System.out.printf("    sourceReadIndexSfh:  mean=%.0f p50=%d p95=%d max=%d%n",
          agg.mean(PhaseTimingAggregate.Phase.SOURCE_READ),
          agg.percentile(PhaseTimingAggregate.Phase.SOURCE_READ, 50),
          agg.percentile(PhaseTimingAggregate.Phase.SOURCE_READ, 95),
          agg.max(PhaseTimingAggregate.Phase.SOURCE_READ));
      System.out.printf("    dataWrite:           mean=%.0f p50=%d p95=%d max=%d%n",
          agg.mean(PhaseTimingAggregate.Phase.DATA_WRITE),
          agg.percentile(PhaseTimingAggregate.Phase.DATA_WRITE, 50),
          agg.percentile(PhaseTimingAggregate.Phase.DATA_WRITE, 95),
          agg.max(PhaseTimingAggregate.Phase.DATA_WRITE));
      if (agg.hasMdt()) {
        System.out.printf("    mdtPrep:             mean=%.0f p50=%d p95=%d max=%d%n",
            agg.mean(PhaseTimingAggregate.Phase.MDT_PREP),
            agg.percentile(PhaseTimingAggregate.Phase.MDT_PREP, 50),
            agg.percentile(PhaseTimingAggregate.Phase.MDT_PREP, 95),
            agg.max(PhaseTimingAggregate.Phase.MDT_PREP));
        System.out.printf("    mdtWrite:            mean=%.0f p50=%d p95=%d max=%d%n",
            agg.mean(PhaseTimingAggregate.Phase.MDT_WRITE),
            agg.percentile(PhaseTimingAggregate.Phase.MDT_WRITE, 50),
            agg.percentile(PhaseTimingAggregate.Phase.MDT_WRITE, 95),
            agg.max(PhaseTimingAggregate.Phase.MDT_WRITE));
        System.out.printf("    mdtTail:             mean=%.0f p50=%d p95=%d max=%d%n",
            agg.mean(PhaseTimingAggregate.Phase.MDT_TAIL),
            agg.percentile(PhaseTimingAggregate.Phase.MDT_TAIL, 50),
            agg.percentile(PhaseTimingAggregate.Phase.MDT_TAIL, 95),
            agg.max(PhaseTimingAggregate.Phase.MDT_TAIL));
      }
      System.out.printf("    total:               mean=%.0f p50=%d p95=%d max=%d%n",
          agg.mean(PhaseTimingAggregate.Phase.TOTAL),
          agg.percentile(PhaseTimingAggregate.Phase.TOTAL, 50),
          agg.percentile(PhaseTimingAggregate.Phase.TOTAL, 95),
          agg.max(PhaseTimingAggregate.Phase.TOTAL));
      // phaseShareOfTotal — fraction of mean total each phase consumes on average.
      double totalMean = agg.mean(PhaseTimingAggregate.Phase.TOTAL);
      if (totalMean > 0) {
        StringBuilder share = new StringBuilder("    share of total (mean):  ");
        share.append("sourceReadIndexSfh=")
            .append(String.format("%.1f%%", 100.0 * agg.mean(
                PhaseTimingAggregate.Phase.SOURCE_READ) / totalMean));
        share.append(", dataWrite=")
            .append(String.format("%.1f%%", 100.0 * agg.mean(
                PhaseTimingAggregate.Phase.DATA_WRITE) / totalMean));
        if (agg.hasMdt()) {
          share.append(", mdtPrep=")
              .append(String.format("%.1f%%", 100.0 * agg.mean(
                  PhaseTimingAggregate.Phase.MDT_PREP) / totalMean));
          share.append(", mdtWrite=")
              .append(String.format("%.1f%%", 100.0 * agg.mean(
                  PhaseTimingAggregate.Phase.MDT_WRITE) / totalMean));
          share.append(", mdtTail=")
              .append(String.format("%.1f%%", 100.0 * agg.mean(
                  PhaseTimingAggregate.Phase.MDT_TAIL) / totalMean));
        }
        System.out.println(share);
      }
    }
    if (skipped > 0) {
      System.out.println();
      System.out.println("skipped " + skipped + " instant(s) with incomplete state-marker files: "
          + formatSkipReasons(skipReasons));
    }
  }

  private void emitPhaseTimingsJson(Args parsed, boolean mdtPresent,
                                    List<PhaseTimingRow> allRows,
                                    List<PhaseTimingRow> capped,
                                    Map<String, PhaseTimingAggregate> perAction,
                                    Map<String, Integer> skipReasons,
                                    int skipped) throws IOException {
    Map<String, Object> root = new LinkedHashMap<>();
    root.put("startInstant", parsed.startInstant);
    root.put("endInstant", parsed.endInstant);
    root.put("includeReplacecommit", parsed.includeReplacecommit);
    root.put("mdtPresent", mdtPresent);
    root.put("totalInstants", allRows.size());
    root.put("returnedInstants", capped.size());
    List<Map<String, Object>> rows = new ArrayList<>(capped.size());
    for (PhaseTimingRow r : capped) {
      Map<String, Object> m = new LinkedHashMap<>();
      m.put("instant", r.instant);
      m.put("action", r.action);
      m.put("sourceReadIndexSfhMs", r.sourceReadIndexSfhMs());
      m.put("dataWriteMs", r.dataWriteMs());
      m.put("mdtPrepMs", r.mdtPrepMs());
      m.put("mdtWriteMs", r.mdtWriteMs());
      m.put("mdtTailMs", r.mdtTailMs());
      m.put("totalMs", r.totalMs());
      rows.add(m);
    }
    root.put("instants", rows);
    Map<String, Object> aggBlock = new LinkedHashMap<>();
    for (Map.Entry<String, PhaseTimingAggregate> e : perAction.entrySet()) {
      Map<String, Object> a = new LinkedHashMap<>();
      a.put("count", e.getValue().count());
      a.put("hasMdt", e.getValue().hasMdt());
      a.put("sourceReadIndexSfh", phaseStats(e.getValue(), PhaseTimingAggregate.Phase.SOURCE_READ));
      a.put("dataWrite", phaseStats(e.getValue(), PhaseTimingAggregate.Phase.DATA_WRITE));
      if (e.getValue().hasMdt()) {
        a.put("mdtPrep", phaseStats(e.getValue(), PhaseTimingAggregate.Phase.MDT_PREP));
        a.put("mdtWrite", phaseStats(e.getValue(), PhaseTimingAggregate.Phase.MDT_WRITE));
        a.put("mdtTail", phaseStats(e.getValue(), PhaseTimingAggregate.Phase.MDT_TAIL));
      }
      a.put("total", phaseStats(e.getValue(), PhaseTimingAggregate.Phase.TOTAL));
      aggBlock.put(e.getKey(), a);
    }
    root.put("aggregates", aggBlock);
    Map<String, Object> skip = new LinkedHashMap<>();
    skip.put("count", skipped);
    skip.put("reasons", skipReasons);
    root.put("skipped", skip);
    System.out.println(JSON_MAPPER.writeValueAsString(root));
  }

  private static Map<String, Object> phaseStats(PhaseTimingAggregate agg,
                                                PhaseTimingAggregate.Phase phase) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("mean", agg.mean(phase));
    m.put("p50", agg.percentile(phase, 50));
    m.put("p95", agg.percentile(phase, 95));
    m.put("max", agg.max(phase));
    return m;
  }

  private static String formatMs(Long v) {
    return v == null ? "-" : Long.toString(v);
  }

  private static String formatSkipReasons(Map<String, Integer> reasons) {
    if (reasons == null || reasons.isEmpty()) {
      return "(no breakdown)";
    }
    return reasons.entrySet().stream()
        .map(e -> e.getKey() + "=" + e.getValue())
        .collect(Collectors.joining(", "));
  }

  // ---- show-instant ---------------------------------------------------------

  private void showInstant(HoodieTableMetaClient metaClient, Args parsed) throws IOException, ExitException {
    String instantTime = parsed.showInstantTime;
    List<InstantLocator> located = findInstantStates(metaClient, instantTime, parsed.actionFilter,
        parsed.stateFilter, parsed.includeArchived);
    if (located.isEmpty()) {
      System.err.println("No instant found with time=" + instantTime
          + " in active or archived timeline (action filter="
          + (parsed.actionFilter.isEmpty() ? "<any>" : String.join(",", parsed.actionFilter))
          + (parsed.stateFilter == null ? "" : ", state=" + parsed.stateFilter) + ")");
      throw new ExitException(3, "No instant found with time=" + instantTime);
    }

    if (parsed.output == OutputFormat.JSON) {
      Map<String, Object> root = new LinkedHashMap<>();
      root.put("instant", instantTime);
      List<Map<String, Object>> states = new ArrayList<>();
      for (InstantLocator loc : located) {
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("action", loc.instant.getAction());
        entry.put("state", loc.instant.getState().toString());
        entry.put("timeline", loc.timelineType);
        entry.put("completion", loc.instant.getCompletionTime());
        try {
          Object content = readContent(metaClient, loc);
          entry.put("content", content);
        } catch (Exception e) {
          entry.put("error", e.getClass().getSimpleName() + ": " + e.getMessage());
        }
        states.add(entry);
      }
      root.put("states", states);
      System.out.println(JSON_MAPPER.writeValueAsString(root));
      return;
    }

    if (located.size() > 1) {
      System.out.println("found " + located.size() + " state rows for instant=" + instantTime
          + " (" + located.stream().map(l -> l.instant.getState().toString())
              .collect(Collectors.joining(", ")) + ")");
      System.out.println();
    }
    boolean first = true;
    for (InstantLocator loc : located) {
      if (!first) {
        System.out.println();
        System.out.println("================================================================");
        System.out.println();
      }
      first = false;
      System.out.println("instant       : " + loc.instant.requestedTime());
      System.out.println("action        : " + loc.instant.getAction());
      System.out.println("state         : " + loc.instant.getState());
      System.out.println("timeline      : " + loc.timelineType);
      System.out.println("completion    : " + loc.instant.getCompletionTime());
      System.out.println();
      System.out.println("---- content ----");
      Object content;
      try {
        content = readContent(metaClient, loc);
      } catch (Exception e) {
        System.out.println("(failed to deserialize: " + e.getMessage() + ")");
        continue;
      }
      if (content == null) {
        System.out.println("(no content / instant has no body)");
      } else {
        System.out.println(JSON_MAPPER.writeValueAsString(content));
      }
    }
  }

  // ---- find-file-id ---------------------------------------------------------

  private void findFileId(HoodieTableMetaClient metaClient, Args parsed) throws IOException {
    String needle = parsed.fileIdNeedle;
    int limit = parsed.limit;

    HoodieActiveTimeline active = metaClient.reloadActiveTimeline();
    List<Event> events = new ArrayList<>();
    scanTimeline(metaClient, active, "ACTIVE", needle, parsed, events, limit);

    if (parsed.includeArchived && events.size() < limit) {
      HoodieArchivedTimeline archived = metaClient.getArchivedTimeline();
      if (parsed.startInstant != null && parsed.endInstant != null) {
        archived.loadInstantDetailsInMemory(parsed.startInstant, parsed.endInstant);
      } else {
        archived.loadCompletedInstantDetailsInMemory();
      }
      scanTimeline(metaClient, archived, "ARCHIVED", needle, parsed, events, limit);
    }

    events.sort(Comparator
        .comparing((Event e) -> e.instantTime)
        .thenComparing(e -> e.action)
        .thenComparing(e -> e.matchType));

    List<Event> capped = events.size() > limit ? events.subList(0, limit) : events;

    if (parsed.lifecycle) {
      emitLifecycle(needle, capped, events.size(), parsed);
      return;
    }

    if (parsed.output == OutputFormat.JSON) {
      Map<String, Object> root = new LinkedHashMap<>();
      root.put("needle", needle);
      root.put("totalEvents", events.size());
      root.put("returnedEvents", capped.size());
      List<Map<String, Object>> rows = new ArrayList<>(capped.size());
      for (Event e : capped) {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("instant", e.instantTime);
        r.put("timeline", e.timelineType);
        r.put("action", e.action);
        r.put("state", e.state);
        r.put("matchType", e.matchType);
        r.put("partition", e.partition);
        r.put("detail", e.detail);
        rows.add(r);
      }
      root.put("events", rows);
      System.out.println(JSON_MAPPER.writeValueAsString(root));
      return;
    }

    printEventTable(capped);
    if (events.size() > limit) {
      System.out.println();
      System.out.println("(showing first " + limit + " of " + events.size()
          + " events; raise with --limit to see more)");
    }
  }

  /**
   * Collapse the events list to lifecycle landmarks: created (first write-stat),
   * replaced (replaceFileId / clustering), and removed (clean / rollback). Prints a
   * focused table plus a summary line. Honors --output json|table.
   */
  private void emitLifecycle(String needle, List<Event> events, int totalEvents, Args parsed)
      throws IOException {
    Event created = null;
    Event replaced = null;
    Event cleaned = null;
    Event rolledBack = null;
    int writeStatCount = 0;

    for (Event e : events) {
      switch (e.matchType) {
        case "writeStat":
          writeStatCount++;
          if (created == null) {
            created = e;
          }
          break;
        case "replaceFileId":
        case "clusteringInputSlice":
          if (replaced == null
              || e.instantTime.compareTo(replaced.instantTime) < 0) {
            replaced = e;
          }
          break;
        case "cleanSuccessDelete":
        case "cleanFailedDelete":
        case "cleanDeletePattern":
          if (cleaned == null
              || e.instantTime.compareTo(cleaned.instantTime) < 0) {
            cleaned = e;
          }
          break;
        case "rollbackSuccessDelete":
        case "rollbackFailedDelete":
        case "rollbackPlanFile":
        case "rollbackPlanLogBlock":
        case "rollbackOfCommit":
        case "rollbackPlanTargetCommit":
          if (rolledBack == null
              || e.instantTime.compareTo(rolledBack.instantTime) < 0) {
            rolledBack = e;
          }
          break;
        default:
          break;
      }
    }

    List<String[]> rows = new ArrayList<>();
    if (created != null) {
      rows.add(landmarkRow("CREATED", created));
    }
    if (replaced != null) {
      rows.add(landmarkRow("REPLACED", replaced));
    }
    if (cleaned != null) {
      rows.add(landmarkRow("CLEANED", cleaned));
    }
    if (rolledBack != null) {
      rows.add(landmarkRow("ROLLED_BACK", rolledBack));
    }

    if (parsed.output == OutputFormat.JSON) {
      Map<String, Object> root = new LinkedHashMap<>();
      root.put("needle", needle);
      root.put("totalEvents", totalEvents);
      root.put("writeStatHits", writeStatCount);
      List<Map<String, Object>> landmarks = new ArrayList<>();
      addLandmarkJson(landmarks, "CREATED", created);
      addLandmarkJson(landmarks, "REPLACED", replaced);
      addLandmarkJson(landmarks, "CLEANED", cleaned);
      addLandmarkJson(landmarks, "ROLLED_BACK", rolledBack);
      root.put("landmarks", landmarks);
      System.out.println(JSON_MAPPER.writeValueAsString(root));
      return;
    }

    if (rows.isEmpty()) {
      System.out.println("(no lifecycle landmarks found for needle=" + needle + ")");
      return;
    }
    String[] headers = {"landmark", "instant", "action", "state", "timeline", "match_type", "partition", "detail"};
    printRowsWithHeaders(headers, rows);

    System.out.println();
    StringBuilder summary = new StringBuilder("summary: ");
    summary.append(writeStatCount).append(" writeStat hit(s)");
    if (created != null) {
      summary.append("; created @ ").append(created.instantTime)
          .append(" (").append(created.action).append(")");
    }
    if (replaced != null) {
      summary.append("; replaced @ ").append(replaced.instantTime)
          .append(" (").append(replaced.action).append(")");
    }
    if (cleaned != null) {
      summary.append("; cleaned @ ").append(cleaned.instantTime)
          .append(" (").append(cleaned.matchType).append(")");
    }
    if (rolledBack != null) {
      summary.append("; rolled-back @ ").append(rolledBack.instantTime)
          .append(" (").append(rolledBack.matchType).append(")");
    }
    summary.append("; ").append(totalEvents).append(" total matching events");
    System.out.println(summary.toString());
  }

  private String[] landmarkRow(String label, Event e) {
    return new String[]{
        label, e.instantTime, e.action, e.state, e.timelineType,
        e.matchType, e.partition == null ? "" : e.partition,
        e.detail == null ? "" : e.detail
    };
  }

  private void addLandmarkJson(List<Map<String, Object>> out, String label, Event e) {
    if (e == null) {
      return;
    }
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("landmark", label);
    m.put("instant", e.instantTime);
    m.put("action", e.action);
    m.put("state", e.state);
    m.put("timeline", e.timelineType);
    m.put("matchType", e.matchType);
    m.put("partition", e.partition);
    m.put("detail", e.detail);
    out.add(m);
  }

  private void scanTimeline(HoodieTableMetaClient metaClient,
                            HoodieTimeline timeline,
                            String timelineType,
                            String needle,
                            Args parsed,
                            List<Event> out,
                            int hardCap) {
    Stream<HoodieInstant> instants = timeline.getInstantsAsStream()
        .filter(i -> parsed.actionFilter.isEmpty() || parsed.actionFilter.contains(i.getAction()))
        .filter(i -> parsed.startInstant == null || i.requestedTime().compareTo(parsed.startInstant) >= 0)
        .filter(i -> parsed.endInstant == null || i.requestedTime().compareTo(parsed.endInstant) <= 0);

    for (HoodieInstant instant : (Iterable<HoodieInstant>) instants::iterator) {
      if (out.size() >= hardCap * 4L) {
        // 4x headroom so events from both timelines can be collected and sorted before capping.
        break;
      }
      try {
        scanInstant(metaClient, timeline, timelineType, instant, needle, out);
      } catch (Exception e) {
        // Best-effort scan: bad bytes on a single instant shouldn't kill the whole run.
        if (!parsed.quiet) {
          System.err.println("WARN: failed to process " + timelineType + " " + instant + ": " + e.getMessage());
        }
      }
    }
  }

  private void scanInstant(HoodieTableMetaClient metaClient,
                           HoodieTimeline timeline,
                           String timelineType,
                           HoodieInstant instant,
                           String needle,
                           List<Event> out) throws IOException {
    String action = instant.getAction();
    boolean isCompleted = instant.isCompleted();
    boolean isRequested = instant.isRequested();

    switch (action) {
      case HoodieTimeline.COMMIT_ACTION:
      case HoodieTimeline.DELTA_COMMIT_ACTION:
        if (isCompleted) {
          HoodieCommitMetadata cm = readCommitMetadata(timeline, instant, HoodieCommitMetadata.class);
          collectFromCommitMetadata(cm, instant, timelineType, needle, out);
        }
        break;

      case HoodieTimeline.REPLACE_COMMIT_ACTION:
        if (isCompleted) {
          HoodieReplaceCommitMetadata rcm =
              readCommitMetadata(timeline, instant, HoodieReplaceCommitMetadata.class);
          collectFromCommitMetadata(rcm, instant, timelineType, needle, out);
          if (rcm.getPartitionToReplaceFileIds() != null) {
            for (Map.Entry<String, List<String>> e : rcm.getPartitionToReplaceFileIds().entrySet()) {
              for (String replaced : e.getValue()) {
                if (matches(replaced, needle)) {
                  out.add(new Event(instant, timelineType, "replaceFileId", e.getKey(), replaced));
                }
              }
            }
          }
        } else if (isRequested) {
          // The requested-replace metadata may carry an embedded clustering plan.
          Option<HoodieRequestedReplaceMetadata> rrm = Option.empty();
          try {
            rrm = Option.of(readSpecificRecord(timeline, instant,
                HoodieRequestedReplaceMetadata.class, HoodieRequestedReplaceMetadata.getClassSchema()));
          } catch (Exception ignored) {
            // not all requested replacecommits carry a body
          }
          if (rrm.isPresent() && rrm.get().getClusteringPlan() != null) {
            collectFromClusteringPlan(rrm.get().getClusteringPlan(), instant, timelineType, needle, out);
          } else {
            // Fallback to the metaClient-aware helper (works against active timeline only).
            try {
              Option<org.apache.hudi.common.util.collection.Pair<HoodieInstant, HoodieClusteringPlan>> cp =
                  ClusteringUtils.getClusteringPlan(metaClient, instant);
              if (cp.isPresent()) {
                collectFromClusteringPlan(cp.get().getValue(), instant, timelineType, needle, out);
              }
            } catch (Exception ignored) {
              // archived-timeline path; rrm above already covered it
            }
          }
        }
        break;

      case HoodieTimeline.CLEAN_ACTION:
        if (isCompleted) {
          HoodieCleanMetadata cm = readSpecificRecord(timeline, instant,
              HoodieCleanMetadata.class, HoodieCleanMetadata.getClassSchema());
          collectFromCleanMetadata(cm, instant, timelineType, needle, out);
        }
        // Requested clean plan also lists files-to-delete, but the upstream commit
        // didn't surface it -- skip for now to keep output focused on actual deletions.
        break;

      case HoodieTimeline.ROLLBACK_ACTION:
        if (isCompleted) {
          HoodieRollbackMetadata rm = readSpecificRecord(timeline, instant,
              HoodieRollbackMetadata.class, HoodieRollbackMetadata.getClassSchema());
          collectFromRollbackMetadata(rm, instant, timelineType, needle, out);
        } else if (isRequested) {
          try {
            HoodieRollbackPlan plan = readSpecificRecord(timeline, instant,
                HoodieRollbackPlan.class, HoodieRollbackPlan.getClassSchema());
            collectFromRollbackPlan(plan, instant, timelineType, needle, out);
          } catch (Exception ignored) {
            // some old tables have empty requested rollback bodies
          }
        }
        break;

      case HoodieTimeline.RESTORE_ACTION:
        if (isCompleted) {
          try {
            HoodieRestoreMetadata rm = readSpecificRecord(timeline, instant,
                HoodieRestoreMetadata.class, HoodieRestoreMetadata.getClassSchema());
            collectFromRestoreMetadata(rm, instant, timelineType, needle, out);
          } catch (Exception ignored) {
            // best-effort
          }
        } else if (isRequested) {
          try {
            HoodieRestorePlan plan = readSpecificRecord(timeline, instant,
                HoodieRestorePlan.class, HoodieRestorePlan.getClassSchema());
            collectFromRestorePlan(plan, instant, timelineType, needle, out);
          } catch (Exception ignored) {
            // best-effort
          }
        }
        break;

      default:
        // savepoint, compaction (the .compaction.* files are themselves write stats once
        // they complete as a regular commit), indexing, etc. -- not part of the file-id
        // dragnet for now.
        break;
    }
  }

  private void collectFromCommitMetadata(HoodieCommitMetadata cm, HoodieInstant instant,
                                         String timelineType, String needle, List<Event> out) {
    if (cm == null || cm.getPartitionToWriteStats() == null) {
      return;
    }
    for (Map.Entry<String, List<HoodieWriteStat>> e : cm.getPartitionToWriteStats().entrySet()) {
      for (HoodieWriteStat ws : e.getValue()) {
        if (matches(ws.getFileId(), needle) || matches(ws.getPath(), needle)) {
          String detail = "path=" + ws.getPath()
              + " numWrites=" + ws.getNumWrites()
              + " numUpdates=" + ws.getNumUpdateWrites()
              + " numDeletes=" + ws.getNumDeletes()
              + " prevCommit=" + ws.getPrevCommit();
          out.add(new Event(instant, timelineType, "writeStat", e.getKey(), detail));
        }
      }
    }
  }

  private void collectFromClusteringPlan(HoodieClusteringPlan plan, HoodieInstant instant,
                                         String timelineType, String needle, List<Event> out) {
    if (plan == null || plan.getInputGroups() == null) {
      return;
    }
    plan.getInputGroups().forEach(group -> group.getSlices().forEach(slice -> {
      String dataFile = slice.getDataFilePath();
      if (matches(dataFile, needle) || matches(slice.getFileId(), needle)) {
        out.add(new Event(instant, timelineType, "clusteringInputSlice",
            slice.getPartitionPath(),
            "dataFile=" + dataFile + " fileId=" + slice.getFileId()));
      }
    }));
  }

  private void collectFromCleanMetadata(HoodieCleanMetadata cm, HoodieInstant instant,
                                        String timelineType, String needle, List<Event> out) {
    if (cm == null || cm.getPartitionMetadata() == null) {
      return;
    }
    for (Map.Entry<String, HoodieCleanPartitionMetadata> e : cm.getPartitionMetadata().entrySet()) {
      HoodieCleanPartitionMetadata pm = e.getValue();
      collectFileMatches(instant, timelineType, "cleanSuccessDelete", e.getKey(),
          pm.getSuccessDeleteFiles(), needle, out);
      collectFileMatches(instant, timelineType, "cleanFailedDelete", e.getKey(),
          pm.getFailedDeleteFiles(), needle, out);
      collectFileMatches(instant, timelineType, "cleanDeletePattern", e.getKey(),
          pm.getDeletePathPatterns(), needle, out);
    }
  }

  private void collectFromRollbackMetadata(HoodieRollbackMetadata rm, HoodieInstant instant,
                                           String timelineType, String needle, List<Event> out) {
    if (rm == null) {
      return;
    }
    // commitsRollback (legacy field; populated on most 0.14.x rollbacks but not all).
    if (rm.getCommitsRollback() != null) {
      for (String rolledBack : rm.getCommitsRollback()) {
        if (matches(rolledBack, needle)) {
          out.add(new Event(instant, timelineType, "rollbackOfCommit", "<any>",
              "rolledBackCommit=" + rolledBack));
        }
      }
    }
    // instantsRollback (newer field; carries both commit time and action). The schema
    // explicitly notes this "overlaps with commitsRollback" -- some rollbacks fill only
    // this one. Check it independently so we don't miss matches.
    if (rm.getInstantsRollback() != null) {
      for (HoodieInstantInfo info : rm.getInstantsRollback()) {
        if (info != null && matches(info.getCommitTime(), needle)) {
          out.add(new Event(instant, timelineType, "rollbackOfCommit", "<any>",
              "rolledBackCommit=" + info.getCommitTime() + " action=" + info.getAction()));
        }
      }
    }
    if (rm.getPartitionMetadata() != null) {
      for (Map.Entry<String, HoodieRollbackPartitionMetadata> e : rm.getPartitionMetadata().entrySet()) {
        HoodieRollbackPartitionMetadata pm = e.getValue();
        collectFileMatches(instant, timelineType, "rollbackSuccessDelete", e.getKey(),
            pm.getSuccessDeleteFiles(), needle, out);
        collectFileMatches(instant, timelineType, "rollbackFailedDelete", e.getKey(),
            pm.getFailedDeleteFiles(), needle, out);
      }
    }
  }

  private void collectFromRestoreMetadata(HoodieRestoreMetadata rm, HoodieInstant instant,
                                          String timelineType, String needle, List<Event> out) {
    if (rm == null) {
      return;
    }
    if (rm.getInstantsToRollback() != null) {
      for (String rolledBack : rm.getInstantsToRollback()) {
        if (matches(rolledBack, needle)) {
          out.add(new Event(instant, timelineType, "restoreOfCommit", "<any>",
              "rolledBackCommit=" + rolledBack));
        }
      }
    }
    // hoodieRestoreMetadata = map of partition -> list of nested HoodieRollbackMetadata.
    if (rm.getHoodieRestoreMetadata() != null) {
      for (Map.Entry<String, List<HoodieRollbackMetadata>> e : rm.getHoodieRestoreMetadata().entrySet()) {
        for (HoodieRollbackMetadata nested : e.getValue()) {
          collectFromRollbackMetadata(nested, instant, timelineType, needle, out);
        }
      }
    }
  }

  private void collectFromRestorePlan(HoodieRestorePlan plan, HoodieInstant instant,
                                      String timelineType, String needle, List<Event> out) {
    if (plan == null) {
      return;
    }
    if (plan.getInstantsToRollback() != null) {
      for (HoodieInstantInfo info : plan.getInstantsToRollback()) {
        if (info != null && matches(info.getCommitTime(), needle)) {
          out.add(new Event(instant, timelineType, "restorePlanTargetCommit", "<any>",
              "rolledBackCommit=" + info.getCommitTime() + " action=" + info.getAction()));
        }
      }
    }
    if (matches(plan.getSavepointToRestoreTimestamp(), needle)) {
      out.add(new Event(instant, timelineType, "restorePlanSavepoint", "<any>",
          "savepointToRestore=" + plan.getSavepointToRestoreTimestamp()));
    }
  }

  private void collectFromRollbackPlan(HoodieRollbackPlan plan, HoodieInstant instant,
                                       String timelineType, String needle, List<Event> out) {
    if (plan == null) {
      return;
    }
    if (plan.getInstantToRollback() != null
        && matches(plan.getInstantToRollback().getCommitTime(), needle)) {
      out.add(new Event(instant, timelineType, "rollbackPlanTargetCommit", "<any>",
          "rolledBackCommit=" + plan.getInstantToRollback().getCommitTime()));
    }
    if (plan.getRollbackRequests() != null) {
      for (HoodieRollbackRequest req : plan.getRollbackRequests()) {
        collectFileMatches(instant, timelineType, "rollbackPlanFile", req.getPartitionPath(),
            req.getFilesToBeDeleted(), needle, out);
        // Also include log-block scopes if present
        if (req.getLogBlocksToBeDeleted() != null) {
          for (String logFile : req.getLogBlocksToBeDeleted().keySet()) {
            if (matches(logFile, needle)) {
              out.add(new Event(instant, timelineType, "rollbackPlanLogBlock",
                  req.getPartitionPath(), "logFile=" + logFile));
            }
          }
        }
      }
    }
  }

  private void collectFileMatches(HoodieInstant instant, String timelineType, String matchType,
                                  String partition, Collection<String> files,
                                  String needle, List<Event> out) {
    if (files == null) {
      return;
    }
    for (String f : files) {
      if (matches(f, needle)) {
        out.add(new Event(instant, timelineType, matchType, partition, f));
      }
    }
  }

  private static boolean matches(String haystack, String needle) {
    return haystack != null && !haystack.isEmpty() && haystack.contains(needle);
  }

  // ---- output ---------------------------------------------------------------

  private void printEventTable(List<Event> events) {
    if (events.isEmpty()) {
      System.out.println("(no events)");
      return;
    }
    String[] headers = {"instant", "timeline", "action", "state", "match_type", "partition", "detail"};
    List<String[]> rows = new ArrayList<>(events.size());
    for (Event e : events) {
      rows.add(new String[]{
          e.instantTime, e.timelineType, e.action, e.state, e.matchType,
          e.partition == null ? "" : e.partition,
          e.detail == null ? "" : e.detail
      });
    }
    printRowsWithHeaders(headers, rows);
  }

  private void printRowsWithHeaders(String[] headers, List<String[]> rows) {
    int[] widths = new int[headers.length];
    for (int i = 0; i < headers.length; i++) {
      widths[i] = headers[i].length();
    }
    for (String[] r : rows) {
      for (int i = 0; i < headers.length; i++) {
        if (r[i] != null && r[i].length() > widths[i]) {
          widths[i] = Math.min(r[i].length(), 120);
        }
      }
    }
    printRow(headers, widths);
    StringBuilder rule = new StringBuilder();
    for (int w : widths) {
      for (int i = 0; i < w; i++) {
        rule.append('-');
      }
      rule.append("  ");
    }
    System.out.println(rule);
    for (String[] r : rows) {
      printRow(r, widths);
    }
  }

  private void printRow(String[] cols, int[] widths) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < cols.length; i++) {
      String c = cols[i] == null ? "" : cols[i];
      if (c.length() > widths[i]) {
        c = c.substring(0, widths[i] - 1) + "…";
      }
      sb.append(c);
      for (int p = c.length(); p < widths[i]; p++) {
        sb.append(' ');
      }
      sb.append("  ");
    }
    System.out.println(sb);
  }

  // ---- locator + readers ----------------------------------------------------

  /**
   * Finds every state of the given instant time across both timelines (subject to optional
   * action + state filters), in REQUESTED → INFLIGHT → COMPLETED order. With no filters,
   * an instant that completed normally will return all three rows.
   */
  private List<InstantLocator> findInstantStates(HoodieTableMetaClient metaClient,
                                                 String instantTime,
                                                 Set<String> actionFilter,
                                                 HoodieInstant.State stateFilter,
                                                 boolean includeArchived) {
    List<InstantLocator> hits = new ArrayList<>();
    HoodieActiveTimeline active = metaClient.reloadActiveTimeline();
    active.getInstantsAsStream()
        .filter(i -> i.requestedTime().equals(instantTime))
        .filter(i -> actionFilter.isEmpty() || actionFilter.contains(i.getAction()))
        .filter(i -> stateFilter == null || i.getState() == stateFilter)
        .forEach(i -> hits.add(new InstantLocator(i, active, "ACTIVE")));
    if (hits.isEmpty() && includeArchived) {
      HoodieArchivedTimeline archived = metaClient.getArchivedTimeline();
      archived.loadInstantDetailsInMemory(instantTime, instantTime);
      archived.getInstantsAsStream()
          .filter(i -> i.requestedTime().equals(instantTime))
          .filter(i -> actionFilter.isEmpty() || actionFilter.contains(i.getAction()))
          .filter(i -> stateFilter == null || i.getState() == stateFilter)
          .forEach(i -> hits.add(new InstantLocator(i, archived, "ARCHIVED")));
    }
    hits.sort(Comparator.comparingInt(loc -> statePriority(loc.instant.getState())));
    return hits;
  }

  /** REQUESTED → INFLIGHT → COMPLETED, so output reads in lifecycle order. */
  private static int statePriority(HoodieInstant.State state) {
    switch (state) {
      case REQUESTED: return 0;
      case INFLIGHT:  return 1;
      case COMPLETED: return 2;
      default:        return 99;
    }
  }

  private Object readContent(HoodieTableMetaClient metaClient, InstantLocator loc) throws IOException {
    HoodieInstant instant = loc.instant;
    String action = instant.getAction();

    switch (action) {
      case HoodieTimeline.COMMIT_ACTION:
      case HoodieTimeline.DELTA_COMMIT_ACTION:
        return readCommitMetadataTolerant(loc, instant, HoodieCommitMetadata.class);
      case HoodieTimeline.REPLACE_COMMIT_ACTION:
        if (instant.isCompleted()) {
          return readCommitMetadataTolerant(loc, instant, HoodieReplaceCommitMetadata.class);
        } else if (instant.isRequested()) {
          try {
            return readSpecificRecord(loc.timeline, instant,
                HoodieRequestedReplaceMetadata.class, HoodieRequestedReplaceMetadata.getClassSchema());
          } catch (Exception e) {
            return Collections.singletonMap("note", "no body or schema mismatch");
          }
        }
        return null;
      case HoodieTimeline.CLEAN_ACTION:
        if (instant.isCompleted()) {
          return readSpecificRecord(loc.timeline, instant,
              HoodieCleanMetadata.class, HoodieCleanMetadata.getClassSchema());
        }
        return null;
      case HoodieTimeline.ROLLBACK_ACTION:
        if (instant.isCompleted()) {
          return readSpecificRecord(loc.timeline, instant,
              HoodieRollbackMetadata.class, HoodieRollbackMetadata.getClassSchema());
        } else if (instant.isRequested()) {
          return readSpecificRecord(loc.timeline, instant,
              HoodieRollbackPlan.class, HoodieRollbackPlan.getClassSchema());
        }
        return null;
      case HoodieTimeline.RESTORE_ACTION:
        if (instant.isCompleted()) {
          return readSpecificRecord(loc.timeline, instant,
              HoodieRestoreMetadata.class, HoodieRestoreMetadata.getClassSchema());
        } else if (instant.isRequested()) {
          return readSpecificRecord(loc.timeline, instant,
              HoodieRestorePlan.class, HoodieRestorePlan.getClassSchema());
        }
        return null;
      default:
        // Generic best-effort: print raw bytes length and a hex prefix.
        Option<byte[]> bytes = loc.timeline.getInstantDetails(instant);
        if (!bytes.isPresent() || bytes.get().length == 0) {
          return null;
        }
        Map<String, Object> generic = new LinkedHashMap<>();
        generic.put("byteLength", bytes.get().length);
        generic.put("hexPrefix", toHex(bytes.get(), Math.min(64, bytes.get().length)));
        return generic;
    }
  }

  /**
   * Reads a non-Avro-SpecificRecord commit metadata (HoodieCommitMetadata / its replace subclass)
   * via Hudi's typed-content reader, which handles the JSON path that 0.14.1 uses on disk for
   * both active and archived timelines.
   */
  private <T> T readCommitMetadata(HoodieTimeline timeline, HoodieInstant instant, Class<T> clazz)
      throws IOException {
    return timeline.readInstantContent(instant, clazz);
  }

  /**
   * Reads commit/replace metadata for show-instant. The archived timeline stores these via
   * {@code GenericRecord.toString()} -- an Avro-flavored JSON -- which Jackson cannot
   * round-trip into the POJO {@code HoodieCommitMetadata}: it returns an empty shell
   * (empty maps, {@code UNKNOWN} operationType). To avoid that data loss, if the bytes
   * start with '{' we return the parsed JSON tree directly instead of the POJO.
   */
  private Object readCommitMetadataTolerant(InstantLocator loc, HoodieInstant instant, Class<?> clazz)
      throws IOException {
    Option<byte[]> rawOpt = loc.timeline.getInstantDetails(instant);
    if (!rawOpt.isPresent() || rawOpt.get().length == 0) {
      return null;
    }
    byte[] raw = rawOpt.get();
    int firstNonWs = 0;
    while (firstNonWs < raw.length && Character.isWhitespace((char) raw[firstNonWs])) {
      firstNonWs++;
    }
    if (firstNonWs < raw.length && raw[firstNonWs] == '{') {
      // Parse the JSON tree and return it as-is. Faithful to whatever the archive stored
      // (which is Avro's toString() of the GenericRecord, lossless modulo whitespace).
      try {
        return JSON_MAPPER.readTree(raw);
      } catch (Exception parseFail) {
        // Fall through to the POJO path; better than nothing.
      }
    }
    return loc.timeline.readInstantContent(instant, clazz);
  }

  /**
   * Reads an Avro SpecificRecord (clean / rollback / requested-replace / rollback-plan) tolerating
   * both Avro-container bytes (active timeline writes them as .avro) and JSON-string bytes (the
   * HoodieArchivedTimeline in 0.14.1 stores non-compaction archived actions as JSON strings in
   * its in-memory readCommits cache).
   */
  private <T extends SpecificRecordBase> T readSpecificRecord(HoodieTimeline timeline,
                                                              HoodieInstant instant,
                                                              Class<T> clazz,
                                                              Schema schema) throws IOException {
    byte[] bytes = timeline.getInstantDetails(instant).orElseGet(() -> new byte[0]);
    if (bytes.length >= 4 && bytes[0] == 'O' && bytes[1] == 'b' && bytes[2] == 'j' && bytes[3] == 1) {
      try (DataFileStream<T> reader =
               new DataFileStream<>(new ByteArrayInputStream(bytes), new SpecificDatumReader<>(clazz))) {
        if (!reader.hasNext()) {
          throw new IOException("empty Avro container for instant " + instant);
        }
        return reader.next();
      }
    }
    if (bytes.length == 0) {
      throw new IOException("no body for instant " + instant);
    }
    // First-try: pristine Avro JSON encoding (tagged unions).
    try {
      return new SpecificDatumReader<T>(schema).read(null,
          DecoderFactory.get().jsonDecoder(schema, new ByteArrayInputStream(bytes)));
    } catch (AvroTypeException firstAttempt) {
      // HoodieArchivedTimeline serializes non-compaction archived metadata via Avro's
      // GenericData.toString() which emits union values as the raw branch value (e.g.
      // "isPartitionDeleted": false) instead of Avro's spec-compliant tagged form
      // ({"boolean": false}). Walk the JSON tree against the schema, tag-wrap any
      // raw-branch union values, and retry. Falls back to throwing the original error
      // if fixup itself fails.
      JsonNode root;
      try {
        root = JSON_MAPPER.readTree(bytes);
      } catch (Exception parseFail) {
        throw firstAttempt;
      }
      JsonNode fixed = fixupUnionTags(root, schema);
      byte[] rewritten = JSON_MAPPER.writeValueAsBytes(fixed);
      return new SpecificDatumReader<T>(schema).read(null,
          DecoderFactory.get().jsonDecoder(schema, new ByteArrayInputStream(rewritten)));
    }
  }

  /**
   * Walks a Jackson tree alongside the given Avro schema and rewrites any value that lands
   * on a UNION whose declared form is {@code ["null", X]} but whose JSON form is the raw X
   * branch value. Avro JsonDecoder needs the tagged form {@code {"X": value}} except for
   * the null branch (which stays bare {@code null}). Records and arrays are recursed into.
   */
  private static JsonNode fixupUnionTags(JsonNode node, Schema schema) {
    if (node == null || node.isMissingNode()) {
      return node;
    }
    switch (schema.getType()) {
      case UNION:
        return fixupUnion(node, schema);
      case RECORD: {
        if (!node.isObject()) {
          return node;
        }
        ObjectNode obj = (ObjectNode) node;
        ObjectNode out = JsonNodeFactory.instance.objectNode();
        for (Schema.Field field : schema.getFields()) {
          JsonNode child = obj.get(field.name());
          if (child != null) {
            out.set(field.name(), fixupUnionTags(child, field.schema()));
          }
        }
        return out;
      }
      case ARRAY: {
        if (!node.isArray()) {
          return node;
        }
        ArrayNode arr = (ArrayNode) node;
        ArrayNode out = JsonNodeFactory.instance.arrayNode();
        for (JsonNode el : arr) {
          out.add(fixupUnionTags(el, schema.getElementType()));
        }
        return out;
      }
      case MAP: {
        if (!node.isObject()) {
          return node;
        }
        ObjectNode mapNode = (ObjectNode) node;
        ObjectNode out = JsonNodeFactory.instance.objectNode();
        java.util.Iterator<String> it = mapNode.fieldNames();
        while (it.hasNext()) {
          String k = it.next();
          out.set(k, fixupUnionTags(mapNode.get(k), schema.getValueType()));
        }
        return out;
      }
      default:
        // Primitive types pass through unchanged.
        return node;
    }
  }

  private static JsonNode fixupUnion(JsonNode node, Schema unionSchema) {
    List<Schema> branches = unionSchema.getTypes();
    boolean hasNull = false;
    Schema nonNull = null;
    for (Schema b : branches) {
      if (b.getType() == Schema.Type.NULL) {
        hasNull = true;
      } else if (nonNull == null) {
        nonNull = b;
      } else {
        // Multi-non-null unions are uncommon in Hudi metadata; bail and trust the input.
        return node;
      }
    }
    if (node.isNull()) {
      return node;
    }
    if (node.isObject() && node.size() == 1) {
      // Already tagged; recurse into the branch value to keep nested unions consistent.
      String key = node.fieldNames().next();
      Schema branch = null;
      for (Schema b : branches) {
        if (avroJsonBranchName(b).equals(key)) {
          branch = b;
          break;
        }
      }
      if (branch != null) {
        ObjectNode out = JsonNodeFactory.instance.objectNode();
        out.set(key, fixupUnionTags(node.get(key), branch));
        return out;
      }
      // Unrecognized tag — leave as-is.
      return node;
    }
    if (hasNull && nonNull != null) {
      // Raw branch value (the malformed-archive case). Recurse first so nested records
      // also get their unions fixed.
      JsonNode recursed = fixupUnionTags(node, nonNull);
      ObjectNode wrapper = JsonNodeFactory.instance.objectNode();
      wrapper.set(avroJsonBranchName(nonNull), recursed);
      return wrapper;
    }
    return node;
  }

  /** Avro's JSON branch name for a non-null schema (matches DataFileWriter conventions). */
  private static String avroJsonBranchName(Schema schema) {
    switch (schema.getType()) {
      case RECORD: case ENUM: case FIXED:
        return schema.getFullName();
      default:
        return schema.getType().getName();
    }
  }

  private static String toHex(byte[] bytes, int limit) {
    StringBuilder sb = new StringBuilder(limit * 3);
    for (int i = 0; i < limit; i++) {
      sb.append(String.format("%02x ", bytes[i]));
    }
    return sb.toString().trim();
  }

  // ---- args + types ---------------------------------------------------------

  enum Mode { SHOW_INSTANT, FIND_FILE_ID, PARSE_FILENAME, RAW_ARCHIVE, COMMIT_STATS, PHASE_TIMINGS }

  enum OutputFormat { TABLE, JSON }

  static class InstantLocator {
    final HoodieInstant instant;
    final HoodieTimeline timeline;
    final String timelineType;

    InstantLocator(HoodieInstant instant, HoodieTimeline timeline, String timelineType) {
      this.instant = instant;
      this.timeline = timeline;
      this.timelineType = timelineType;
    }
  }

  static class CommitStatsRow {
    final String instant;
    final String timelineType;
    final String action;
    final String operation;
    final long numInserts;
    final long numUpdates;
    final long numDeletes;
    final long filesInserted;
    final long filesUpdated;
    final long filesDeleted;
    final long bytesWritten;
    final long partitions;

    CommitStatsRow(String instant, String timelineType, String action, String operation,
                   long numInserts, long numUpdates, long numDeletes,
                   long filesInserted, long filesUpdated, long filesDeleted,
                   long bytesWritten, long partitions) {
      this.instant = instant;
      this.timelineType = timelineType;
      this.action = action;
      this.operation = operation;
      this.numInserts = numInserts;
      this.numUpdates = numUpdates;
      this.numDeletes = numDeletes;
      this.filesInserted = filesInserted;
      this.filesUpdated = filesUpdated;
      this.filesDeleted = filesDeleted;
      this.bytesWritten = bytesWritten;
      this.partitions = partitions;
    }

    static CommitStatsRow empty(HoodieInstant instant, String timelineType) {
      return new CommitStatsRow(instant.requestedTime(), timelineType, instant.getAction(),
          "UNKNOWN", 0, 0, 0, 0, 0, 0, 0, 0);
    }

    static CommitStatsRow fromPojo(HoodieInstant instant, String timelineType,
                                   HoodieCommitMetadata cm, boolean isReplace) {
      long filesDeleted = 0;
      if (isReplace && cm instanceof HoodieReplaceCommitMetadata) {
        Map<String, List<String>> replaced =
            ((HoodieReplaceCommitMetadata) cm).getPartitionToReplaceFileIds();
        if (replaced != null) {
          for (List<String> ids : replaced.values()) {
            filesDeleted += ids.size();
          }
        }
      }
      WriteOperationType op = cm.getOperationType();
      return new CommitStatsRow(
          instant.requestedTime(), timelineType, instant.getAction(),
          op == null ? "UNKNOWN" : op.toString(),
          cm.fetchTotalInsertRecordsWritten(),
          cm.fetchTotalUpdateRecordsWritten(),
          cm.getTotalRecordsDeleted(),
          cm.fetchTotalFilesInsert(),
          cm.fetchTotalFilesUpdated(),
          filesDeleted,
          cm.fetchTotalBytesWritten(),
          cm.fetchTotalPartitionsWritten());
    }

    /**
     * Aggregate stats from the archived-shell JSON. Walks {@code partitionToWriteStats} and
     * sums per-stat fields directly, replicating the {@code fetchTotal*} helpers from
     * {@link HoodieCommitMetadata}.
     */
    static CommitStatsRow fromJson(HoodieInstant instant, String timelineType,
                                   JsonNode root, boolean isReplace) {
      long numInserts = 0;
      long numUpdates = 0;
      long numDeletes = 0;
      long filesInserted = 0;
      long filesUpdated = 0;
      long bytesWritten = 0;
      long partitions = 0;
      String operation = "UNKNOWN";
      if (root.hasNonNull("operationType")) {
        operation = root.get("operationType").asText();
      }

      JsonNode p2ws = root.get("partitionToWriteStats");
      if (p2ws != null && p2ws.isObject()) {
        partitions = p2ws.size();
        java.util.Iterator<String> partIt = p2ws.fieldNames();
        while (partIt.hasNext()) {
          String partition = partIt.next();
          JsonNode statsArr = p2ws.get(partition);
          if (statsArr == null || !statsArr.isArray()) {
            continue;
          }
          for (JsonNode ws : statsArr) {
            long ins = ws.path("numInserts").asLong(0);
            long upd = ws.path("numUpdateWrites").asLong(0);
            long del = ws.path("numDeletes").asLong(0);
            long bytes = ws.path("totalWriteBytes").asLong(0);
            // prevCommit semantics (mirror HoodieCommitMetadata):
            //  - missing/JSON-null  → field unset, file is neither inserted nor updated here
            //  - literal string "null" → new file (file insert)
            //  - any other string   → file update (writes against an existing file)
            JsonNode prevCommitNode = ws.get("prevCommit");
            boolean prevCommitFieldSet = prevCommitNode != null && !prevCommitNode.isNull();
            String prevCommit = prevCommitFieldSet ? prevCommitNode.asText() : null;
            numDeletes += del;
            bytesWritten += bytes;
            numUpdates += upd;
            if (prevCommitFieldSet) {
              numInserts += ins;
              if (prevCommit.equalsIgnoreCase("null")) {
                filesInserted++;
              } else {
                filesUpdated++;
              }
            }
          }
        }
      }

      long filesDeleted = 0;
      if (isReplace) {
        JsonNode replaced = root.get("partitionToReplaceFileIds");
        if (replaced != null && replaced.isObject()) {
          java.util.Iterator<String> it = replaced.fieldNames();
          while (it.hasNext()) {
            JsonNode ids = replaced.get(it.next());
            if (ids != null && ids.isArray()) {
              filesDeleted += ids.size();
            }
          }
        }
      }

      return new CommitStatsRow(instant.requestedTime(), timelineType, instant.getAction(),
          operation, numInserts, numUpdates, numDeletes,
          filesInserted, filesUpdated, filesDeleted, bytesWritten, partitions);
    }
  }

  /**
   * One phase-timing row. Holds the six raw timestamps (epoch ms; nullable for MDT phases
   * when the table has no MDT or this instant predates MDT enablement) and exposes the
   * derived per-phase durations as nullable longs.
   */
  static class PhaseTimingRow {
    final String instant;
    final String action;
    final long t0Requested;
    final long t1Inflight;
    final Long t2MdtRequested;
    final Long t3MdtInflight;
    final Long t4MdtCompleted;
    final long t5Completed;

    PhaseTimingRow(String instant, String action,
                   long t0Requested, long t1Inflight,
                   Long t2MdtRequested, Long t3MdtInflight, Long t4MdtCompleted,
                   long t5Completed) {
      this.instant = instant;
      this.action = action;
      this.t0Requested = t0Requested;
      this.t1Inflight = t1Inflight;
      this.t2MdtRequested = t2MdtRequested;
      this.t3MdtInflight = t3MdtInflight;
      this.t4MdtCompleted = t4MdtCompleted;
      this.t5Completed = t5Completed;
    }

    boolean hasMdt() {
      return t2MdtRequested != null && t3MdtInflight != null && t4MdtCompleted != null;
    }

    Long sourceReadIndexSfhMs() {
      return t1Inflight - t0Requested;
    }

    Long dataWriteMs() {
      // T2-T1 if MDT present; otherwise T5-T1 (data write extends until ingest completed).
      return hasMdt() ? (t2MdtRequested - t1Inflight) : (t5Completed - t1Inflight);
    }

    Long mdtPrepMs() {
      return hasMdt() ? (t3MdtInflight - t2MdtRequested) : null;
    }

    Long mdtWriteMs() {
      return hasMdt() ? (t4MdtCompleted - t3MdtInflight) : null;
    }

    Long mdtTailMs() {
      return hasMdt() ? (t5Completed - t4MdtCompleted) : null;
    }

    Long totalMs() {
      return t5Completed - t0Requested;
    }
  }

  /**
   * Per-action aggregator for phase timings. Accumulates each phase's samples into a
   * resizable list and exposes mean / percentile / max on demand. {@code hasMdt()}
   * reflects whether *any* sample in this action's bucket had MDT phases populated —
   * mixed populations are tolerated (early rows without MDT, later rows with).
   */
  static class PhaseTimingAggregate {
    enum Phase { SOURCE_READ, DATA_WRITE, MDT_PREP, MDT_WRITE, MDT_TAIL, TOTAL }

    final String action;
    private final Map<Phase, List<Long>> samples = new LinkedHashMap<>();
    private int rowCount = 0;
    private boolean anyMdt = false;

    PhaseTimingAggregate(String action) {
      this.action = action;
      for (Phase p : Phase.values()) {
        samples.put(p, new ArrayList<>());
      }
    }

    void add(PhaseTimingRow r) {
      rowCount++;
      samples.get(Phase.SOURCE_READ).add(r.sourceReadIndexSfhMs());
      samples.get(Phase.DATA_WRITE).add(r.dataWriteMs());
      samples.get(Phase.TOTAL).add(r.totalMs());
      if (r.hasMdt()) {
        anyMdt = true;
        samples.get(Phase.MDT_PREP).add(r.mdtPrepMs());
        samples.get(Phase.MDT_WRITE).add(r.mdtWriteMs());
        samples.get(Phase.MDT_TAIL).add(r.mdtTailMs());
      }
    }

    int count() {
      return rowCount;
    }

    boolean hasMdt() {
      return anyMdt;
    }

    double mean(Phase phase) {
      List<Long> vs = samples.get(phase);
      if (vs == null || vs.isEmpty()) {
        return 0.0;
      }
      long sum = 0;
      for (Long v : vs) {
        sum += v;
      }
      return (double) sum / vs.size();
    }

    long max(Phase phase) {
      List<Long> vs = samples.get(phase);
      if (vs == null || vs.isEmpty()) {
        return 0;
      }
      long m = Long.MIN_VALUE;
      for (Long v : vs) {
        if (v > m) {
          m = v;
        }
      }
      return m;
    }

    /**
     * Nearest-rank percentile (no interpolation). For n=1 returns the single value at
     * any pct; for n=10 and pct=95 returns the 10th-ranked (max) value. Good enough for
     * the small-N forensic use case this tool targets.
     */
    long percentile(Phase phase, int pct) {
      List<Long> vs = samples.get(phase);
      if (vs == null || vs.isEmpty()) {
        return 0;
      }
      List<Long> sorted = new ArrayList<>(vs);
      Collections.sort(sorted);
      int rank = (int) Math.ceil(pct / 100.0 * sorted.size());
      if (rank < 1) {
        rank = 1;
      }
      if (rank > sorted.size()) {
        rank = sorted.size();
      }
      return sorted.get(rank - 1);
    }
  }

  static class CommitStatsTotals {
    final long totalInserts;
    final long totalUpdates;
    final long totalDeletes;
    final int commitCount;

    CommitStatsTotals(long ti, long tu, long td, int n) {
      this.totalInserts = ti;
      this.totalUpdates = tu;
      this.totalDeletes = td;
      this.commitCount = n;
    }

    double avgInserts() {
      return commitCount == 0 ? 0.0 : (double) totalInserts / commitCount;
    }

    double avgUpdates() {
      return commitCount == 0 ? 0.0 : (double) totalUpdates / commitCount;
    }

    double avgDeletes() {
      return commitCount == 0 ? 0.0 : (double) totalDeletes / commitCount;
    }

    static CommitStatsTotals from(List<CommitStatsRow> rows) {
      long ti = 0;
      long tu = 0;
      long td = 0;
      for (CommitStatsRow r : rows) {
        ti += r.numInserts;
        tu += r.numUpdates;
        td += r.numDeletes;
      }
      return new CommitStatsTotals(ti, tu, td, rows.size());
    }
  }

  static class Event {
    final String instantTime;
    final String action;
    final String state;
    final String timelineType;
    final String matchType;
    final String partition;
    final String detail;

    Event(HoodieInstant instant, String timelineType, String matchType,
          String partition, String detail) {
      this.instantTime = instant.requestedTime();
      this.action = instant.getAction();
      this.state = instant.getState().toString();
      this.timelineType = timelineType;
      this.matchType = matchType;
      this.partition = partition;
      this.detail = detail;
    }
  }

  static class Args {
    Mode mode;
    String basePath;
    String showInstantTime;
    String fileIdNeedle;
    String filenameToParse;
    String rawArchiveInstant;
    boolean lifecycle = false;
    OutputFormat output = OutputFormat.TABLE;
    boolean includeArchived = true;
    String startInstant;
    String endInstant;
    Set<String> actionFilter = new HashSet<>();
    HoodieInstant.State stateFilter;
    int limit = DEFAULT_LIMIT;
    boolean quiet = false;
    // commit-stats / phase-timings default: newest first, so --limit N returns the latest N.
    boolean sortDescending = true;
    // phase-timings: opt-in inclusion of replacecommit instants (default off).
    boolean includeReplacecommit = false;
    boolean helpRequested = false;

    static Args parse(String[] argv) {
      Args a = new Args();
      for (int i = 0; i < argv.length; i++) {
        String k = argv[i];
        switch (k) {
          case "--base-path":
            a.basePath = require(argv, ++i, k);
            break;
          case "--show-instant":
            a.mode = Mode.SHOW_INSTANT;
            a.showInstantTime = require(argv, ++i, k);
            break;
          case "--find-file-id":
            a.mode = Mode.FIND_FILE_ID;
            a.fileIdNeedle = require(argv, ++i, k);
            break;
          case "--parse-filename":
            a.mode = Mode.PARSE_FILENAME;
            a.filenameToParse = require(argv, ++i, k);
            break;
          case "--raw-archive":
            a.mode = Mode.RAW_ARCHIVE;
            a.rawArchiveInstant = require(argv, ++i, k);
            break;
          case "--commit-stats":
            a.mode = Mode.COMMIT_STATS;
            break;
          case "--phase-timings":
            a.mode = Mode.PHASE_TIMINGS;
            break;
          case "--include-replacecommit":
            a.includeReplacecommit = true;
            break;
          case "--lifecycle":
            a.lifecycle = true;
            break;
          case "--output": {
            String raw = require(argv, ++i, k).toUpperCase(Locale.ROOT);
            try {
              a.output = OutputFormat.valueOf(raw);
            } catch (IllegalArgumentException ex) {
              throw new IllegalArgumentException("--output must be one of TABLE|JSON (got " + raw + ")");
            }
            break;
          }
          case "--include-archived":
            a.includeArchived = Boolean.parseBoolean(require(argv, ++i, k));
            break;
          case "--no-archived":
            a.includeArchived = false;
            break;
          case "--start-instant":
            a.startInstant = require(argv, ++i, k);
            break;
          case "--end-instant":
            a.endInstant = require(argv, ++i, k);
            break;
          case "--actions":
            a.actionFilter = new HashSet<>(Arrays.asList(require(argv, ++i, k).split(",")));
            break;
          case "--state": {
            String raw = require(argv, ++i, k).toUpperCase(Locale.ROOT);
            try {
              a.stateFilter = HoodieInstant.State.valueOf(raw);
            } catch (IllegalArgumentException ex) {
              throw new IllegalArgumentException("--state must be one of REQUESTED|INFLIGHT|COMPLETED (got "
                  + raw + ")");
            }
            break;
          }
          case "--limit":
            a.limit = Integer.parseInt(require(argv, ++i, k));
            if (a.limit <= 0) {
              throw new IllegalArgumentException("--limit must be positive (got " + a.limit + ")");
            }
            break;
          case "--quiet":
          case "-q":
            a.quiet = true;
            break;
          case "--sort": {
            String raw = require(argv, ++i, k).toLowerCase(Locale.ROOT);
            if (raw.equals("asc")) {
              a.sortDescending = false;
            } else if (raw.equals("desc")) {
              a.sortDescending = true;
            } else {
              throw new IllegalArgumentException("--sort must be asc or desc (got " + raw + ")");
            }
            break;
          }
          case "--help":
          case "-h":
            printUsage();
            a.helpRequested = true;
            return a;
          default:
            throw new IllegalArgumentException("unknown argument: " + k);
        }
      }
      if (a.mode == null) {
        throw new IllegalArgumentException(
            "specify --show-instant <ts>, --find-file-id <id>, --commit-stats, "
                + "--parse-filename <name>, or --raw-archive <ts>");
      }
      if (a.mode != Mode.PARSE_FILENAME && a.basePath == null) {
        throw new IllegalArgumentException("--base-path is required");
      }
      if (a.lifecycle && a.mode != Mode.FIND_FILE_ID) {
        throw new IllegalArgumentException("--lifecycle is only valid with --find-file-id");
      }
      if (a.includeReplacecommit && a.mode != Mode.PHASE_TIMINGS) {
        throw new IllegalArgumentException(
            "--include-replacecommit is only valid with --phase-timings");
      }
      // Restrict action filter to a known set; reject typos early.
      Set<String> validActions = new HashSet<>(Arrays.asList(
          HoodieTimeline.COMMIT_ACTION, HoodieTimeline.DELTA_COMMIT_ACTION,
          HoodieTimeline.REPLACE_COMMIT_ACTION, HoodieTimeline.CLEAN_ACTION,
          HoodieTimeline.ROLLBACK_ACTION, HoodieTimeline.RESTORE_ACTION,
          HoodieTimeline.SAVEPOINT_ACTION,
          HoodieTimeline.COMPACTION_ACTION, HoodieTimeline.LOG_COMPACTION_ACTION));
      List<String> unknown = a.actionFilter.stream()
          .filter(act -> !validActions.contains(act)).collect(Collectors.toList());
      if (!unknown.isEmpty()) {
        throw new IllegalArgumentException("unknown actions in --actions: " + unknown
            + " (valid: " + validActions + ")");
      }
      return a;
    }

    private static String require(String[] argv, int i, String flag) {
      if (i >= argv.length) {
        throw new IllegalArgumentException("missing value for " + flag);
      }
      return argv[i];
    }

    static void printUsage() {
      System.err.println("Usage: TimelineInspector (one of the modes below) [options]");
      System.err.println();
      System.err.println("Modes:");
      System.err.println("  --base-path <path> --show-instant <ts>");
      System.err.println("       deserialize and print all-state rows for one instant");
      System.err.println("  --base-path <path> --find-file-id <id>");
      System.err.println("       table of all events touching the given file id/name");
      System.err.println("  --parse-filename <name>");
      System.err.println("       decode a Hudi parquet/log filename into its parts (no base path needed)");
      System.err.println("  --base-path <path> --raw-archive <ts>");
      System.err.println("       walk archive log files and dump the full HoodieArchivedMetaEntry for");
      System.err.println("       an instant (shows every sibling field, even ones --show-instant skips)");
      System.err.println("  --base-path <path> --commit-stats [--start-instant ts] [--end-instant ts]");
      System.err.println("       per-ingestion-commit (commit/deltacommit/replacecommit) stats:");
      System.err.println("       numInserts, numUpdates, numDeletes, files inserted/updated/deleted,");
      System.err.println("       bytes written, partitions. Footer prints totals + per-commit averages");
      System.err.println("       for inserts/updates/deletes. Honors --actions, --start/end-instant,");
      System.err.println("       --no-archived, --output, --limit.");
      System.err.println("  --base-path <path> --phase-timings [--include-replacecommit]");
      System.err.println("       per-ingestion-instant wall-clock split derived from .hoodie/ state-marker");
      System.err.println("       file mtimes on the active timeline. Phases: sourceReadIndexSfh,");
      System.err.println("       dataWrite, mdtPrep, mdtWrite, mdtTail, total. MDT phases are populated");
      System.err.println("       per-instant when the metadata-table instant exists; rows with any missing");
      System.err.println("       state file are excluded from output and counted in the footer. Footer");
      System.err.println("       reports per-action mean/p50/p95/max + phaseShareOfTotal. Active timeline");
      System.err.println("       only (archived instants lack the .requested/.inflight files needed).");
      System.err.println();
      System.err.println("Options:");
      System.err.println("  --base-path <path>        table base path (required for show/find modes)");
      System.err.println("  --include-archived <bool> include archived timeline (default true)");
      System.err.println("  --no-archived             shortcut for --include-archived false");
      System.err.println("  --start-instant <ts>      only consider instants >= this timestamp");
      System.err.println("  --end-instant <ts>        only consider instants <= this timestamp");
      System.err.println("  --actions <csv>           restrict to these actions");
      System.err.println("                            (commit,deltacommit,replacecommit,clean,rollback,");
      System.err.println("                             restore,compaction,logcompaction,savepoint)");
      System.err.println("  --state <state>           in --show-instant, restrict to one of");
      System.err.println("                            REQUESTED|INFLIGHT|COMPLETED (default: show all)");
      System.err.println("  --lifecycle               in --find-file-id, collapse to landmark rows");
      System.err.println("                            (created / replaced / cleaned / rolled-back) plus summary");
      System.err.println("  --output <fmt>            TABLE (default) or JSON output format");
      System.err.println("  --limit <N>               max rows to print in find-file-id (default "
          + DEFAULT_LIMIT + ")");
      System.err.println("  --sort <asc|desc>         instant sort order for --commit-stats /");
      System.err.println("                            --phase-timings (default desc — newest first,");
      System.err.println("                            so --limit N returns the latest N in range)");
      System.err.println("  --include-replacecommit   in --phase-timings, also report replacecommit");
      System.err.println("                            instants (clustering / insert-overwrite); off by");
      System.err.println("                            default since the focus is ingest cadence");
      System.err.println("  --quiet (-q)              suppress per-instant deserialization warnings");
    }
  }
}
