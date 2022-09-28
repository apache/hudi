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

package org.apache.hudi.cli.commands;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.HoodiePrintHelper;
import org.apache.hudi.cli.HoodieTableHeaderFields;
import org.apache.hudi.cli.TableHeader;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * CLI command to display timeline options.
 */
@ShellComponent
public class TimelineCommand {

  private static final Logger LOG = LogManager.getLogger(TimelineCommand.class);
  private static final SimpleDateFormat DATE_FORMAT_DEFAULT = new SimpleDateFormat("MM-dd HH:mm");
  private static final SimpleDateFormat DATE_FORMAT_SECONDS = new SimpleDateFormat("MM-dd HH:mm:ss");

  @ShellMethod(key = "timeline show active", value = "List all instants in active timeline")
  public String showActive(
      @ShellOption(value = {"--limit"}, help = "Limit #rows to be displayed", defaultValue = "10") Integer limit,
      @ShellOption(value = {"--sortBy"}, help = "Sorting Field", defaultValue = "") final String sortByField,
      @ShellOption(value = {"--desc"}, help = "Ordering", defaultValue = "false") final boolean descending,
      @ShellOption(value = {"--headeronly"}, help = "Print Header Only",
          defaultValue = "false") final boolean headerOnly,
      @ShellOption(value = {"--with-metadata-table"}, help = "Show metadata table timeline together with data table",
          defaultValue = "false") final boolean withMetadataTable,
      @ShellOption(value = {"--show-rollback-info"}, help = "Show instant to rollback for rollbacks",
          defaultValue = "false") final boolean showRollbackInfo,
      @ShellOption(value = {"--show-time-seconds"}, help = "Show seconds in instant file modification time",
          defaultValue = "false") final boolean showTimeSeconds) {
    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();
    try {
      if (withMetadataTable) {
        HoodieTableMetaClient mtMetaClient = getMetadataTableMetaClient(metaClient);
        return printTimelineInfoWithMetadataTable(
            metaClient.getActiveTimeline(), mtMetaClient.getActiveTimeline(),
            getInstantInfoFromTimeline(metaClient.getFs(), metaClient.getMetaPath()),
            getInstantInfoFromTimeline(mtMetaClient.getFs(), mtMetaClient.getMetaPath()),
            limit, sortByField, descending, headerOnly, true, showTimeSeconds, showRollbackInfo);
      }
      return printTimelineInfo(
          metaClient.getActiveTimeline(),
          getInstantInfoFromTimeline(metaClient.getFs(), metaClient.getMetaPath()),
          limit, sortByField, descending, headerOnly, true, showTimeSeconds, showRollbackInfo);
    } catch (IOException e) {
      e.printStackTrace();
      return e.getMessage();
    }
  }

  @ShellMethod(key = "timeline show incomplete", value = "List all incomplete instants in active timeline")
  public String showIncomplete(
      @ShellOption(value = {"--limit"}, help = "Limit #rows to be displayed", defaultValue = "10") Integer limit,
      @ShellOption(value = {"--sortBy"}, help = "Sorting Field", defaultValue = "") final String sortByField,
      @ShellOption(value = {"--desc"}, help = "Ordering", defaultValue = "false") final boolean descending,
      @ShellOption(value = {"--headeronly"}, help = "Print Header Only",
          defaultValue = "false") final boolean headerOnly,
      @ShellOption(value = {"--show-rollback-info"}, help = "Show instant to rollback for rollbacks",
          defaultValue = "false") final boolean showRollbackInfo,
      @ShellOption(value = {"--show-time-seconds"}, help = "Show seconds in instant file modification time",
          defaultValue = "false") final boolean showTimeSeconds) {
    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();
    try {
      return printTimelineInfo(
          metaClient.getActiveTimeline().filterInflightsAndRequested(),
          getInstantInfoFromTimeline(metaClient.getFs(), metaClient.getMetaPath()),
          limit, sortByField, descending, headerOnly, true, showTimeSeconds, showRollbackInfo);
    } catch (IOException e) {
      e.printStackTrace();
      return e.getMessage();
    }
  }

  @ShellMethod(key = "metadata timeline show active",
      value = "List all instants in active timeline of metadata table")
  public String metadataShowActive(
      @ShellOption(value = {"--limit"}, help = "Limit #rows to be displayed", defaultValue = "10") Integer limit,
      @ShellOption(value = {"--sortBy"}, help = "Sorting Field", defaultValue = "") final String sortByField,
      @ShellOption(value = {"--desc"}, help = "Ordering", defaultValue = "false") final boolean descending,
      @ShellOption(value = {"--headeronly"}, help = "Print Header Only",
          defaultValue = "false") final boolean headerOnly,
      @ShellOption(value = {"--show-time-seconds"}, help = "Show seconds in instant file modification time",
          defaultValue = "false") final boolean showTimeSeconds) {
    HoodieTableMetaClient metaClient = getMetadataTableMetaClient(HoodieCLI.getTableMetaClient());
    try {
      return printTimelineInfo(
          metaClient.getActiveTimeline(),
          getInstantInfoFromTimeline(metaClient.getFs(), metaClient.getMetaPath()),
          limit, sortByField, descending, headerOnly, true, showTimeSeconds, false);
    } catch (IOException e) {
      e.printStackTrace();
      return e.getMessage();
    }
  }

  @ShellMethod(key = "metadata timeline show incomplete",
      value = "List all incomplete instants in active timeline of metadata table")
  public String metadataShowIncomplete(
      @ShellOption(value = {"--limit"}, help = "Limit #rows to be displayed", defaultValue = "10") Integer limit,
      @ShellOption(value = {"--sortBy"}, help = "Sorting Field", defaultValue = "") final String sortByField,
      @ShellOption(value = {"--desc"}, help = "Ordering", defaultValue = "false") final boolean descending,
      @ShellOption(value = {"--headeronly"}, help = "Print Header Only",
          defaultValue = "false") final boolean headerOnly,
      @ShellOption(value = {"--show-time-seconds"}, help = "Show seconds in instant file modification time",
          defaultValue = "false") final boolean showTimeSeconds) {
    HoodieTableMetaClient metaClient = getMetadataTableMetaClient(HoodieCLI.getTableMetaClient());
    try {
      return printTimelineInfo(
          metaClient.getActiveTimeline().filterInflightsAndRequested(),
          getInstantInfoFromTimeline(metaClient.getFs(), metaClient.getMetaPath()),
          limit, sortByField, descending, headerOnly, true, showTimeSeconds, false);
    } catch (IOException e) {
      e.printStackTrace();
      return e.getMessage();
    }
  }

  private HoodieTableMetaClient getMetadataTableMetaClient(HoodieTableMetaClient metaClient) {
    return HoodieTableMetaClient.builder().setConf(HoodieCLI.conf)
        .setBasePath(HoodieTableMetadata.getMetadataTableBasePath(metaClient.getBasePath()))
        .setLoadActiveTimelineOnLoad(false)
        .setConsistencyGuardConfig(HoodieCLI.consistencyGuardConfig)
        .build();
  }

  private Map<String, Map<HoodieInstant.State, HoodieInstantWithModTime>> getInstantInfoFromTimeline(
      FileSystem fs, String metaPath) throws IOException {
    Map<String, Map<HoodieInstant.State, HoodieInstantWithModTime>> instantMap = new HashMap<>();
    Stream<HoodieInstantWithModTime> instantStream = Arrays.stream(
        HoodieTableMetaClient.scanFiles(fs, new Path(metaPath), path -> {
          // Include only the meta files with extensions that needs to be included
          String extension = HoodieInstant.getTimelineFileExtension(path.getName());
          return HoodieActiveTimeline.VALID_EXTENSIONS_IN_ACTIVE_TIMELINE.contains(extension);
        })).map(HoodieInstantWithModTime::new);
    instantStream.forEach(instant -> {
      instantMap.computeIfAbsent(instant.getTimestamp(), t -> new HashMap<>())
          .put(instant.getState(), instant);
    });
    return instantMap;
  }

  private String getFormattedDate(
      String instantTimestamp, HoodieInstant.State state,
      Map<String, Map<HoodieInstant.State, HoodieInstantWithModTime>> instantInfoMap,
      boolean showTimeSeconds) {
    Long timeMs = null;
    Map<HoodieInstant.State, HoodieInstantWithModTime> mapping = instantInfoMap.get(instantTimestamp);
    if (mapping != null && mapping.containsKey(state)) {
      timeMs = mapping.get(state).getModificationTime();
    }
    SimpleDateFormat sdf = showTimeSeconds ? DATE_FORMAT_SECONDS : DATE_FORMAT_DEFAULT;
    return timeMs != null ? sdf.format(new Date(timeMs)) : "-";
  }

  private String printTimelineInfo(
      HoodieTimeline timeline,
      Map<String, Map<HoodieInstant.State, HoodieInstantWithModTime>> instantInfoMap,
      Integer limit, String sortByField, boolean descending, boolean headerOnly, boolean withRowNo,
      boolean showTimeSeconds, boolean showRollbackInfo) {
    Map<String, List<String>> rollbackInfo = getRolledBackInstantInfo(timeline);
    final List<Comparable[]> rows = timeline.getInstants().map(instant -> {
      int numColumns = showRollbackInfo ? 7 : 6;
      Comparable[] row = new Comparable[numColumns];
      String instantTimestamp = instant.getTimestamp();
      row[0] = instantTimestamp;
      row[1] = instant.getAction();
      row[2] = instant.getState();
      if (showRollbackInfo) {
        if (HoodieTimeline.ROLLBACK_ACTION.equalsIgnoreCase(instant.getAction())) {
          row[3] = "Rolls back\n" + getInstantToRollback(timeline, instant);
        } else {
          if (rollbackInfo.containsKey(instantTimestamp)) {
            row[3] = "Rolled back by\n" + String.join(",\n", rollbackInfo.get(instantTimestamp));
          } else {
            row[3] = "-";
          }
        }
      }
      row[numColumns - 3] = getFormattedDate(
          instantTimestamp, HoodieInstant.State.REQUESTED, instantInfoMap, showTimeSeconds);
      row[numColumns - 2] = getFormattedDate(
          instantTimestamp, HoodieInstant.State.INFLIGHT, instantInfoMap, showTimeSeconds);
      row[numColumns - 1] = getFormattedDate(
          instantTimestamp, HoodieInstant.State.COMPLETED, instantInfoMap, showTimeSeconds);
      return row;
    }).collect(Collectors.toList());
    TableHeader header = new TableHeader()
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_INSTANT)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_ACTION)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_STATE);
    if (showRollbackInfo) {
      header.addTableHeaderField(HoodieTableHeaderFields.HEADER_ROLLBACK_INFO);
    }
    header.addTableHeaderField(HoodieTableHeaderFields.HEADER_REQUESTED_TIME)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_INFLIGHT_TIME)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_COMPLETED_TIME);
    return HoodiePrintHelper.print(
        header, new HashMap<>(), withRowNo, sortByField, descending, limit, headerOnly, rows);
  }

  private String printTimelineInfoWithMetadataTable(
      HoodieTimeline dtTimeline, HoodieTimeline mtTimeline,
      Map<String, Map<HoodieInstant.State, HoodieInstantWithModTime>> dtInstantInfoMap,
      Map<String, Map<HoodieInstant.State, HoodieInstantWithModTime>> mtInstantInfoMap,
      Integer limit, String sortByField, boolean descending, boolean headerOnly, boolean withRowNo,
      boolean showTimeSeconds, boolean showRollbackInfo) {
    Set<String> instantTimeSet = new HashSet(dtInstantInfoMap.keySet());
    instantTimeSet.addAll(mtInstantInfoMap.keySet());
    List<String> instantTimeList = instantTimeSet.stream()
        .sorted(new HoodieInstantTimeComparator()).collect(Collectors.toList());
    Map<String, List<String>> dtRollbackInfo = getRolledBackInstantInfo(dtTimeline);

    final List<Comparable[]> rows = instantTimeList.stream().map(instantTimestamp -> {
      int numColumns = showRollbackInfo ? 12 : 11;
      Option<HoodieInstant> dtInstant = getInstant(dtTimeline, instantTimestamp);
      Option<HoodieInstant> mtInstant = getInstant(mtTimeline, instantTimestamp);
      Comparable[] row = new Comparable[numColumns];
      row[0] = instantTimestamp;
      row[1] = dtInstant.isPresent() ? dtInstant.get().getAction() : "-";
      row[2] = dtInstant.isPresent() ? dtInstant.get().getState() : "-";
      if (showRollbackInfo) {
        if (dtInstant.isPresent()
            && HoodieTimeline.ROLLBACK_ACTION.equalsIgnoreCase(dtInstant.get().getAction())) {
          row[3] = "Rolls back\n" + getInstantToRollback(dtTimeline, dtInstant.get());
        } else {
          if (dtRollbackInfo.containsKey(instantTimestamp)) {
            row[3] = "Rolled back by\n" + String.join(",\n", dtRollbackInfo.get(instantTimestamp));
          } else {
            row[3] = "-";
          }
        }
      }
      row[numColumns - 8] = getFormattedDate(
          instantTimestamp, HoodieInstant.State.REQUESTED, dtInstantInfoMap, showTimeSeconds);
      row[numColumns - 7] = getFormattedDate(
          instantTimestamp, HoodieInstant.State.INFLIGHT, dtInstantInfoMap, showTimeSeconds);
      row[numColumns - 6] = getFormattedDate(
          instantTimestamp, HoodieInstant.State.COMPLETED, dtInstantInfoMap, showTimeSeconds);
      row[numColumns - 5] = mtInstant.isPresent() ? mtInstant.get().getAction() : "-";
      row[numColumns - 4] = mtInstant.isPresent() ? mtInstant.get().getState() : "-";
      row[numColumns - 3] = getFormattedDate(
          instantTimestamp, HoodieInstant.State.REQUESTED, mtInstantInfoMap, showTimeSeconds);
      row[numColumns - 2] = getFormattedDate(
          instantTimestamp, HoodieInstant.State.INFLIGHT, mtInstantInfoMap, showTimeSeconds);
      row[numColumns - 1] = getFormattedDate(
          instantTimestamp, HoodieInstant.State.COMPLETED, mtInstantInfoMap, showTimeSeconds);
      return row;
    }).collect(Collectors.toList());
    TableHeader header = new TableHeader()
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_INSTANT)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_ACTION)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_STATE);
    if (showRollbackInfo) {
      header.addTableHeaderField(HoodieTableHeaderFields.HEADER_ROLLBACK_INFO);
    }
    header.addTableHeaderField(HoodieTableHeaderFields.HEADER_REQUESTED_TIME)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_INFLIGHT_TIME)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_COMPLETED_TIME)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_MT_ACTION)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_MT_STATE)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_MT_REQUESTED_TIME)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_MT_INFLIGHT_TIME)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_MT_COMPLETED_TIME);
    return HoodiePrintHelper.print(
        header, new HashMap<>(), withRowNo, sortByField, descending, limit, headerOnly, rows);
  }

  private Option<HoodieInstant> getInstant(HoodieTimeline timeline, String instantTimestamp) {
    return timeline.filter(instant -> instant.getTimestamp().equals(instantTimestamp)).firstInstant();
  }

  private String getInstantToRollback(HoodieTimeline timeline, HoodieInstant instant) {
    try {
      if (instant.isInflight()) {
        HoodieInstant instantToUse = new HoodieInstant(
            HoodieInstant.State.REQUESTED, instant.getAction(), instant.getTimestamp());
        HoodieRollbackPlan metadata = TimelineMetadataUtils
            .deserializeAvroMetadata(timeline.getInstantDetails(instantToUse).get(), HoodieRollbackPlan.class);
        return metadata.getInstantToRollback().getCommitTime();
      } else {
        HoodieRollbackMetadata metadata = TimelineMetadataUtils
            .deserializeAvroMetadata(timeline.getInstantDetails(instant).get(), HoodieRollbackMetadata.class);
        return String.join(",", metadata.getCommitsRollback());
      }
    } catch (IOException e) {
      LOG.error(String.format("Error reading rollback info of %s", instant));
      e.printStackTrace();
      return "-";
    }
  }

  private Map<String, List<String>> getRolledBackInstantInfo(HoodieTimeline timeline) {
    // Instant rolled back or to roll back -> rollback instants
    Map<String, List<String>> rollbackInfoMap = new HashMap<>();
    List<HoodieInstant> rollbackInstants = timeline.filter(instant ->
            HoodieTimeline.ROLLBACK_ACTION.equalsIgnoreCase(instant.getAction()))
        .getInstants().collect(Collectors.toList());
    rollbackInstants.forEach(rollbackInstant -> {
      try {
        if (rollbackInstant.isInflight()) {
          HoodieInstant instantToUse = new HoodieInstant(
              HoodieInstant.State.REQUESTED, rollbackInstant.getAction(), rollbackInstant.getTimestamp());
          HoodieRollbackPlan metadata = TimelineMetadataUtils
              .deserializeAvroMetadata(timeline.getInstantDetails(instantToUse).get(), HoodieRollbackPlan.class);
          rollbackInfoMap.computeIfAbsent(metadata.getInstantToRollback().getCommitTime(), k -> new ArrayList<>())
              .add(rollbackInstant.getTimestamp());
        } else {
          HoodieRollbackMetadata metadata = TimelineMetadataUtils
              .deserializeAvroMetadata(timeline.getInstantDetails(rollbackInstant).get(), HoodieRollbackMetadata.class);
          metadata.getCommitsRollback().forEach(instant -> {
            rollbackInfoMap.computeIfAbsent(instant, k -> new ArrayList<>())
                .add(rollbackInstant.getTimestamp());
          });
        }
      } catch (IOException e) {
        LOG.error(String.format("Error reading rollback info of %s", rollbackInstant));
        e.printStackTrace();
      }
    });
    return rollbackInfoMap;
  }

  static class HoodieInstantWithModTime extends HoodieInstant {

    private final long modificationTimeMs;

    public HoodieInstantWithModTime(FileStatus fileStatus) {
      super(fileStatus);
      this.modificationTimeMs = fileStatus.getModificationTime();
    }

    public long getModificationTime() {
      return modificationTimeMs;
    }
  }

  static class HoodieInstantTimeComparator implements Comparator<String> {
    @Override
    public int compare(String o1, String o2) {
      // For metadata table, the compaction instant time is "012345001" while the delta commit
      // later is "012345", i.e., the compaction instant time has trailing "001".  In the
      // actual event sequence, metadata table compaction happens before the corresponding
      // delta commit.  For better visualization, we put "012345001" before "012345"
      // when sorting in ascending order.
      if (o1.length() != o2.length()) {
        // o1 is longer than o2
        if (o1.length() - o2.length() == 3 && o1.endsWith("001") && o1.startsWith(o2)) {
          return -1;
        }
        // o1 is shorter than o2
        if (o2.length() - o1.length() == 3 && o2.endsWith("001") && o2.startsWith(o1)) {
          return 1;
        }
      }
      return o1.compareTo(o2);
    }
  }
}
