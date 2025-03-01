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

import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.HoodiePrintHelper;
import org.apache.hudi.cli.HoodieTableHeaderFields;
import org.apache.hudi.cli.TableHeader;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantComparator;
import org.apache.hudi.common.table.timeline.InstantFileNameParser;
import org.apache.hudi.common.table.timeline.InstantGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
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

  private static final Logger LOG = LoggerFactory.getLogger(TimelineCommand.class);
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
            getInstantInfoFromTimeline(metaClient, metaClient.getStorage(), metaClient.getTimelinePath()),
            getInstantInfoFromTimeline(mtMetaClient, mtMetaClient.getStorage(), mtMetaClient.getTimelinePath()),
            limit, sortByField, descending, headerOnly, true, showTimeSeconds, showRollbackInfo);
      }
      return printTimelineInfo(
          metaClient.getActiveTimeline(),
          getInstantInfoFromTimeline(metaClient, metaClient.getStorage(), metaClient.getTimelinePath()),
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
          getInstantInfoFromTimeline(metaClient, metaClient.getStorage(), metaClient.getTimelinePath()),
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
          getInstantInfoFromTimeline(metaClient, metaClient.getStorage(), metaClient.getTimelinePath()),
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
          getInstantInfoFromTimeline(metaClient, metaClient.getStorage(), metaClient.getTimelinePath()),
          limit, sortByField, descending, headerOnly, true, showTimeSeconds, false);
    } catch (IOException e) {
      e.printStackTrace();
      return e.getMessage();
    }
  }

  private HoodieTableMetaClient getMetadataTableMetaClient(HoodieTableMetaClient metaClient) {
    return HoodieTableMetaClient.builder().setConf(HoodieCLI.conf.newInstance())
        .setBasePath(HoodieTableMetadata.getMetadataTableBasePath(metaClient.getBasePath()))
        .setLoadActiveTimelineOnLoad(false)
        .setConsistencyGuardConfig(HoodieCLI.consistencyGuardConfig)
        .build();
  }

  private Map<String, Map<HoodieInstant.State, HoodieInstantWithModTime>> getInstantInfoFromTimeline(
      HoodieTableMetaClient metaClient, HoodieStorage storage, StoragePath metaPath) throws IOException {
    Map<String, Map<HoodieInstant.State, HoodieInstantWithModTime>> instantMap = new HashMap<>();

    final InstantFileNameParser instantFileNameParser = metaClient.getInstantFileNameParser();
    final InstantComparator instantComparator = metaClient.getTimelineLayout().getInstantComparator();
    final InstantGenerator instantGenerator = metaClient.getInstantGenerator();

    Stream<HoodieInstantWithModTime> instantStream =
        HoodieTableMetaClient.scanFiles(storage, metaPath, path -> {
          // Include only the meta files with extensions that needs to be included
          String extension = instantFileNameParser.getTimelineFileExtension(path.getName());
          return metaClient.getActiveTimeline().getValidExtensionsInActiveTimeline().contains(extension);
        }).stream().map(storagePathInfo -> new HoodieInstantWithModTime(storagePathInfo, instantGenerator, instantComparator));
    instantStream.forEach(instant -> {
      instantMap.computeIfAbsent(instant.requestedTime(), t -> new HashMap<>())
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
    Map<String, List<String>> rollbackInfoMap = getRolledBackInstantInfo(timeline);
    final List<Comparable[]> rows = timeline.getInstantsAsStream().map(instant -> {
      Comparable[] row = new Comparable[6];
      String instantTimestamp = instant.requestedTime();
      String rollbackInfoString = showRollbackInfo
          ? getRollbackInfoString(Option.of(instant), timeline, rollbackInfoMap) : "";

      row[0] = instantTimestamp;
      row[1] = instant.getAction() + rollbackInfoString;
      row[2] = instant.getState();
      row[3] = getFormattedDate(
          instantTimestamp, HoodieInstant.State.REQUESTED, instantInfoMap, showTimeSeconds);
      row[4] = getFormattedDate(
          instantTimestamp, HoodieInstant.State.INFLIGHT, instantInfoMap, showTimeSeconds);
      row[5] = getFormattedDate(
          instantTimestamp, HoodieInstant.State.COMPLETED, instantInfoMap, showTimeSeconds);
      return row;
    }).collect(Collectors.toList());
    TableHeader header = new TableHeader()
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_INSTANT)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_ACTION)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_STATE)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_REQUESTED_TIME)
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
    Map<String, List<String>> dtRollbackInfoMap = getRolledBackInstantInfo(dtTimeline);
    Map<String, List<String>> mtRollbackInfoMap = getRolledBackInstantInfo(mtTimeline);

    final List<Comparable[]> rows = instantTimeList.stream().map(instantTimestamp -> {
      Option<HoodieInstant> dtInstant = getInstant(dtTimeline, instantTimestamp);
      Option<HoodieInstant> mtInstant = getInstant(mtTimeline, instantTimestamp);
      Comparable[] row = new Comparable[11];
      row[0] = instantTimestamp;
      String dtRollbackInfoString = showRollbackInfo
          ? getRollbackInfoString(dtInstant, dtTimeline, dtRollbackInfoMap) : "";
      row[1] = (dtInstant.isPresent() ? dtInstant.get().getAction() : "-") + dtRollbackInfoString;
      row[2] = dtInstant.isPresent() ? dtInstant.get().getState() : "-";
      row[3] = getFormattedDate(
          instantTimestamp, HoodieInstant.State.REQUESTED, dtInstantInfoMap, showTimeSeconds);
      row[4] = getFormattedDate(
          instantTimestamp, HoodieInstant.State.INFLIGHT, dtInstantInfoMap, showTimeSeconds);
      row[5] = getFormattedDate(
          instantTimestamp, HoodieInstant.State.COMPLETED, dtInstantInfoMap, showTimeSeconds);

      String mtRollbackInfoString = showRollbackInfo
          ? getRollbackInfoString(mtInstant, mtTimeline, mtRollbackInfoMap) : "";
      row[6] = (mtInstant.isPresent() ? mtInstant.get().getAction() : "-") + mtRollbackInfoString;
      row[7] = mtInstant.isPresent() ? mtInstant.get().getState() : "-";
      row[8] = getFormattedDate(
          instantTimestamp, HoodieInstant.State.REQUESTED, mtInstantInfoMap, showTimeSeconds);
      row[9] = getFormattedDate(
          instantTimestamp, HoodieInstant.State.INFLIGHT, mtInstantInfoMap, showTimeSeconds);
      row[10] = getFormattedDate(
          instantTimestamp, HoodieInstant.State.COMPLETED, mtInstantInfoMap, showTimeSeconds);
      return row;
    }).collect(Collectors.toList());
    TableHeader header = new TableHeader()
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_INSTANT)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_ACTION)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_STATE)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_REQUESTED_TIME)
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
    return timeline.filter(instant -> instant.requestedTime().equals(instantTimestamp)).firstInstant();
  }

  private String getInstantToRollback(HoodieTimeline timeline, HoodieInstant instant) {
    try {
      if (instant.isInflight()) {
        HoodieInstant instantToUse = HoodieCLI.getTableMetaClient().createNewInstant(
            HoodieInstant.State.REQUESTED, instant.getAction(), instant.requestedTime());
        HoodieRollbackPlan metadata = timeline.readRollbackPlan(instantToUse);
        return metadata.getInstantToRollback().getCommitTime();
      } else {
        HoodieRollbackMetadata metadata = timeline.readRollbackMetadata(instant);
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
            HoodieTimeline.ROLLBACK_ACTION.equalsIgnoreCase(instant.getAction())).getInstants();
    rollbackInstants.forEach(rollbackInstant -> {
      try {
        if (rollbackInstant.isInflight()) {
          HoodieInstant instantToUse = HoodieCLI.getTableMetaClient().createNewInstant(
              HoodieInstant.State.REQUESTED, rollbackInstant.getAction(), rollbackInstant.requestedTime());
          HoodieRollbackPlan metadata = timeline.readRollbackPlan(instantToUse);
          rollbackInfoMap.computeIfAbsent(metadata.getInstantToRollback().getCommitTime(), k -> new ArrayList<>())
              .add(rollbackInstant.requestedTime());
        } else {
          HoodieRollbackMetadata metadata = timeline.readRollbackMetadata(rollbackInstant);
          metadata.getCommitsRollback().forEach(instant -> {
            rollbackInfoMap.computeIfAbsent(instant, k -> new ArrayList<>())
                .add(rollbackInstant.requestedTime());
          });
        }
      } catch (IOException e) {
        LOG.error(String.format("Error reading rollback info of %s", rollbackInstant));
        e.printStackTrace();
      }
    });
    return rollbackInfoMap;
  }

  private String getRollbackInfoString(Option<HoodieInstant> instant,
                                       HoodieTimeline timeline,
                                       Map<String, List<String>> rollbackInfoMap) {
    String rollbackInfoString = "";
    if (instant.isPresent()) {
      if (HoodieTimeline.ROLLBACK_ACTION.equalsIgnoreCase(instant.get().getAction())) {
        rollbackInfoString = "\nRolls back\n" + getInstantToRollback(timeline, instant.get());
      } else {
        String instantTimestamp = instant.get().requestedTime();
        if (rollbackInfoMap.containsKey(instantTimestamp)) {
          rollbackInfoString = "\nRolled back by\n" + String.join(",\n", rollbackInfoMap.get(instantTimestamp));
        }
      }
    }
    return rollbackInfoString;
  }

  static class HoodieInstantWithModTime extends HoodieInstant {

    private final long modificationTimeMs;

    public HoodieInstantWithModTime(StoragePathInfo pathInfo, InstantGenerator instantGenerator, InstantComparator instantComparator) {
      this(instantGenerator.createNewInstant(pathInfo), pathInfo, instantComparator.requestedTimeOrderedComparator());
    }

    public HoodieInstantWithModTime(HoodieInstant instant, StoragePathInfo pathInfo, Comparator<HoodieInstant> comparator) {
      super(instant.getState(), instant.getAction(), instant.requestedTime(), instant.getCompletionTime(), comparator);
      this.modificationTimeMs = pathInfo.getModificationTime();
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
