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

package org.apache.hudi.cli.commands;

import org.apache.hudi.avro.model.HoodieInstantInfo;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRestorePlan;
import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.HoodiePrintHelper;
import org.apache.hudi.cli.HoodieTableHeaderFields;
import org.apache.hudi.cli.TableHeader;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;

import lombok.extern.slf4j.Slf4j;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.RESTORE_ACTION;

/**
 * CLI command to display info about restore actions.
 */
@ShellComponent
@Slf4j
public class RestoresCommand {

  @ShellMethod(key = "show restores", value = "List all restore instants")
  public String showRestores(
          @ShellOption(value = {"--limit"}, help = "Limit #rows to be displayed", defaultValue = "10") Integer limit,
          @ShellOption(value = {"--sortBy"}, help = "Sorting Field", defaultValue = "") final String sortByField,
          @ShellOption(value = {"--desc"}, help = "Ordering", defaultValue = "false") final boolean descending,
          @ShellOption(value = {"--headeronly"}, help = "Print Header Only",
                  defaultValue = "false") final boolean headerOnly,
          @ShellOption(value = {"--includeInflights"}, help = "Also list restores that are in flight",
                  defaultValue = "false") final boolean includeInflights) {

    HoodieActiveTimeline activeTimeline = HoodieCLI.getTableMetaClient().getActiveTimeline();
    List<HoodieInstant> restoreInstants = getRestoreInstants(activeTimeline, includeInflights);

    final List<Comparable[]> outputRows = new ArrayList<>();
    for (HoodieInstant restoreInstant : restoreInstants) {
      populateOutputFromRestoreInstant(restoreInstant, outputRows, activeTimeline);
    }

    TableHeader header = createResultHeader();
    return HoodiePrintHelper.print(header, new HashMap<>(), sortByField, descending, limit, headerOnly, outputRows);
  }

  @ShellMethod(key = "show restore", value = "Show details of a restore instant")
  public String showRestore(
          @ShellOption(value = {"--instant"}, help = "Restore instant") String restoreInstant,
          @ShellOption(value = {"--limit"}, help = "Limit #rows to be displayed", defaultValue = "10") Integer limit,
          @ShellOption(value = {"--sortBy"}, help = "Sorting Field", defaultValue = "") final String sortByField,
          @ShellOption(value = {"--desc"}, help = "Ordering", defaultValue = "false") final boolean descending,
          @ShellOption(value = {"--headeronly"}, help = "Print Header Only",
                  defaultValue = "false") final boolean headerOnly) {

    HoodieActiveTimeline activeTimeline = HoodieCLI.getTableMetaClient().getActiveTimeline();
    List<HoodieInstant> matchingInstants = activeTimeline.filterCompletedInstants().filter(completed ->
            completed.requestedTime().equals(restoreInstant)).getInstants();
    if (matchingInstants.isEmpty()) {
      matchingInstants = activeTimeline.filterInflights().filter(inflight ->
              inflight.requestedTime().equals(restoreInstant)).getInstants();
    }

    // Assuming a single exact match is found in either completed or inflight instants
    HoodieInstant instant = matchingInstants.get(0);
    List<Comparable[]> outputRows = new ArrayList<>();
    populateOutputFromRestoreInstant(instant, outputRows, activeTimeline);

    TableHeader header = createResultHeader();
    return HoodiePrintHelper.print(header, new HashMap<>(), sortByField, descending, limit, headerOnly, outputRows);
  }

  private void addDetailsOfCompletedRestore(HoodieActiveTimeline activeTimeline, List<Comparable[]> rows,
                                            HoodieInstant restoreInstant) throws IOException {
    HoodieRestoreMetadata instantMetadata = activeTimeline.readRestoreMetadata(restoreInstant);

    for (String rolledbackInstant : instantMetadata.getInstantsToRollback()) {
      Comparable[] row = createDataRow(instantMetadata.getStartRestoreTime(), rolledbackInstant,
              instantMetadata.getTimeTakenInMillis(), restoreInstant.getState());
      rows.add(row);
    }
  }

  private void addDetailsOfInflightRestore(HoodieActiveTimeline activeTimeline, List<Comparable[]> rows,
                                           HoodieInstant restoreInstant) throws IOException {
    HoodieRestorePlan restorePlan = getRestorePlan(activeTimeline, restoreInstant);
    for (HoodieInstantInfo instantToRollback : restorePlan.getInstantsToRollback()) {
      Comparable[] dataRow = createDataRow(restoreInstant.requestedTime(), instantToRollback.getCommitTime(), "",
              restoreInstant.getState());
      rows.add(dataRow);
    }
  }

  private HoodieRestorePlan getRestorePlan(HoodieActiveTimeline activeTimeline, HoodieInstant restoreInstant) throws IOException {
    HoodieInstant instantKey = HoodieCLI.getTableMetaClient().createNewInstant(HoodieInstant.State.REQUESTED, RESTORE_ACTION,
            restoreInstant.requestedTime());
    return activeTimeline.readRestorePlan(instantKey);
  }

  private List<HoodieInstant> getRestoreInstants(HoodieActiveTimeline activeTimeline, boolean includeInFlight) {
    List<HoodieInstant> restores = new ArrayList<>();
    restores.addAll(activeTimeline.getRestoreTimeline().filterCompletedInstants().getInstants());

    if (includeInFlight) {
      restores.addAll(activeTimeline.getRestoreTimeline().filterInflights().getInstants());
    }

    return restores;
  }

  private TableHeader createResultHeader() {
    return new TableHeader()
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_INSTANT)
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_RESTORE_INSTANT)
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_TIME_TOKEN_MILLIS)
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_RESTORE_STATE);
  }

  private void populateOutputFromRestoreInstant(HoodieInstant restoreInstant, List<Comparable[]> outputRows,
                                                HoodieActiveTimeline activeTimeline) {
    try {
      if (restoreInstant.isInflight() || restoreInstant.isRequested()) {
        addDetailsOfInflightRestore(activeTimeline, outputRows, restoreInstant);
      } else if (restoreInstant.isCompleted()) {
        addDetailsOfCompletedRestore(activeTimeline, outputRows, restoreInstant);
      }
    } catch (IOException e) {
      log.error("Error reading restore metadata for instant {}", restoreInstant, e);
    }
  }

  private Comparable[] createDataRow(Comparable restoreInstantTimestamp, Comparable rolledbackInstantTimestamp,
                                         Comparable timeInterval, Comparable state) {
    Comparable[] row = new Comparable[4];
    row[0] = restoreInstantTimestamp;
    row[1] = rolledbackInstantTimestamp;
    row[2] = timeInterval;
    row[3] = state;
    return row;
  }

}
