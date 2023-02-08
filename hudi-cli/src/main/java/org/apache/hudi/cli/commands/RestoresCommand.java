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
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.Option;
import org.jetbrains.annotations.NotNull;
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
public class RestoresCommand {

  @ShellMethod(key = "show restores", value = "List all restore instants")
  public String showRestores(
          @ShellOption(value = {"--limit"}, help = "Limit #rows to be displayed", defaultValue = "10") Integer limit,
          @ShellOption(value = {"--sortBy"}, help = "Sorting Field", defaultValue = "") final String sortByField,
          @ShellOption(value = {"--desc"}, help = "Ordering", defaultValue = "false") final boolean descending,
          @ShellOption(value = {"--headeronly"}, help = "Print Header Only",
                  defaultValue = "false") final boolean headerOnly,
          @ShellOption(value = {"--inflight"}, help = "Also list restores that are in flight",
                  defaultValue = "false") final boolean showInFlight) {

    HoodieActiveTimeline activeTimeline = HoodieCLI.getTableMetaClient().getActiveTimeline();
    List<HoodieInstant> restoreInstants = getRestoreInstants(activeTimeline, showInFlight);

    final List<Comparable[]> rows = new ArrayList<>();
    for (HoodieInstant restoreInstant : restoreInstants) {
      populateRowsFromRestoreInstant(restoreInstant, rows, activeTimeline);
    };

    TableHeader header = createResultHeader();
    return HoodiePrintHelper.print(header, new HashMap<>(), sortByField, descending, limit, headerOnly, rows);
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
            completed.getTimestamp().equals(restoreInstant)).getInstants();
    if (matchingInstants.isEmpty()) {
      matchingInstants = activeTimeline.filterInflights().filter(inflight ->
              inflight.getTimestamp().equals(restoreInstant)).getInstants();
    }

    // Assuming a single exact match is found in either completed or inflight instants
    HoodieInstant instant = matchingInstants.get(0);
    List<Comparable[]> rows = new ArrayList<>();
    populateRowsFromRestoreInstant(instant, rows, activeTimeline);

    TableHeader header = createResultHeader();
    return HoodiePrintHelper.print(header, new HashMap<>(), sortByField, descending, limit, headerOnly, rows);
  }

  private void addDetailsOfCompletedRestore(HoodieActiveTimeline activeTimeline, List<Comparable[]> rows,
                                            HoodieInstant restoreInstant) throws IOException {
    HoodieRestoreMetadata instantMetadata;
    Option<byte[]> instantDetails = activeTimeline.getInstantDetails(restoreInstant);
    instantMetadata = TimelineMetadataUtils
            .deserializeAvroMetadata(instantDetails.get(), HoodieRestoreMetadata.class);

    for (String rolledbackInstant : instantMetadata.getInstantsToRollback()) {
      Comparable[] row = createDataRow(instantMetadata.getStartRestoreTime(), rolledbackInstant,
              instantMetadata.getTimeTakenInMillis(), restoreInstant.getState());
      rows.add(row);
    }
  }

  private void addDetailsOfInflightRestore(HoodieActiveTimeline activeTimeline, List<Comparable[]> rows,
                                           HoodieInstant restoreInstant) throws IOException {
    HoodieInstant instantKey = new HoodieInstant(HoodieInstant.State.REQUESTED, RESTORE_ACTION,
            restoreInstant.getTimestamp());
    Option<byte[]> instantDetails = activeTimeline.getInstantDetails(instantKey);
    HoodieRestorePlan restorePlan = TimelineMetadataUtils
            .deserializeAvroMetadata(instantDetails.get(), HoodieRestorePlan.class);
    for (HoodieInstantInfo instantToRollback : restorePlan.getInstantsToRollback()) {
      Comparable[] dataRow = createDataRow(restoreInstant.getTimestamp(), instantToRollback.getCommitTime(), "",
              restoreInstant.getState());
      rows.add(dataRow);
    }
  }

  @NotNull
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

  private void populateRowsFromRestoreInstant(HoodieInstant restoreInstant, List<Comparable[]> rows,
                                              HoodieActiveTimeline activeTimeline) {
    try {
      if (restoreInstant.isInflight() || restoreInstant.isRequested()) {
        addDetailsOfInflightRestore(activeTimeline, rows, restoreInstant);
      } else if (restoreInstant.isCompleted()) {
        addDetailsOfCompletedRestore(activeTimeline, rows, restoreInstant);
      }
    } catch (IOException e) {
      e.printStackTrace();
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
