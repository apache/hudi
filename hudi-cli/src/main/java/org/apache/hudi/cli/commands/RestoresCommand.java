package org.apache.hudi.cli.commands;

import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.HoodiePrintHelper;
import org.apache.hudi.cli.HoodieTableHeaderFields;
import org.apache.hudi.cli.TableHeader;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.jetbrains.annotations.NotNull;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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
          @ShellOption(value = {"--show-inflight"}, help = "Also list restores that are in flight",
                  defaultValue = "false") final boolean showInFlight) {

    HoodieActiveTimeline activeTimeline = HoodieCLI.getTableMetaClient().getActiveTimeline();
    List<HoodieInstant> hoodieInstants = getInstants(activeTimeline, showInFlight);

    final List<Comparable[]> rows = new ArrayList<>();
    TableHeader header = new TableHeader()
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_INSTANT)
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_RESTORE_INSTANT)
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_TIME_TOKEN_MILLIS)
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_RESTORE_STATE);

    hoodieInstants.forEach(restoreInstant -> {
        try {
          HoodieRestoreMetadata instantMetadata = TimelineMetadataUtils
                  .deserializeAvroMetadata(activeTimeline.getInstantDetails(restoreInstant).get(),
                          HoodieRestoreMetadata.class);
          instantMetadata.getInstantsToRollback().forEach((String rolledbackInstant) -> {
            Comparable[] row = new Comparable[4];
            row[0] = instantMetadata.getStartRestoreTime();
            row[1] = rolledbackInstant;
            row[2] = instantMetadata.getTimeTakenInMillis();
            row[3] = restoreInstant.getState();
            rows.add(row);
          });
        } catch (IOException e) {
          e.printStackTrace();
        }
    });

    return HoodiePrintHelper.print(header, new HashMap<>(), sortByField, descending, limit, headerOnly, rows);
  }

  @ShellMethod(key = "show restore", value = "Show details of a restore instant")
  public String showRestore(
          @ShellOption(value = {"--instant"}, help = "Restore instant") String restoreInstant,
          @ShellOption(value = {"--limit"}, help = "Limit #rows to be displayed", defaultValue = "10") Integer limit,
          @ShellOption(value = {"--sortBy"}, help = "Sorting Field", defaultValue = "") final String sortByField,
          @ShellOption(value = {"--desc"}, help = "Ordering", defaultValue = "false") final boolean descending,
          @ShellOption(value = {"--headeronly"}, help = "Print Header Only",
                  defaultValue = "false") final boolean headerOnly)
          throws IOException {

    HoodieActiveTimeline activeTimeline = HoodieCLI.getTableMetaClient().getActiveTimeline();
    List<HoodieInstant> matchingInstants = activeTimeline.filterCompletedInstants().filter(completed ->
            completed.getTimestamp().equals(restoreInstant)).getInstants();
    if (matchingInstants.isEmpty()) {
      matchingInstants = activeTimeline.filterInflights().filter(inflight ->
              inflight.getTimestamp().equals(restoreInstant)).getInstants();
    }

    // Assuming a single exact match is found in either completed or inflight instants
    HoodieInstant instant = matchingInstants.get(0);
    HoodieRestoreMetadata instantMetadata = TimelineMetadataUtils.deserializeAvroMetadata(
        activeTimeline.getInstantDetails(instant).get(),
            HoodieRestoreMetadata.class);

    TableHeader header = new TableHeader()
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_INSTANT)
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_RESTORE_INSTANT)
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_TIME_TOKEN_MILLIS)
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_RESTORE_STATE);

    List<Comparable[]> rows = new ArrayList<>();
    instantMetadata.getInstantsToRollback().forEach((String rolledbackInstant) -> {
        Comparable[] row = new Comparable[4];
        row[0] = instantMetadata.getStartRestoreTime();
        row[1] = rolledbackInstant;
        row[2] = instantMetadata.getTimeTakenInMillis();
        row[3] = instant.getState();
        rows.add(row);
      });

    return HoodiePrintHelper.print(header, new HashMap<>(), sortByField, descending, limit, headerOnly, rows);
  }

  @NotNull
  private List<HoodieInstant> getInstants(HoodieActiveTimeline activeTimeline, boolean showInFlight) {
    List<HoodieInstant> hoodieInstants = new ArrayList<>();
    hoodieInstants.addAll(activeTimeline.getRestoreTimeline().filterCompletedInstants().getInstants());
    if (showInFlight) {
      hoodieInstants.addAll(activeTimeline.getRestoreTimeline().filterInflights().getInstants());
    }
    return hoodieInstants;
  }

}
