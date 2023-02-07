package org.apache.hudi.cli.commands;

import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.HoodiePrintHelper;
import org.apache.hudi.cli.HoodieTableHeaderFields;
import org.apache.hudi.cli.TableHeader;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
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
                  defaultValue = "false") final boolean headerOnly) {

    HoodieActiveTimeline activeTimeline = HoodieCLI.getTableMetaClient().getActiveTimeline();
    HoodieTimeline restores = activeTimeline.getRestoreTimeline().filterCompletedInstants();

    final List<Comparable[]> rows = new ArrayList<>();
    TableHeader header = new TableHeader()
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_INSTANT)
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_RESTORE_INSTANT)
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_TIME_TOKEN_MILLIS);

    restores.getInstants().forEach(restoreInstant -> {
        try {
          HoodieRestoreMetadata instantMetadata = TimelineMetadataUtils
                  .deserializeAvroMetadata(activeTimeline.getInstantDetails(restoreInstant).get(),
                          HoodieRestoreMetadata.class);
          instantMetadata.getInstantsToRollback().forEach((String rolledbackInstant) -> {
            Comparable[] row = new Comparable[3];
            row[0] = instantMetadata.getStartRestoreTime();
            row[1] = rolledbackInstant;
            row[2] = instantMetadata.getTimeTakenInMillis();
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
    HoodieRestoreMetadata instantMetadata = TimelineMetadataUtils.deserializeAvroMetadata(
        activeTimeline.getInstantDetails(new HoodieInstant(HoodieInstant.State.COMPLETED, RESTORE_ACTION,
                restoreInstant)).get(),
            HoodieRestoreMetadata.class);

    TableHeader header = new TableHeader()
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_INSTANT)
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_RESTORE_INSTANT)
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_TIME_TOKEN_MILLIS);

    List<Comparable[]> rows = new ArrayList<>();
    instantMetadata.getInstantsToRollback().forEach((String rolledbackInstant) -> {
        Comparable[] row = new Comparable[3];
        row[0] = instantMetadata.getStartRestoreTime();
        row[1] = rolledbackInstant;
        row[2] = instantMetadata.getTimeTakenInMillis();
        rows.add(row);
      });

    return HoodiePrintHelper.print(header, new HashMap<>(), sortByField, descending, limit, headerOnly, rows);

  }

}
