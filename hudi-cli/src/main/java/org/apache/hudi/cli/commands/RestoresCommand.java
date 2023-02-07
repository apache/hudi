package org.apache.hudi.cli.commands;

import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.HoodiePrintHelper;
import org.apache.hudi.cli.HoodieTableHeaderFields;
import org.apache.hudi.cli.TableHeader;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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
          @ShellOption(value = {"--instant"}, help = "Rollback instant") String rollbackInstant,
          @ShellOption(value = {"--limit"}, help = "Limit  #rows to be displayed", defaultValue = "10") Integer limit,
          @ShellOption(value = {"--sortBy"}, help = "Sorting Field", defaultValue = "") final String sortByField,
          @ShellOption(value = {"--desc"}, help = "Ordering", defaultValue = "false") final boolean descending,
          @ShellOption(value = {"--headeronly"}, help = "Print Header Only",
                  defaultValue = "false") final boolean headerOnly)
          throws IOException {
    // TODO: Modify this code from RollbacksCommand appropriately

//    HoodieActiveTimeline activeTimeline = HoodieCLI.getTableMetaClient().getActiveTimeline();
//    final List<Comparable[]> rows = new ArrayList<>();
//    HoodieRollbackMetadata metadata = TimelineMetadataUtils.deserializeAvroMetadata(
//            activeTimeline.getInstantDetails(new HoodieInstant(HoodieInstant.State.COMPLETED, ROLLBACK_ACTION, rollbackInstant)).get(),
//            HoodieRollbackMetadata.class);
//    metadata.getPartitionMetadata().forEach((key, value) -> Stream
//            .concat(value.getSuccessDeleteFiles().stream().map(f -> Pair.of(f, true)),
//                    value.getFailedDeleteFiles().stream().map(f -> Pair.of(f, false)))
//            .forEach(fileWithDeleteStatus -> {
//              Comparable[] row = new Comparable[5];
//              row[0] = metadata.getStartRollbackTime();
//              row[1] = metadata.getCommitsRollback().toString();
//              row[2] = key;
//              row[3] = fileWithDeleteStatus.getLeft();
//              row[4] = fileWithDeleteStatus.getRight();
//              rows.add(row);
//            }));
//
//    TableHeader header = new TableHeader().addTableHeaderField(HoodieTableHeaderFields.HEADER_INSTANT)
//            .addTableHeaderField(HoodieTableHeaderFields.HEADER_ROLLBACK_INSTANT)
//            .addTableHeaderField(HoodieTableHeaderFields.HEADER_PARTITION)
//            .addTableHeaderField(HoodieTableHeaderFields.HEADER_DELETED_FILE)
//            .addTableHeaderField(HoodieTableHeaderFields.HEADER_SUCCEEDED);
//    return HoodiePrintHelper.print(header, new HashMap<>(), sortByField, descending, limit, headerOnly, rows);
    return null;
  }

}
