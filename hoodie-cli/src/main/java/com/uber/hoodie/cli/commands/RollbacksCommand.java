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

package com.uber.hoodie.cli.commands;

import static com.uber.hoodie.common.table.HoodieTimeline.ROLLBACK_ACTION;

import com.google.common.collect.ImmutableSet;
import com.uber.hoodie.avro.model.HoodieRollbackMetadata;
import com.uber.hoodie.cli.HoodieCLI;
import com.uber.hoodie.cli.HoodiePrintHelper;
import com.uber.hoodie.cli.TableHeader;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.timeline.HoodieActiveTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.table.timeline.HoodieInstant.State;
import com.uber.hoodie.common.util.AvroUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

@Component
public class RollbacksCommand implements CommandMarker {

  @CliCommand(value = "show rollbacks", help = "List all rollback instants")
  public String showRollbacks(
      @CliOption(key = {"limit"}, help = "Limit #rows to be displayed", unspecifiedDefaultValue = "10") Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") final String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") final boolean descending,
      @CliOption(key = {
          "headeronly"}, help = "Print Header Only", unspecifiedDefaultValue = "false") final boolean headerOnly)
      throws IOException {
    HoodieActiveTimeline activeTimeline = new RollbackTimeline(HoodieCLI.tableMetadata);
    HoodieTimeline rollback = activeTimeline.getRollbackTimeline().filterCompletedInstants();

    final List<Comparable[]> rows = new ArrayList<>();
    rollback.getInstants().forEach(instant -> {
      try {
        HoodieRollbackMetadata metadata = AvroUtils.deserializeAvroMetadata(
            activeTimeline.getInstantDetails(instant).get(), HoodieRollbackMetadata.class);
        metadata.getCommitsRollback().forEach(c -> {
          Comparable[] row = new Comparable[5];
          row[0] = metadata.getStartRollbackTime();
          row[1] = c;
          row[2] = metadata.getTotalFilesDeleted();
          row[3] = metadata.getTimeTakenInMillis();
          row[4] = metadata.getPartitionMetadata() != null ? metadata.getPartitionMetadata().size() : 0;
          rows.add(row);
        });
      } catch (IOException e) {
        e.printStackTrace();
      }
    });
    TableHeader header = new TableHeader()
        .addTableHeaderField("Instant")
        .addTableHeaderField("Rolledback Instant")
        .addTableHeaderField("Total Files Deleted")
        .addTableHeaderField("Time taken in millis")
        .addTableHeaderField("Total Partitions");
    return HoodiePrintHelper.print(header, new HashMap<>(), sortByField, descending, limit, headerOnly, rows);
  }

  @CliCommand(value = "show rollback", help = "Show details of a rollback instant")
  public String showRollback(
      @CliOption(key = {"instant"}, help = "Rollback instant", mandatory = true) String rollbackInstant,
      @CliOption(key = {"limit"}, help = "Limit  #rows to be displayed", unspecifiedDefaultValue = "10") Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") final String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") final boolean descending,
      @CliOption(key = {
          "headeronly"}, help = "Print Header Only", unspecifiedDefaultValue = "false") final boolean headerOnly)
      throws IOException {
    HoodieActiveTimeline activeTimeline = new RollbackTimeline(HoodieCLI.tableMetadata);
    final List<Comparable[]> rows = new ArrayList<>();
    HoodieRollbackMetadata metadata = AvroUtils.deserializeAvroMetadata(
        activeTimeline.getInstantDetails(new HoodieInstant(State.COMPLETED, ROLLBACK_ACTION, rollbackInstant))
            .get(), HoodieRollbackMetadata.class);
    metadata.getPartitionMetadata().entrySet().forEach(e -> {
      Stream.concat(e.getValue().getSuccessDeleteFiles().stream().map(f -> Pair.of(f, true)),
          e.getValue().getFailedDeleteFiles().stream().map(f -> Pair.of(f, false)))
          .forEach(fileWithDeleteStatus -> {
            Comparable[] row = new Comparable[5];
            row[0] = metadata.getStartRollbackTime();
            row[1] = metadata.getCommitsRollback().toString();
            row[2] = e.getKey();
            row[3] = fileWithDeleteStatus.getLeft();
            row[4] = fileWithDeleteStatus.getRight();
            rows.add(row);
          });
    });

    TableHeader header = new TableHeader()
        .addTableHeaderField("Instant")
        .addTableHeaderField("Rolledback Instants")
        .addTableHeaderField("Partition")
        .addTableHeaderField("Deleted File")
        .addTableHeaderField("Succeeded");
    return HoodiePrintHelper.print(header, new HashMap<>(), sortByField, descending, limit, headerOnly, rows);
  }

  /**
   * An Active timeline containing only rollbacks
   */
  class RollbackTimeline extends HoodieActiveTimeline {

    public RollbackTimeline(HoodieTableMetaClient metaClient) {
      super(metaClient, ImmutableSet.<String>builder().add(ROLLBACK_EXTENSION).build());
    }
  }
}
