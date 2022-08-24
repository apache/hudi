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

import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.utils.HiveUtil;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.exception.HoodieException;

import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.cli.utils.TimelineUtil.countNewRecords;

/**
 * CLI command to display sync options.
 */
@Component
public class HoodieSyncValidateCommand implements CommandMarker {

  @CliCommand(value = "sync validate", help = "Validate the sync by counting the number of records")
  public String validateSync(
      @CliOption(key = {"mode"}, unspecifiedDefaultValue = "complete", help = "Check mode") final String mode,
      @CliOption(key = {"sourceDb"}, unspecifiedDefaultValue = "rawdata", help = "source database") final String srcDb,
      @CliOption(key = {"targetDb"}, unspecifiedDefaultValue = "dwh_hoodie",
          help = "target database") final String tgtDb,
      @CliOption(key = {"partitionCount"}, unspecifiedDefaultValue = "5",
          help = "total number of recent partitions to validate") final int partitionCount,
      @CliOption(key = {"hiveServerUrl"}, mandatory = true,
          help = "hiveServerURL to connect to") final String hiveServerUrl,
      @CliOption(key = {"hiveUser"}, unspecifiedDefaultValue = "",
          help = "hive username to connect to") final String hiveUser,
      @CliOption(key = {"hivePass"}, mandatory = true, unspecifiedDefaultValue = "",
          help = "hive password to connect to") final String hivePass)
      throws Exception {
    if (HoodieCLI.syncTableMetadata == null) {
      throw new HoodieException("Sync validate request target table not null.");
    }
    HoodieTableMetaClient target = HoodieCLI.syncTableMetadata;
    HoodieTimeline targetTimeline = target.getActiveTimeline().getCommitsTimeline();
    HoodieTableMetaClient source = HoodieCLI.getTableMetaClient();
    HoodieTimeline sourceTimeline = source.getActiveTimeline().getCommitsTimeline();
    long sourceCount = 0;
    long targetCount = 0;
    if ("complete".equals(mode)) {
      sourceCount = HiveUtil.countRecords(hiveServerUrl, source, srcDb, hiveUser, hivePass);
      targetCount = HiveUtil.countRecords(hiveServerUrl, target, tgtDb, hiveUser, hivePass);
    } else if ("latestPartitions".equals(mode)) {
      sourceCount = HiveUtil.countRecords(hiveServerUrl, source, srcDb, partitionCount, hiveUser, hivePass);
      targetCount = HiveUtil.countRecords(hiveServerUrl, target, tgtDb, partitionCount, hiveUser, hivePass);
    }

    String targetLatestCommit =
        targetTimeline.getInstants().iterator().hasNext() ? targetTimeline.lastInstant().get().getTimestamp() : "0";
    String sourceLatestCommit =
        sourceTimeline.getInstants().iterator().hasNext() ? sourceTimeline.lastInstant().get().getTimestamp() : "0";

    if (sourceLatestCommit != null
        && HoodieTimeline.compareTimestamps(targetLatestCommit, HoodieTimeline.GREATER_THAN, sourceLatestCommit)) {
      // source is behind the target
      return getString(target, targetTimeline, source, sourceCount, targetCount, sourceLatestCommit);
    } else {
      return getString(source, sourceTimeline, target, targetCount, sourceCount, targetLatestCommit);

    }
  }

  private String getString(HoodieTableMetaClient target, HoodieTimeline targetTimeline, HoodieTableMetaClient source, long sourceCount, long targetCount, String sourceLatestCommit)
      throws IOException {
    List<HoodieInstant> commitsToCatchup = targetTimeline.findInstantsAfter(sourceLatestCommit, Integer.MAX_VALUE)
        .getInstants().collect(Collectors.toList());
    if (commitsToCatchup.isEmpty()) {
      return "Count difference now is (count(" + target.getTableConfig().getTableName() + ") - count("
          + source.getTableConfig().getTableName() + ") == " + (targetCount - sourceCount);
    } else {
      long newInserts = countNewRecords(target,
          commitsToCatchup.stream().map(HoodieInstant::getTimestamp).collect(Collectors.toList()));
      return "Count difference now is (count(" + target.getTableConfig().getTableName() + ") - count("
          + source.getTableConfig().getTableName() + ") == " + (targetCount - sourceCount) + ". Catch up count is "
          + newInserts;
    }
  }

}
