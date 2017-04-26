/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.cli.commands;

import com.uber.hoodie.cli.utils.CommitUtil;
import com.uber.hoodie.cli.utils.HiveUtil;
import com.uber.hoodie.cli.HoodieCLI;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Component
public class HoodieSyncCommand implements CommandMarker {
    @CliAvailabilityIndicator({"sync validate"})
    public boolean isSyncVerificationAvailable() {
        return HoodieCLI.tableMetadata != null && HoodieCLI.syncTableMetadata != null;
    }

    @CliCommand(value = "sync validate", help = "Validate the sync by counting the number of records")
    public String validateSync(
        @CliOption(key = {"mode"}, unspecifiedDefaultValue = "complete", help = "Check mode")
        final String mode,
        @CliOption(key = {
            "sourceDb"}, unspecifiedDefaultValue = "rawdata", help = "source database")
        final String srcDb,
        @CliOption(key = {
            "targetDb"}, unspecifiedDefaultValue = "dwh_hoodie", help = "target database")
        final String tgtDb,
        @CliOption(key = {
            "partitionCount"}, unspecifiedDefaultValue = "5", help = "total number of recent partitions to validate")
        final int partitionCount,
        @CliOption(key = {
            "hiveServerUrl"}, mandatory = true, help = "hiveServerURL to connect to")
        final String hiveServerUrl,
        @CliOption(key = {
            "hiveUser"}, mandatory = false, unspecifiedDefaultValue = "", help = "hive username to connect to")
        final String hiveUser,
        @CliOption(key = {
            "hivePass"}, mandatory = true, unspecifiedDefaultValue = "", help = "hive password to connect to")
        final String hivePass) throws Exception {
        HoodieTableMetaClient target = HoodieCLI.syncTableMetadata;
        HoodieTimeline targetTimeline = target.getActiveTimeline().getCommitTimeline();
        HoodieTableMetaClient source = HoodieCLI.tableMetadata;
        HoodieTimeline sourceTimeline = source.getActiveTimeline().getCommitTimeline();
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
            targetTimeline.getInstants().iterator().hasNext() ? "0" : targetTimeline.lastInstant().get().getTimestamp();
        String sourceLatestCommit =
            sourceTimeline.getInstants().iterator().hasNext() ? "0" : sourceTimeline.lastInstant().get().getTimestamp();

        if (sourceLatestCommit != null && HoodieTimeline
            .compareTimestamps(targetLatestCommit, sourceLatestCommit, HoodieTimeline.GREATER)) {
            // source is behind the target
            List<HoodieInstant> commitsToCatchup =
                targetTimeline.findInstantsAfter(sourceLatestCommit, Integer.MAX_VALUE).getInstants()
                    .collect(Collectors.toList());
            if (commitsToCatchup.isEmpty()) {
                return "Count difference now is (count(" + target.getTableConfig().getTableName()
                    + ") - count(" + source.getTableConfig().getTableName() + ") == " + (targetCount
                    - sourceCount);
            } else {
                long newInserts = CommitUtil.countNewRecords(target,
                    commitsToCatchup.stream().map(HoodieInstant::getTimestamp)
                        .collect(Collectors.toList()));
                return "Count difference now is (count(" + target.getTableConfig().getTableName()
                    + ") - count(" + source.getTableConfig().getTableName() + ") == " + (targetCount
                    - sourceCount) + ". Catch up count is " + newInserts;
            }
        } else {
            List<HoodieInstant> commitsToCatchup =
                sourceTimeline.findInstantsAfter(targetLatestCommit, Integer.MAX_VALUE).getInstants()
                    .collect(Collectors.toList());
            if (commitsToCatchup.isEmpty()) {
                return "Count difference now is (count(" + source.getTableConfig().getTableName()
                    + ") - count(" + target.getTableConfig().getTableName() + ") == " + (sourceCount
                    - targetCount);
            } else {
                long newInserts = CommitUtil.countNewRecords(source,
                    commitsToCatchup.stream().map(HoodieInstant::getTimestamp)
                        .collect(Collectors.toList()));
                return "Count difference now is (count(" + source.getTableConfig().getTableName()
                    + ") - count(" + target.getTableConfig().getTableName() + ") == " + (sourceCount
                    - targetCount) + ". Catch up count is " + newInserts;
            }

        }
    }

}
