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

import com.uber.hoodie.cli.HoodieCLI;
import com.uber.hoodie.cli.HoodiePrintHelper;
import com.uber.hoodie.cli.utils.InputStreamConsumer;
import com.uber.hoodie.cli.utils.SparkUtil;
import com.uber.hoodie.common.model.HoodieCommitMetadata;
import com.uber.hoodie.common.model.HoodieCommits;
import com.uber.hoodie.common.model.HoodieTableMetadata;
import com.uber.hoodie.common.model.HoodieWriteStat;
import com.uber.hoodie.common.util.NumericUtils;

import org.apache.spark.launcher.SparkLauncher;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

@Component
public class CommitsCommand implements CommandMarker {
    @CliAvailabilityIndicator({"commits show"})
    public boolean isShowAvailable() {
        return HoodieCLI.tableMetadata != null;
    }

    @CliAvailabilityIndicator({"commits refresh"})
    public boolean isRefreshAvailable() {
        return HoodieCLI.tableMetadata != null;
    }

    @CliAvailabilityIndicator({"commit rollback"})
    public boolean isRollbackAvailable() {
        return HoodieCLI.tableMetadata != null;
    }

    @CliAvailabilityIndicator({"commit show"})
    public boolean isCommitShowAvailable() {
        return HoodieCLI.tableMetadata != null;
    }

    @CliCommand(value = "commits show", help = "Show the commits")
    public String showCommits(
        @CliOption(key = {
            "limit"}, mandatory = false, help = "Limit commits", unspecifiedDefaultValue = "10")
        final Integer limit) throws IOException {
        SortedMap<String, HoodieCommitMetadata> map =
            HoodieCLI.tableMetadata.getAllCommitMetadata();
        int arraySize =
            Math.min(limit, HoodieCLI.tableMetadata.getAllCommits().getCommitList().size());
        String[][] rows = new String[arraySize][];
        ArrayList<String> commitList =
            new ArrayList<String>(HoodieCLI.tableMetadata.getAllCommits().getCommitList());
        Collections.reverse(commitList);
        for (int i = 0; i < arraySize; i++) {
            String commit = commitList.get(i);
            HoodieCommitMetadata commitMetadata = map.get(commit);
            rows[i] = new String[] {commit,
                NumericUtils.humanReadableByteCount(commitMetadata.fetchTotalBytesWritten()),
                String.valueOf(commitMetadata.fetchTotalFilesInsert()),
                String.valueOf(commitMetadata.fetchTotalFilesUpdated()),
                String.valueOf(commitMetadata.fetchTotalPartitionsWritten()),
                String.valueOf(commitMetadata.fetchTotalRecordsWritten()),
                String.valueOf(commitMetadata.fetchTotalUpdateRecordsWritten()),
                String.valueOf(commitMetadata.fetchTotalWriteErrors())};
        }
        return HoodiePrintHelper.print(
            new String[] {"CommitTime", "Total Written (B)", "Total Files Added",
                "Total Files Updated", "Total Partitions Written", "Total Records Written",
                "Total Update Records Written", "Total Errors"}, rows);
    }

    @CliCommand(value = "commits refresh", help = "Refresh the commits")
    public String refreshCommits() throws IOException {
        HoodieTableMetadata metadata =
            new HoodieTableMetadata(HoodieCLI.fs, HoodieCLI.tableMetadata.getBasePath());
        HoodieCLI.setTableMetadata(metadata);
        return "Metadata for table " + metadata.getTableName() + " refreshed.";
    }

    @CliCommand(value = "commit rollback", help = "Rollback a commit")
    public String rollbackCommit(
        @CliOption(key = {"commit"}, help = "Commit to rollback")
        final String commitTime,
        @CliOption(key = {"sparkProperties"}, help = "Spark Properites File Path")
        final String sparkPropertiesPath) throws Exception {
        if (!HoodieCLI.tableMetadata.getAllCommits().contains(commitTime)) {
            return "Commit " + commitTime + " not found in Commits " + HoodieCLI.tableMetadata
                .getAllCommits();
        }
        SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);
        sparkLauncher.addAppArgs(SparkMain.SparkCommand.ROLLBACK.toString(),
                commitTime,
                HoodieCLI.tableMetadata.getBasePath());
        Process process = sparkLauncher.launch();
        InputStreamConsumer.captureOutput(process);
        int exitCode = process.waitFor();
        // Refresh the current
        refreshCommits();
        if (exitCode != 0) {
            return "Commit " + commitTime + " failed to roll back";
        }
        return "Commit " + commitTime + " rolled back";
    }

    @CliCommand(value = "commit showpartitions", help = "Show partition level details of a commit")
    public String showCommitPartitions(
        @CliOption(key = {"commit"}, help = "Commit to show")
        final String commitTime) throws Exception {
        if (!HoodieCLI.tableMetadata.getAllCommits().contains(commitTime)) {
            return "Commit " + commitTime + " not found in Commits " + HoodieCLI.tableMetadata
                .getAllCommits();
        }
        HoodieCommitMetadata meta = HoodieCLI.tableMetadata.getAllCommitMetadata().get(commitTime);
        List<String[]> rows = new ArrayList<String[]>();
        for (Map.Entry<String, List<HoodieWriteStat>> entry : meta.getPartitionToWriteStats()
            .entrySet()) {
            String path = entry.getKey();
            List<HoodieWriteStat> stats = entry.getValue();
            long totalFilesAdded = 0;
            long totalFilesUpdated = 0;
            long totalRecordsUpdated = 0;
            long totalRecordsInserted = 0;
            long totalBytesWritten = 0;
            long totalWriteErrors = 0;
            for (HoodieWriteStat stat : stats) {
                if (stat.getPrevCommit().equals(HoodieWriteStat.NULL_COMMIT)) {
                    totalFilesAdded += 1;
                    totalRecordsInserted += stat.getNumWrites();
                } else {
                    totalFilesUpdated += 1;
                    totalRecordsUpdated += stat.getNumUpdateWrites();
                }
                totalBytesWritten += stat.getTotalWriteBytes();
                totalWriteErrors += stat.getTotalWriteErrors();
            }
            rows.add(new String[] {path, String.valueOf(totalFilesAdded),
                String.valueOf(totalFilesUpdated), String.valueOf(totalRecordsInserted),
                String.valueOf(totalRecordsUpdated),
                NumericUtils.humanReadableByteCount(totalBytesWritten),
                String.valueOf(totalWriteErrors)});

        }
        return HoodiePrintHelper.print(
            new String[] {"Partition Path", "Total Files Added", "Total Files Updated",
                "Total Records Inserted", "Total Records Updated", "Total Bytes Written",
                "Total Errors"}, rows.toArray(new String[rows.size()][]));
    }

    @CliCommand(value = "commit showfiles", help = "Show file level details of a commit")
    public String showCommitFiles(
        @CliOption(key = {"commit"}, help = "Commit to show")
        final String commitTime) throws Exception {
        if (!HoodieCLI.tableMetadata.getAllCommits().contains(commitTime)) {
            return "Commit " + commitTime + " not found in Commits " + HoodieCLI.tableMetadata
                .getAllCommits();
        }
        HoodieCommitMetadata meta = HoodieCLI.tableMetadata.getAllCommitMetadata().get(commitTime);
        List<String[]> rows = new ArrayList<String[]>();
        for (Map.Entry<String, List<HoodieWriteStat>> entry : meta.getPartitionToWriteStats()
            .entrySet()) {
            String path = entry.getKey();
            List<HoodieWriteStat> stats = entry.getValue();
            for (HoodieWriteStat stat : stats) {
                rows.add(new String[] {path, stat.getFileId(), stat.getPrevCommit(),
                    String.valueOf(stat.getNumUpdateWrites()), String.valueOf(stat.getNumWrites()),
                    String.valueOf(stat.getTotalWriteBytes()),
                    String.valueOf(stat.getTotalWriteErrors())});
            }
        }
        return HoodiePrintHelper.print(
            new String[] {"Partition Path", "File ID", "Previous Commit", "Total Records Updated",
                "Total Records Written", "Total Bytes Written", "Total Errors"},
            rows.toArray(new String[rows.size()][]));
    }

    @CliAvailabilityIndicator({"commits compare"})
    public boolean isCompareCommitsAvailable() {
        return HoodieCLI.tableMetadata != null;
    }

    @CliCommand(value = "commits compare", help = "Compare commits with another Hoodie dataset")
    public String compareCommits(
        @CliOption(key = {"path"}, help = "Path of the dataset to compare to")
        final String path) throws Exception {
        HoodieTableMetadata target = new HoodieTableMetadata(HoodieCLI.fs, path);
        HoodieTableMetadata source = HoodieCLI.tableMetadata;
        String targetLatestCommit =
            target.isCommitsEmpty() ? "0" : target.getAllCommits().lastCommit();
        String sourceLatestCommit =
            source.isCommitsEmpty() ? "0" : source.getAllCommits().lastCommit();

        if (sourceLatestCommit != null && HoodieCommits
            .isCommit1After(targetLatestCommit, sourceLatestCommit)) {
            // source is behind the target
            List<String> commitsToCatchup = target.findCommitsSinceTs(sourceLatestCommit);
            return "Source " + source.getTableName() + " is behind by " + commitsToCatchup.size()
                + " commits. Commits to catch up - " + commitsToCatchup;
        } else {
            List<String> commitsToCatchup = source.findCommitsSinceTs(targetLatestCommit);
            return "Source " + source.getTableName() + " is ahead by " + commitsToCatchup.size()
                + " commits. Commits to catch up - " + commitsToCatchup;
        }
    }

    @CliAvailabilityIndicator({"commits sync"})
    public boolean isSyncCommitsAvailable() {
        return HoodieCLI.tableMetadata != null;
    }

    @CliCommand(value = "commits sync", help = "Compare commits with another Hoodie dataset")
    public String syncCommits(
        @CliOption(key = {"path"}, help = "Path of the dataset to compare to")
        final String path) throws Exception {
        HoodieCLI.syncTableMetadata = new HoodieTableMetadata(HoodieCLI.fs, path);
        HoodieCLI.state = HoodieCLI.CLIState.SYNC;
        return "Load sync state between " + HoodieCLI.tableMetadata.getTableName() + " and "
            + HoodieCLI.syncTableMetadata.getTableName();
    }

}
