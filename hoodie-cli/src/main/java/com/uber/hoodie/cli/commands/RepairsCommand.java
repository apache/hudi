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
import com.uber.hoodie.common.model.HoodiePartitionMetadata;
import com.uber.hoodie.common.util.FSUtils;

import org.apache.hadoop.fs.Path;
import org.apache.spark.launcher.SparkLauncher;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;

@Component
public class RepairsCommand implements CommandMarker {

    @CliAvailabilityIndicator({"repair deduplicate"})
    public boolean isRepairDeduplicateAvailable() {
        return HoodieCLI.tableMetadata != null;
    }

    @CliAvailabilityIndicator({"repair addpartitionmeta"})
    public boolean isRepairAddPartitionMetaAvailable() {
        return HoodieCLI.tableMetadata != null;
    }

    @CliCommand(value = "repair deduplicate", help = "De-duplicate a partition path contains duplicates & produce repaired files to replace with")
    public String deduplicate(
        @CliOption(key = {
            "duplicatedPartitionPath"}, help = "Partition Path containing the duplicates")
        final String duplicatedPartitionPath,
        @CliOption(key = {"repairedOutputPath"}, help = "Location to place the repaired files")
        final String repairedOutputPath,
        @CliOption(key = {"sparkProperties"}, help = "Spark Properites File Path")
        final String sparkPropertiesPath) throws Exception {
        SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);
        sparkLauncher
            .addAppArgs(SparkMain.SparkCommand.DEDUPLICATE.toString(), duplicatedPartitionPath,
                repairedOutputPath, HoodieCLI.tableMetadata.getBasePath());
        Process process = sparkLauncher.launch();
        InputStreamConsumer.captureOutput(process);
        int exitCode = process.waitFor();

        if (exitCode != 0) {
            return "Deduplicated files placed in:  " + repairedOutputPath;
        }
        return "Deduplication failed ";
    }



    @CliCommand(value = "repair addpartitionmeta", help = "Add partition metadata to a dataset, if not present")
    public String addPartitionMeta(
            @CliOption(key = {"dryrun"},
                    help = "Should we actually add or just print what would be done",
                    unspecifiedDefaultValue = "true")
            final boolean dryRun) throws IOException {

        String latestCommit  = HoodieCLI.tableMetadata.getAllCommits().lastCommit();
        List<String> partitionPaths = FSUtils.getAllPartitionPaths(HoodieCLI.fs,
                HoodieCLI.tableMetadata.getBasePath());
        Path basePath = new Path(HoodieCLI.tableMetadata.getBasePath());
        String[][] rows = new String[partitionPaths.size() + 1][];

        int ind = 0;
        for (String partition: partitionPaths) {
            Path partitionPath = new Path(basePath, partition);
            String[] row = new String[3];
            row[0] = partition; row[1] = "Yes"; row[2] = "None";
            if (!HoodiePartitionMetadata.hasPartitionMetadata(HoodieCLI.fs, partitionPath)) {
                row[1] = "No";
                if (!dryRun) {
                    HoodiePartitionMetadata partitionMetadata = new HoodiePartitionMetadata(
                            HoodieCLI.fs,
                            latestCommit,
                            basePath,
                            partitionPath);
                    partitionMetadata.trySave(0);
                }
            }
            rows[ind++] = row;
        }

        return HoodiePrintHelper.print(
                new String[] {"Partition Path", "Metadata Present?", "Action"}, rows);
    }
}
