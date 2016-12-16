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
import com.uber.hoodie.cli.utils.InputStreamConsumer;
import com.uber.hoodie.cli.utils.SparkUtil;
import org.apache.spark.launcher.SparkLauncher;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

@Component
public class RecordsCommand implements CommandMarker {

    @CliAvailabilityIndicator({"records deduplicate"})
    public boolean isRecordsDeduplicateAvailable() {
        return HoodieCLI.tableMetadata != null;
    }

    @CliCommand(value = "records deduplicate", help = "De-duplicate a partition path contains duplicates & produce repaired files to replace with")
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

//    @CliCommand(value = "records find", help = "Find Records in a hoodie dataset")
//    public String findRecords(
//        @CliOption(key = {"keys"}, help = "Keys To Find (Comma seperated)")
//        final String hoodieKeys,
//        @CliOption(key = {"sparkProperties"}, help = "Spark Properites File Path")
//        final String sparkPropertiesPath) throws Exception {
//        SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);
//        sparkLauncher
//            .addAppArgs(SparkMain.RECORD_FIND, hoodieKeys, HoodieCLI.tableMetadata.getBasePath());
//        Process process = sparkLauncher.launch();
//        InputStreamConsumer.captureOutput(process);
//        int exitCode = process.waitFor();
//
//        if (exitCode != 0) {
//            return "Deduplicated files placed in:  " + repairedOutputPath;
//        }
//        return "Deduplication failed ";
//    }
}
