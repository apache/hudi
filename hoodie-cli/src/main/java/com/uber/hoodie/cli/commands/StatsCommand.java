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


import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformReservoir;
import com.uber.hoodie.cli.HoodieCLI;
import com.uber.hoodie.cli.HoodiePrintHelper;
import com.uber.hoodie.common.model.HoodieCommitMetadata;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.timeline.HoodieActiveTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.NumericUtils;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.stream.Collectors;

@Component
public class StatsCommand implements CommandMarker {
    @CliAvailabilityIndicator({"stats wa"})
    public boolean isWriteAmpAvailable() {
        return HoodieCLI.tableMetadata != null;
    }

    @CliCommand(value = "stats wa", help = "Write Amplification. Ratio of how many records were upserted to how many records were actually written")
    public String writeAmplificationStats() throws IOException {
        long totalRecordsUpserted = 0;
        long totalRecordsWritten = 0;

        HoodieActiveTimeline activeTimeline = HoodieCLI.tableMetadata.getActiveTimeline();
        HoodieTimeline timeline = activeTimeline.getCommitTimeline().filterCompletedInstants();

        String[][] rows = new String[new Long(timeline.countInstants()).intValue() + 1][];
        int i = 0;
        DecimalFormat df = new DecimalFormat("#.00");
        for (HoodieInstant commitTime : timeline.getInstants().collect(
            Collectors.toList())) {
            String waf = "0";
            HoodieCommitMetadata commit = HoodieCommitMetadata.fromBytes(activeTimeline.getInstantDetails(commitTime).get());
            if (commit.fetchTotalUpdateRecordsWritten() > 0) {
                waf = df.format(
                    (float) commit.fetchTotalRecordsWritten() / commit
                        .fetchTotalUpdateRecordsWritten());
            }
            rows[i++] = new String[] {commitTime.getTimestamp(),
                String.valueOf(commit.fetchTotalUpdateRecordsWritten()),
                String.valueOf(commit.fetchTotalRecordsWritten()), waf};
            totalRecordsUpserted += commit.fetchTotalUpdateRecordsWritten();
            totalRecordsWritten += commit.fetchTotalRecordsWritten();
        }
        String waf = "0";
        if (totalRecordsUpserted > 0) {
            waf = df.format((float) totalRecordsWritten / totalRecordsUpserted);
        }
        rows[i] = new String[] {"Total", String.valueOf(totalRecordsUpserted),
            String.valueOf(totalRecordsWritten), waf};
        return HoodiePrintHelper.print(
            new String[] {"CommitTime", "Total Upserted", "Total Written",
                "Write Amplifiation Factor"}, rows);

    }


    private String[] printFileSizeHistogram(String commitTime, Snapshot s) {
        return new String[]{
                commitTime,
                NumericUtils.humanReadableByteCount(s.getMin()),
                NumericUtils.humanReadableByteCount(s.getValue(0.1)),
                NumericUtils.humanReadableByteCount(s.getMedian()),
                NumericUtils.humanReadableByteCount(s.getMean()),
                NumericUtils.humanReadableByteCount(s.get95thPercentile()),
                NumericUtils.humanReadableByteCount(s.getMax()),
                String.valueOf(s.size()),
                NumericUtils.humanReadableByteCount(s.getStdDev())
        };
    }

    @CliCommand(value = "stats filesizes", help = "File Sizes. Display summary stats on sizes of files")
    public String fileSizeStats(
            @CliOption(key = {"partitionPath"}, help = "regex to select files, eg: 2016/08/02", unspecifiedDefaultValue = "*/*/*")
            final String globRegex) throws IOException {

        FileSystem fs = HoodieCLI.fs;
        String globPath = String.format("%s/%s/*",
                HoodieCLI.tableMetadata.getBasePath(),
                globRegex);
        FileStatus[] statuses = fs.globStatus(new Path(globPath));

        // max, min, #small files < 10MB, 50th, avg, 95th
        final int MAX_FILES = 1000000;
        Histogram globalHistogram = new Histogram(new UniformReservoir(MAX_FILES));
        HashMap<String, Histogram> commitHistoMap = new HashMap<String, Histogram>();
        for (FileStatus fileStatus: statuses) {
            String commitTime = FSUtils.getCommitTime(fileStatus.getPath().getName());
            long sz = fileStatus.getLen();
            if (!commitHistoMap.containsKey(commitTime)) {
             commitHistoMap.put(commitTime, new Histogram(new UniformReservoir(MAX_FILES)));
            }
            commitHistoMap.get(commitTime).update(sz);
            globalHistogram.update(sz);
        }

        String[][] rows = new String[commitHistoMap.size() + 1][];
        int ind = 0;
        for (String commitTime: commitHistoMap.keySet()) {
            Snapshot s = commitHistoMap.get(commitTime).getSnapshot();
            rows[ind++] = printFileSizeHistogram(commitTime, s);
        }
        Snapshot s = globalHistogram.getSnapshot();
        rows[ind++] = printFileSizeHistogram("ALL", s);

        return HoodiePrintHelper.print(
                new String[] {"CommitTime", "Min", "10th", "50th", "avg", "95th", "Max", "NumFiles", "StdDev"}, rows);
    }
}
