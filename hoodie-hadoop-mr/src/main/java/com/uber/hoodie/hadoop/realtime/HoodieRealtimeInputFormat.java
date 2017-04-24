/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.hadoop.realtime;

import com.google.common.base.Preconditions;

import com.uber.hoodie.common.model.HoodieDataFile;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.log.HoodieLogFile;
import com.uber.hoodie.common.table.view.HoodieTableFileSystemView;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.exception.HoodieIOException;
import com.uber.hoodie.hadoop.HoodieInputFormat;
import com.uber.hoodie.hadoop.UseFileSplitsFromInputFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Input Format, that provides a real-time view of data in a Hoodie dataset
 */
@UseFileSplitsFromInputFormat
public class HoodieRealtimeInputFormat extends HoodieInputFormat implements Configurable {

    public static final Log LOG = LogFactory.getLog(HoodieRealtimeInputFormat.class);

    // These positions have to be deterministic across all tables
    public static final int HOODIE_COMMIT_TIME_COL_POS = 0;
    public static final int HOODIE_RECORD_KEY_COL_POS = 2;

    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {

        Stream<FileSplit> fileSplits = Arrays.stream(super.getSplits(job, numSplits)).map(is -> (FileSplit) is);

        // obtain all unique parent folders for splits
        Map<Path, List<FileSplit>> partitionsToParquetSplits = fileSplits.collect(Collectors.groupingBy(split -> split.getPath().getParent()));
        // TODO(vc): Should we handle also non-hoodie splits here?
        Map<String, HoodieTableMetaClient> metaClientMap = new HashMap<>();
        Map<Path, HoodieTableMetaClient> partitionsToMetaClient = partitionsToParquetSplits.keySet().stream()
                .collect(Collectors.toMap(Function.identity(), p -> {
                    // find if we have a metaclient already for this partition.
                    Optional<String> matchingBasePath =  metaClientMap.keySet().stream()
                            .filter(basePath -> p.toString().startsWith(basePath)).findFirst();
                    if (matchingBasePath.isPresent()) {
                        return metaClientMap.get(matchingBasePath.get());
                    }

                    try {
                        HoodieTableMetaClient metaClient = getTableMetaClient(p.getFileSystem(conf), p);
                        metaClientMap.put(metaClient.getBasePath(), metaClient);
                        return metaClient;
                    } catch (IOException e) {
                        throw new HoodieIOException("Error creating hoodie meta client against : " + p, e);
                    }
                }));

        // for all unique split parents, obtain all delta files based on delta commit timeline, grouped on file id
        List<HoodieRealtimeFileSplit> rtSplits = new ArrayList<>();
        partitionsToParquetSplits.keySet().stream().forEach(partitionPath -> {
            // for each partition path obtain the data & log file groupings, then map back to inputsplits
            HoodieTableMetaClient metaClient = partitionsToMetaClient.get(partitionPath);
            HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient, metaClient.getActiveTimeline());
            String relPartitionPath = FSUtils.getRelativePartitionPath(new Path(metaClient.getBasePath()), partitionPath);

            try {
                Map<HoodieDataFile, List<HoodieLogFile>> dataLogFileGrouping = fsView.groupLatestDataFileWithLogFiles(relPartitionPath);

                // subgroup splits again by file id & match with log files.
                Map<String, List<FileSplit>> groupedInputSplits = partitionsToParquetSplits.get(partitionPath).stream()
                        .collect(Collectors.groupingBy(split -> FSUtils.getFileId(split.getPath().getName())));
                dataLogFileGrouping.forEach((dataFile, logFiles) -> {
                    List<FileSplit> dataFileSplits = groupedInputSplits.get(dataFile.getFileId());
                    dataFileSplits.forEach(split -> {
                        try {
                            List<String> logFilePaths = logFiles.stream().map(logFile -> logFile.getPath().toString()).collect(Collectors.toList());
                            String maxCommitTime = metaClient.getActiveTimeline().getDeltaCommitTimeline().filterCompletedInstants().lastInstant().get().getTimestamp();
                            rtSplits.add(new HoodieRealtimeFileSplit(split, logFilePaths, maxCommitTime));
                        } catch (IOException e) {
                            throw new HoodieIOException("Error creating hoodie real time split ", e);
                        }
                    });
                });
            } catch (IOException e) {
                throw new HoodieIOException("Error obtaining data file/log file grouping: " + partitionPath, e);
            }
        });

        return rtSplits.toArray(new InputSplit[rtSplits.size()]);
    }


    @Override
    public FileStatus[] listStatus(JobConf job) throws IOException {
        // Call the HoodieInputFormat::listStatus to obtain all latest parquet files, based on commit timeline.
        return super.listStatus(job);
    }


    private static Configuration addExtraReadColsIfNeeded(Configuration configuration) {
        String readColNames = configuration.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR);
        String readColIds = configuration.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR);

        if (!readColNames.contains(HoodieRecord.RECORD_KEY_METADATA_FIELD)) {
            configuration.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR,
                    readColNames + "," + HoodieRecord.RECORD_KEY_METADATA_FIELD);
            configuration.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR,
                    readColIds + "," + HOODIE_RECORD_KEY_COL_POS);
            LOG.info(String.format("Adding extra _hoodie_record_key column, to enable log merging cols (%s) ids (%s) ",
                    configuration.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR),
                    configuration.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR)));
        }

        if (!readColNames.contains(HoodieRecord.COMMIT_TIME_METADATA_FIELD)) {
            configuration.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR,
                    readColNames + "," + HoodieRecord.COMMIT_TIME_METADATA_FIELD);
            configuration.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR,
                    readColIds + "," + HOODIE_COMMIT_TIME_COL_POS);
            LOG.info(String.format("Adding extra _hoodie_commit_time column, to enable log merging cols (%s) ids (%s) ",
                    configuration.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR),
                    configuration.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR)));
        }

        return configuration;
    }



    @Override
    public RecordReader<Void, ArrayWritable> getRecordReader(final InputSplit split,
                                                             final JobConf job,
                                                             final Reporter reporter) throws IOException {
        LOG.info("Creating record reader with readCols :" + job.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR));
        // sanity check
        Preconditions.checkArgument(split instanceof HoodieRealtimeFileSplit,
                "HoodieRealtimeRecordReader can only work on HoodieRealtimeFileSplit");
        return new HoodieRealtimeRecordReader((HoodieRealtimeFileSplit) split, job, super.getRecordReader(split, job, reporter));
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = addExtraReadColsIfNeeded(conf);
    }

    @Override
    public Configuration getConf() {
        return addExtraReadColsIfNeeded(conf);
    }
}
