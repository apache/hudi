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

package com.uber.hoodie.common;

import com.google.common.base.Optional;
import com.uber.hoodie.HoodieReadClient;
import com.uber.hoodie.common.model.HoodieCommitMetadata;
import com.uber.hoodie.common.model.HoodieDataFile;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.TableFileSystemView;
import com.uber.hoodie.common.table.log.HoodieCompactedLogRecordScanner;
import com.uber.hoodie.common.table.log.HoodieLogFile;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.ParquetUtils;
import com.uber.hoodie.config.HoodieCompactionConfig;
import com.uber.hoodie.config.HoodieIndexConfig;
import com.uber.hoodie.config.HoodieStorageConfig;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.HoodieException;
import com.uber.hoodie.exception.HoodieIOException;
import com.uber.hoodie.hadoop.realtime.HoodieRealtimeFileSplit;
import com.uber.hoodie.hadoop.realtime.HoodieRealtimeRecordReader;
import com.uber.hoodie.index.HoodieIndex;
import com.uber.hoodie.table.HoodieTable;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

//Test Util to workaround HoodieReadClient for MergeOnRead TableType
//NOTE : The implementation is crude at the moment and needs iterations
public class HoodieMergeOnReadClientTestUtil extends HoodieReadClient {

    private transient final JavaSparkContext jsc;
    private transient final FileSystem fs;
    private final HoodieTimeline commitTimeline;
    private HoodieTable hoodieTable;
    private transient com.google.common.base.Optional<SQLContext> sqlContextOpt;
    private String basePath;

    public HoodieMergeOnReadClientTestUtil(JavaSparkContext jsc, String basePath, SQLContext sqlContext) {
        super(jsc, basePath);
        this.jsc = jsc;
        this.fs = FSUtils.getFs();
        this.hoodieTable = HoodieTable
                .getHoodieTable(new HoodieTableMetaClient(fs, basePath, true), null);
        this.commitTimeline = hoodieTable.getCompletedCommitTimeline();
        this.basePath = basePath;
        this.sqlContextOpt = Optional.of(sqlContext);
    }

    @Override
    public boolean hasNewCommits(String commitTimestamp) {
        return listCommitsSince(commitTimestamp).size() > 0;
    }

    @Override
    public List<String> listCommitsSince(String commitTimestamp) {
        return commitTimeline.findInstantsAfter(commitTimestamp, Integer.MAX_VALUE).getInstants()
                .map(HoodieInstant::getTimestamp).collect(Collectors.toList());
    }

    @Override
    public Dataset<Row> readCommit(String commitTime) {
        String actionType = hoodieTable.getCommitActionType();
        HoodieInstant commitInstant =
                new HoodieInstant(false, actionType, commitTime);
        if (!commitTimeline.containsInstant(commitInstant)) {
            new HoodieException("No commit exists at " + commitTime);
        }
        try {
            HoodieCommitMetadata commitMetdata =
                    HoodieCommitMetadata.fromBytes(commitTimeline.getInstantDetails(commitInstant).get());
            Collection<String> partitions = commitMetdata.getPartitionToWriteStats().keySet();
            List<String> commits = new ArrayList<>();
            partitions.stream().forEach(partition -> {
                try {
                    commits.addAll(readAndGetCommits(fs, generateSplit(this.basePath, partition)));
                } catch (IOException e) {
                    new HoodieException(e);
                }
            });
            return convertToDF(commits);
        } catch (Exception e) {
            throw new HoodieException("Error reading commit " + commitTime, e);
        }
    }

    @Override
    public Dataset<Row> read(String... paths) {
        List<String> commits = new ArrayList<>();
        try {
            for (String path : paths) {
                if (!path.contains(hoodieTable.getMetaClient().getBasePath())) {
                    throw new HoodieException("Path " + path
                            + " does not seem to be a part of a Hoodie dataset at base path "
                            + hoodieTable.getMetaClient().getBasePath());
                }
                //TODO : find a way to list partitions only
                String partition = path.substring(hoodieTable.getMetaClient().getBasePath().length() + 1, path.length() - 2);
                commits.addAll(readAndGetCommits(fs, generateSplit(this.basePath, partition)));
            }
            return convertToDF(commits);
        } catch (Exception e) {
            throw new HoodieException("Error reading hoodie dataset as a dataframe", e);
        }
    }

    @Override
    public Dataset<Row> readSince(String lastCommitTimestamp) {

        List<HoodieInstant> commitsToReturn =
                commitTimeline.findInstantsAfter(lastCommitTimestamp, Integer.MAX_VALUE)
                        .getInstants().collect(Collectors.toList());
        List<String> commits = new ArrayList<String>();
        try {
            // Go over the commit metadata, and obtain the new files that need to be read.
            HashMap<String, String> fileIdToFullPath = new HashMap<>();
            for (HoodieInstant commit : commitsToReturn) {
                HoodieCommitMetadata metadata =
                        HoodieCommitMetadata.fromBytes(commitTimeline.getInstantDetails(commit).get());
                // get files from each commit, and replace any previous versions
                fileIdToFullPath.putAll(metadata.getFileIdAndFullPaths());
            }
            HoodieCompactedLogRecordScanner compactedLogRecordScanner = new HoodieCompactedLogRecordScanner(FSUtils.getFs(),
                    fileIdToFullPath.values().stream().collect(Collectors.toList()), augmentSchema(new Path(this.basePath)));

            while(compactedLogRecordScanner.iterator().hasNext()) {
                commits.add(compactedLogRecordScanner.iterator().next().getData()
                        .getInsertValue(augmentSchema(new Path(this.basePath))).get().get(0).toString());
            }
            return convertToDF(commits);
        } catch (IOException e) {
            throw new HoodieException("Error pulling data incrementally from commitTimestamp :" + lastCommitTimestamp, e);
        }
    }

    @Override
    public String latestCommit() { return commitTimeline.lastInstant().get().getTimestamp(); }

    private HoodieWriteConfig getConfig(String basePath) {
        return getConfigBuilder(basePath).build();
    }

    private HoodieWriteConfig.Builder getConfigBuilder(String basePath) {
        return HoodieWriteConfig.newBuilder().withPath(basePath)
                .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
                .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024).build())
                .withStorageConfig(HoodieStorageConfig.newBuilder().limitFileSize(1024 * 1024).build())
                .forTable("test-trip-table").withIndexConfig(
                        HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build());
    }

    //TODO : better way to read schema on file
    private Schema augmentSchema(Path partitionPath) {
        Schema writerSchema = new AvroSchemaConverter()
                .convert(ParquetUtils.readSchema(partitionPath));
        return writerSchema;
    }

    private List<HoodieRealtimeFileSplit> generateSplit(String basePath, String partition) {
        HoodieTableMetaClient metaClient = new HoodieTableMetaClient(fs, basePath);
        HoodieTable table = HoodieTable.getHoodieTable(metaClient, getConfig(basePath));
        final TableFileSystemView fsView = table.getFileSystemView();

        List<HoodieRealtimeFileSplit> rtSplits = new ArrayList<>();
        try {
            Map<HoodieDataFile, List<HoodieLogFile>> dataLogFileGrouping = fsView.groupLatestDataFileWithLogFiles(partition);
            String commitTime = metaClient.getActiveTimeline().getDeltaCommitTimeline().filterCompletedInstants().lastInstant().get().getTimestamp();
            if(dataLogFileGrouping.size() == 0) {
                Iterator<HoodieDataFile> itr = fsView.getLatestVersionInPartition(partition, commitTime).iterator();
                while(itr.hasNext()) {
                    rtSplits.add(new HoodieRealtimeFileSplit(new FileSplit(new Path(itr.next().getPath()),0,1,new JobConf()), Collections.EMPTY_LIST, commitTime));
                }
            } else {
                dataLogFileGrouping.forEach((dataFile, logFiles) -> {
                    try {
                        List<String> logFilePaths = logFiles.stream().map(logFile -> logFile.getPath().toString()).collect(Collectors.toList());
                        String maxCommitTime = metaClient.getActiveTimeline().getDeltaCommitTimeline().filterCompletedInstants().lastInstant().get().getTimestamp();
                        rtSplits.add(new HoodieRealtimeFileSplit(new FileSplit(new Path(dataFile.getPath()), 0, 1, new JobConf()), logFilePaths, maxCommitTime));
                    } catch (IOException e) {
                        throw new HoodieIOException("Error creating hoodie real time split ", e);
                    }
                });
            }
        } catch (IOException e) {
            throw new HoodieIOException("Error obtaining data file/log file grouping: " + e);
        }
        return rtSplits;
    }

    private List<String> readAndGetCommits(FileSystem fs, List<HoodieRealtimeFileSplit> rtSplits) throws IOException {
        List<String> commits = new ArrayList<String>();
        rtSplits.stream().forEach(split -> {
            JobConf jobConf = new JobConf();
            List<Schema.Field> fields = augmentSchema(split.getPath()).getFields();
            String names = fields.stream().map(f -> f.name().toString()).collect(Collectors.joining(","));
            String postions = fields.stream().map(f -> String.valueOf(f.pos())).collect(Collectors.joining(","));
            jobConf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, names);
            jobConf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, postions);
            jobConf.set("partition_columns", "datestr");
            try {
                RecordReader<Void, ArrayWritable> reader =
                        new MapredParquetInputFormat().
                                getRecordReader(new FileSplit(split.getPath(), 0, fs.getLength(split.getPath()), (String[]) null),
                                        new JobConf(), null);
                HoodieRealtimeRecordReader recordReader = new HoodieRealtimeRecordReader(split, jobConf, reader);
                Void key = recordReader.createKey();
                ArrayWritable writable = recordReader.createValue();
                int totalCount = 0;
                //TODO : convert writable values to a full Row, not just commits
                while(recordReader.next(key, writable)) {
                    // writable returns an array with [field1, field2, _hoodie_commit_time, _hoodie_commit_seqno]
                    // Take the commit time and compare with the one we are interested in
                    Writable[] values = writable.get();
                    commits.add(values[0].toString()); //Since commitTime is the first index
                    totalCount++;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        return commits;
    }

    private Dataset<Row> convertToDF(List<String> commits) {
        return sqlContextOpt.get().jsonRDD(jsc.parallelize(commits));
    }
}
