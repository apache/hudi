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
import com.uber.hoodie.common.model.HoodieWriteStat;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.TableFileSystemView;
import com.uber.hoodie.common.table.log.HoodieCompactedLogRecordScanner;
import com.uber.hoodie.common.table.log.HoodieLogFile;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.HoodieAvroUtils;
import com.uber.hoodie.common.util.ParquetUtils;
import com.uber.hoodie.config.HoodieCompactionConfig;
import com.uber.hoodie.config.HoodieIndexConfig;
import com.uber.hoodie.config.HoodieStorageConfig;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.HoodieException;
import com.uber.hoodie.exception.HoodieIOException;
import com.uber.hoodie.exception.SchemaCompatabilityException;
import com.uber.hoodie.hadoop.HoodieInputFormat;
import com.uber.hoodie.hadoop.realtime.HoodieRealtimeFileSplit;
import com.uber.hoodie.hadoop.realtime.HoodieRealtimeInputFormat;
import com.uber.hoodie.hadoop.realtime.HoodieRealtimeRecordReader;
import com.uber.hoodie.index.HoodieIndex;
import com.uber.hoodie.table.HoodieTable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.uber.hoodie.common.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.avro.TypeEnum.a;
import static org.apache.hadoop.hdfs.TestBlockStoragePolicy.conf;
import static org.codehaus.groovy.runtime.DefaultGroovyMethods.collect;

//Test Util to workaround HoodieReadClient for MergeOnRead TableType
//NOTE : The implementation is crude at the moment and needs iterations
//TODO(na) : Use HoodieRealtimeInputFormat wherever possible
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
        HoodieTableMetaClient metaClient = new HoodieTableMetaClient(fs, basePath);
        HoodieInstant commitInstant =
                new HoodieInstant(false, actionType, commitTime);
        final TableFileSystemView fsView = hoodieTable.getFileSystemView();
        if (!commitTimeline.containsInstant(commitInstant)) {
            new HoodieException("No commit exists at " + commitTime);
        }
        try {
            HoodieCommitMetadata commitMetdata =
                    HoodieCommitMetadata.fromBytes(commitTimeline.getInstantDetails(commitInstant).get());
            List<HoodieWriteStat> allStats = new ArrayList<>();
            commitMetdata.getPartitionToWriteStats().forEach((k,v) -> {
                allStats.addAll(v);
            });
            List<String> allFiles = allStats.stream()
                    .map(stat -> stat.getFullPath())
                    .collect(Collectors.toList());

            return convertToDF(getRecordsUsingInputFormat(allFiles));

        } catch (Exception e) {
            throw new HoodieException("Error reading commit " + commitTime, e);
        }
    }

    public List<GenericRecord> readCommitAndReturnRecords(String commitTime) {
        String actionType = hoodieTable.getCommitActionType();
        HoodieInstant commitInstant =
                new HoodieInstant(false, actionType, commitTime);
        if (!commitTimeline.containsInstant(commitInstant)) {
            new HoodieException("No commit exists at " + commitTime);
        }
        try {
            HoodieCommitMetadata commitMetdata =
                    HoodieCommitMetadata.fromBytes(commitTimeline.getInstantDetails(commitInstant).get());
            List<HoodieWriteStat> allStats = new ArrayList<>();
            commitMetdata.getPartitionToWriteStats().forEach((k,v) -> {
                allStats.addAll(v);
            });
            List<String> allFiles = allStats.stream()
                    .map(stat -> stat.getFullPath())
                    .collect(Collectors.toList());

            return getRecordsUsingInputFormat(allFiles);

        } catch (Exception e) {
            throw new HoodieException("Error reading commit " + commitTime, e);
        }
    }

    @Override
    public Dataset<Row> read(String... paths) {
        try {
            List<String> inputPaths = new ArrayList<>();
            for (String path : paths) {
                if (!path.contains(hoodieTable.getMetaClient().getBasePath())) {
                    throw new HoodieException("Path " + path
                            + " does not seem to be a part of a Hoodie dataset at base path "
                            + hoodieTable.getMetaClient().getBasePath());
                }
                //TODO(na) : find a better way to list partitions only
                String partition = path.substring(hoodieTable.getMetaClient().getBasePath().length() + 1, path.length() - 2);
                inputPaths.add(basePath + "/" + partition);
            }
            return convertToDF(getRecordsUsingInputFormat(inputPaths));
        } catch (Exception e) {
            throw new HoodieException("Error reading hoodie dataset as a dataframe", e);
        }
    }

    @Override
    public Dataset<Row> readSince(String lastCommitTimestamp) {

        List<HoodieInstant> commitsToReturn =
                commitTimeline.findInstantsAfter(lastCommitTimestamp, Integer.MAX_VALUE)
                        .getInstants().collect(Collectors.toList());

        Schema schema = HoodieAvroUtils.addMetadataFields(Schema.parse(TRIP_EXAMPLE_SCHEMA));
        List<GenericRecord> records = new ArrayList<GenericRecord>();
        try {
            // Go over the commit metadata, and obtain the new files that need to be read.
            HashMap<String, String> fileIdToFullPath = new HashMap<>(); //NOTE : problem with fileId for parquet and log file is the same
            List<String> fullPaths = new ArrayList<>();
            for (HoodieInstant commit : commitsToReturn) {
                HoodieCommitMetadata metadata =
                        HoodieCommitMetadata.fromBytes(commitTimeline.getInstantDetails(commit).get());
                // get files from each commit, and replace any previous versions
                fullPaths.addAll(metadata.getFileIdAndFullPaths().values());
                fileIdToFullPath.putAll(metadata.getFileIdAndFullPaths());
            }

            return convertToDF(getRecordsUsingInputFormat(fullPaths));
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
                .withSchema(TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
                .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024).build())
                .withStorageConfig(HoodieStorageConfig.newBuilder().limitFileSize(1024 * 1024).build())
                .forTable("test-trip-table").withIndexConfig(
                        HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build());
    }

    private Dataset<Row> convertToDF(List<GenericRecord> records) {
        //List<Row> rows = records.stream().map(r -> createRowFromGenericRecord(HoodieAvroUtils.addMetadataFields(Schema.parse(TRIP_EXAMPLE_SCHEMA)), r)).collect(Collectors.toList());
        return sqlContextOpt.get().createDataFrame(records,
                GenericRecord.class);
    }


    private List<GenericRecord> getRecordsUsingScanner(List<String> allFiles) {
        List<GenericRecord> records = new ArrayList<>();
        HoodieTableMetaClient metaClient = new HoodieTableMetaClient(fs, basePath);
        List<String> allLogFiles = allFiles.stream().filter(s -> s
                .contains(metaClient.getTableConfig().getRTFileFormat().getFileExtension()))
                .collect(Collectors.toList());

        List<String> allParquetFiles = allFiles.stream().filter(s -> !s
                .contains(metaClient.getTableConfig().getRTFileFormat().getFileExtension()))
                .collect(Collectors.toList());

        Schema schema = HoodieAvroUtils.addMetadataFields(Schema.parse(TRIP_EXAMPLE_SCHEMA));
        HoodieCompactedLogRecordScanner scanner =
                new HoodieCompactedLogRecordScanner(fs, allLogFiles, schema);
        scanner.iterator().forEachRemaining(d -> {
            IndexedRecord rec = null;
            try {
                rec = d.getData().getInsertValue(schema).get();
            } catch (IOException e) {
                e.printStackTrace();
            }
            records.add((GenericRecord) rec);
        });
        allParquetFiles.forEach(parquetFile -> {
            ParquetUtils.readAvroRecords(new Path(parquetFile)).forEach(record -> {
                records.add(record);
            });
        });

        return records;
    }

    private void setPropsForInputFormat(HoodieRealtimeInputFormat inputFormat, JobConf jobConf, Schema schema) {
        List<Schema.Field> fields = schema.getFields();
        String names = fields.stream().map(f -> f.name().toString()).collect(Collectors.joining(","));
        String postions = fields.stream().map(f -> String.valueOf(f.pos())).collect(Collectors.joining(","));
        Configuration conf = FSUtils.getFs().getConf();
        jobConf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, names);
        jobConf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, postions);
        jobConf.set("partition_columns", "datestr");
        conf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, names);
        conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, postions);
        conf.set("partition_columns", "datestr");
        inputFormat.setConf(conf);
        jobConf.addResource(conf);
    }

    private void setInputPath(JobConf jobConf, String inputPath) {
        jobConf.set("mapreduce.input.fileinputformat.inputdir", inputPath);
        jobConf.set("mapreduce.input.fileinputformat.inputdir", inputPath);
        jobConf.set("map.input.dir", inputPath);
    }

    private List<GenericRecord> getRecordsUsingInputFormat(List<String> inputPaths) throws IOException {
        JobConf jobConf = new JobConf();
        Schema schema = HoodieAvroUtils.addMetadataFields(Schema.parse(TRIP_EXAMPLE_SCHEMA));
        HoodieRealtimeInputFormat inputFormat = new HoodieRealtimeInputFormat();
        setPropsForInputFormat(inputFormat, jobConf, schema);
        return inputPaths.stream().map(path -> {
            setInputPath(jobConf, path);
            List<GenericRecord> records = new ArrayList<>();
            try {
                List<InputSplit> splits = Arrays.asList(inputFormat.getSplits(jobConf, 1));
                RecordReader recordReader = inputFormat.getRecordReader(splits.get(0), jobConf, null);
                Void key = (Void) recordReader.createKey();
                ArrayWritable writable = (ArrayWritable) recordReader.createValue();
                while (recordReader.next(key, writable)) {
                    GenericRecordBuilder newRecord = new GenericRecordBuilder(schema);
                    // writable returns an array with [field1, field2, _hoodie_commit_time, _hoodie_commit_seqno]
                    Writable[] values = writable.get();
                    schema.getFields().forEach(field -> {
                        newRecord.set(field, values[0]);
                    });
                    records.add(newRecord.build());
                }
            } catch (IOException ie) {
                ie.printStackTrace();
            }
            return records;
        }).reduce((a, b) -> {
            a.addAll(b);
            return a;
        }).get();
    }


}
