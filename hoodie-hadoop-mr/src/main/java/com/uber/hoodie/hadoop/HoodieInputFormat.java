/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.hadoop;

import com.uber.hoodie.common.model.HoodieDataFile;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.TableFileSystemView;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.table.view.HoodieTableFileSystemView;
import com.uber.hoodie.exception.InvalidDatasetException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.read.ParquetFilterPredicateConverter;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import parquet.filter2.predicate.FilterPredicate;
import parquet.filter2.predicate.Operators;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.metadata.FileMetaData;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.io.api.Binary;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static parquet.filter2.predicate.FilterApi.and;
import static parquet.filter2.predicate.FilterApi.binaryColumn;
import static parquet.filter2.predicate.FilterApi.gt;

/**
 * HoodieInputFormat which understands the Hoodie File Structure and filters
 * files based on the Hoodie Mode. If paths that does not correspond to a hoodie dataset
 * then they are passed in as is (as what FileInputFormat.listStatus() would do).
 * The JobConf could have paths from multipe Hoodie/Non-Hoodie datasets
 */
@UseFileSplitsFromInputFormat
public class HoodieInputFormat extends MapredParquetInputFormat
    implements Configurable {
    public static final Log LOG = LogFactory.getLog(HoodieInputFormat.class);
    private Configuration conf;

    @Override
    public FileStatus[] listStatus(JobConf job) throws IOException {
        // Get all the file status from FileInputFormat and then do the filter
        FileStatus[] fileStatuses = super.listStatus(job);
        Map<HoodieTableMetaClient, List<FileStatus>> groupedFileStatus = groupFileStatus(fileStatuses);
        LOG.info("Found a total of " + groupedFileStatus.size() + " groups");
        List<FileStatus> returns = new ArrayList<FileStatus>();
        for(Map.Entry<HoodieTableMetaClient, List<FileStatus>> entry:groupedFileStatus.entrySet()) {
            HoodieTableMetaClient metadata = entry.getKey();
            if(metadata == null) {
                // Add all the paths which are not hoodie specific
                returns.addAll(entry.getValue());
                continue;
            }

            FileStatus[] value = entry.getValue().toArray(new FileStatus[entry.getValue().size()]);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Hoodie Metadata initialized with completed commit Ts as :" + metadata);
            }
            String tableName = metadata.getTableConfig().getTableName();
            String mode = HoodieHiveUtil.readMode(Job.getInstance(job), tableName);
            HoodieTimeline timeline = metadata.getActiveTimeline().getCommitTimeline().filterCompletedInstants();
            TableFileSystemView fsView = new HoodieTableFileSystemView(metadata, timeline);

            if (HoodieHiveUtil.INCREMENTAL_SCAN_MODE.equals(mode)) {
                // this is of the form commitTs_partition_sequenceNumber
                String lastIncrementalTs = HoodieHiveUtil.readStartCommitTime(Job.getInstance(job), tableName);
                // Total number of commits to return in this batch. Set this to -1 to get all the commits.
                Integer maxCommits = HoodieHiveUtil.readMaxCommits(Job.getInstance(job), tableName);
                LOG.info("Last Incremental timestamp was set as " + lastIncrementalTs);
                List<String> commitsToReturn =
                    timeline.findInstantsAfter(lastIncrementalTs, maxCommits).getInstants()
                        .map(HoodieInstant::getTimestamp).collect(Collectors.toList());
                List<HoodieDataFile> filteredFiles =
                    fsView.getLatestVersionInRange(value, commitsToReturn)
                        .collect(Collectors.toList());
                for (HoodieDataFile filteredFile : filteredFiles) {
                    LOG.info("Processing incremental hoodie file - " + filteredFile.getPath());
                    returns.add(filteredFile.getFileStatus());
                }
                LOG.info(
                    "Total paths to process after hoodie incremental filter " + filteredFiles.size());
            } else {
                // filter files on the latest commit found
                List<HoodieDataFile> filteredFiles = fsView.getLatestVersions(value).collect(Collectors.toList());
                LOG.info("Total paths to process after hoodie filter " + filteredFiles.size());
                for (HoodieDataFile filteredFile : filteredFiles) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Processing latest hoodie file - " + filteredFile.getPath());
                    }
                    returns.add(filteredFile.getFileStatus());
                }
            }
        }
        return returns.toArray(new FileStatus[returns.size()]);

    }

    private Map<HoodieTableMetaClient, List<FileStatus>> groupFileStatus(FileStatus[] fileStatuses)
        throws IOException {
        // This assumes the paths for different tables are grouped together
        Map<HoodieTableMetaClient, List<FileStatus>> grouped = new HashMap<>();
        HoodieTableMetaClient metadata = null;
        String nonHoodieBasePath = null;
        for(FileStatus status:fileStatuses) {
            if ((metadata == null && nonHoodieBasePath == null) || (metadata == null && !status.getPath().toString()
                .contains(nonHoodieBasePath)) || (metadata != null && !status.getPath().toString()
                .contains(metadata.getBasePath()))) {
                try {
                    metadata = getTableMetaClient(status.getPath().getParent());
                    nonHoodieBasePath = null;
                } catch (InvalidDatasetException e) {
                    LOG.info("Handling a non-hoodie path " + status.getPath());
                    metadata = null;
                    nonHoodieBasePath =
                        status.getPath().getParent().toString();
                }
                if(!grouped.containsKey(metadata)) {
                    grouped.put(metadata, new ArrayList<>());
                }
            }
            grouped.get(metadata).add(status);
        }
        return grouped;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public Configuration getConf() {
        return conf;
    }

    @Override
    public RecordReader<Void, ArrayWritable> getRecordReader(final InputSplit split,
        final JobConf job, final Reporter reporter) throws IOException {
        // TODO enable automatic predicate pushdown after fixing issues
//        FileSplit fileSplit = (FileSplit) split;
//        HoodieTableMetadata metadata = getTableMetadata(fileSplit.getPath().getParent());
//        String tableName = metadata.getTableName();
//        String mode = HoodieHiveUtil.readMode(job, tableName);

//        if (HoodieHiveUtil.INCREMENTAL_SCAN_MODE.equals(mode)) {
//            FilterPredicate predicate = constructHoodiePredicate(job, tableName, split);
//            LOG.info("Setting parquet predicate push down as " + predicate);
//            ParquetInputFormat.setFilterPredicate(job, predicate);
            //clearOutExistingPredicate(job);
//        }
        return super.getRecordReader(split, job, reporter);
    }

    /**
     * Clears out the filter expression (if this is not done, then ParquetReader will override the FilterPredicate set)
     *
     * @param job
     */
    private void clearOutExistingPredicate(JobConf job) {
        job.unset(TableScanDesc.FILTER_EXPR_CONF_STR);
    }

    /**
     * Constructs the predicate to push down to parquet storage.
     * This creates the predicate for `hoodie_commit_time` > 'start_commit_time' and ANDs with the existing predicate if one is present already.
     *
     * @param job
     * @param tableName
     * @return
     */
    private FilterPredicate constructHoodiePredicate(JobConf job, String tableName,
        InputSplit split) throws IOException {
        FilterPredicate commitTimePushdown = constructCommitTimePushdownPredicate(job, tableName);
        LOG.info("Commit time predicate - " + commitTimePushdown.toString());
        FilterPredicate existingPushdown = constructHQLPushdownPredicate(job, split);
        LOG.info("Existing predicate - " + existingPushdown);

        FilterPredicate hoodiePredicate;
        if (existingPushdown != null) {
            hoodiePredicate = and(existingPushdown, commitTimePushdown);
        } else {
            hoodiePredicate = commitTimePushdown;
        }
        LOG.info("Hoodie Predicate - " + hoodiePredicate);
        return hoodiePredicate;
    }

    private FilterPredicate constructHQLPushdownPredicate(JobConf job, InputSplit split)
        throws IOException {
        String serializedPushdown = job.get(TableScanDesc.FILTER_EXPR_CONF_STR);
        String columnNamesString = job.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR);
        if (serializedPushdown == null || columnNamesString == null || serializedPushdown.isEmpty()
            || columnNamesString.isEmpty()) {
            return null;
        } else {
            SearchArgument sarg =
                SearchArgumentFactory.create(Utilities.deserializeExpression(serializedPushdown));
            final Path finalPath = ((FileSplit) split).getPath();
            final ParquetMetadata parquetMetadata = ParquetFileReader.readFooter(job, finalPath);
            final FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
            return ParquetFilterPredicateConverter
                .toFilterPredicate(sarg, fileMetaData.getSchema());
        }
    }

    private FilterPredicate constructCommitTimePushdownPredicate(JobConf job, String tableName)
        throws IOException {
        String lastIncrementalTs = HoodieHiveUtil.readStartCommitTime(Job.getInstance(job), tableName);
        Operators.BinaryColumn sequenceColumn =
            binaryColumn(HoodieRecord.COMMIT_TIME_METADATA_FIELD);
        FilterPredicate p = gt(sequenceColumn, Binary.fromString(lastIncrementalTs));
        LOG.info("Setting predicate in InputFormat " + p.toString());
        return p;
    }

    /**
     * Read the table metadata from a data path. This assumes certain hierarchy of files which
     * should be changed once a better way is figured out to pass in the hoodie meta directory
     *
     * @param dataPath
     * @return
     * @throws IOException
     */
    private HoodieTableMetaClient getTableMetaClient(Path dataPath) throws IOException {
        FileSystem fs = dataPath.getFileSystem(conf);
        // TODO - remove this hard-coding. Pass this in job conf, somehow. Or read the Table Location
        Path baseDir = dataPath.getParent().getParent().getParent();
        LOG.info("Reading hoodie metadata from path " + baseDir.toString());
        return new HoodieTableMetaClient(fs, baseDir.toString());

    }
}
