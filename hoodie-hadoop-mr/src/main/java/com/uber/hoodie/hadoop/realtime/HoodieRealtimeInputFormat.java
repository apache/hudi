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

package com.uber.hoodie.hadoop.realtime;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.uber.hoodie.common.model.FileSlice;
import com.uber.hoodie.common.model.HoodieLogFile;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.table.view.HoodieTableFileSystemView;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.exception.HoodieException;
import com.uber.hoodie.exception.HoodieIOException;
import com.uber.hoodie.hadoop.HoodieInputFormat;
import com.uber.hoodie.hadoop.UseFileSplitsFromInputFormat;
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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * Input Format, that provides a real-time view of data in a Hoodie dataset
 */
@UseFileSplitsFromInputFormat
public class HoodieRealtimeInputFormat extends HoodieInputFormat implements Configurable {

  public static final Log LOG = LogFactory.getLog(HoodieRealtimeInputFormat.class);

  // These positions have to be deterministic across all tables
  public static final int HOODIE_COMMIT_TIME_COL_POS = 0;
  public static final int HOODIE_RECORD_KEY_COL_POS = 2;
  public static final int HOODIE_PARTITION_PATH_COL_POS = 3;
  // Track the read column ids and names to be used throughout the execution and lifetime of this task
  // Needed for Hive on Spark. Our theory is that due to
  // {@link org.apache.hadoop.hive.ql.io.parquet.ProjectionPusher}
  // not handling empty list correctly, the ParquetRecordReaderWrapper ends up adding the same column ids multiple
  // times which ultimately breaks the query.
  // TODO : Find why RO view works fine but RT doesn't, JIRA: https://issues.apache.org/jira/browse/HUDI-151
  public static String READ_COLUMN_IDS;
  public static String READ_COLUMN_NAMES;
  public static boolean isReadColumnsSet = false;

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {

    Stream<FileSplit> fileSplits = Arrays.stream(super.getSplits(job, numSplits))
        .map(is -> (FileSplit) is);

    // obtain all unique parent folders for splits
    Map<Path, List<FileSplit>> partitionsToParquetSplits = fileSplits
        .collect(Collectors.groupingBy(split -> split.getPath().getParent()));
    // TODO(vc): Should we handle also non-hoodie splits here?
    Map<String, HoodieTableMetaClient> metaClientMap = new HashMap<>();
    Map<Path, HoodieTableMetaClient> partitionsToMetaClient = partitionsToParquetSplits.keySet()
        .stream().collect(Collectors.toMap(Function.identity(), p -> {
          // find if we have a metaclient already for this partition.
          Optional<String> matchingBasePath = metaClientMap.keySet().stream()
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

    // for all unique split parents, obtain all delta files based on delta commit timeline,
    // grouped on file id
    List<HoodieRealtimeFileSplit> rtSplits = new ArrayList<>();
    partitionsToParquetSplits.keySet().stream().forEach(partitionPath -> {
      // for each partition path obtain the data & log file groupings, then map back to inputsplits
      HoodieTableMetaClient metaClient = partitionsToMetaClient.get(partitionPath);
      HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient,
          metaClient.getActiveTimeline());
      String relPartitionPath = FSUtils
          .getRelativePartitionPath(new Path(metaClient.getBasePath()), partitionPath);

      try {
        // Both commit and delta-commits are included - pick the latest completed one
        Optional<HoodieInstant> latestCompletedInstant =
            metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().lastInstant();

        Stream<FileSlice> latestFileSlices = latestCompletedInstant.map(instant ->
            fsView.getLatestMergedFileSlicesBeforeOrOn(relPartitionPath, instant.getTimestamp()))
            .orElse(Stream.empty());

        // subgroup splits again by file id & match with log files.
        Map<String, List<FileSplit>> groupedInputSplits = partitionsToParquetSplits
            .get(partitionPath).stream()
            .collect(Collectors.groupingBy(split -> FSUtils.getFileId(split.getPath().getName())));
        latestFileSlices.forEach(fileSlice -> {
          List<FileSplit> dataFileSplits = groupedInputSplits.get(fileSlice.getFileId());
          dataFileSplits.forEach(split -> {
            try {
              List<String> logFilePaths = fileSlice.getLogFiles().sorted(HoodieLogFile.getLogFileComparator())
                  .map(logFile -> logFile.getPath().toString()).collect(Collectors.toList());
              // Get the maxCommit from the last delta or compaction or commit - when
              // bootstrapped from COW table
              String maxCommitTime = metaClient.getActiveTimeline().getTimelineOfActions(
                  Sets.newHashSet(HoodieTimeline.COMMIT_ACTION, HoodieTimeline.ROLLBACK_ACTION,
                      HoodieTimeline.DELTA_COMMIT_ACTION)).filterCompletedInstants().lastInstant()
                  .get().getTimestamp();
              rtSplits.add(
                  new HoodieRealtimeFileSplit(split, metaClient.getBasePath(), logFilePaths,
                      maxCommitTime));
            } catch (IOException e) {
              throw new HoodieIOException("Error creating hoodie real time split ", e);
            }
          });
        });
      } catch (Exception e) {
        throw new HoodieException("Error obtaining data file/log file grouping: " + partitionPath,
            e);
      }
    });
    LOG.info("Returning a total splits of " + rtSplits.size());
    return rtSplits.toArray(new InputSplit[rtSplits.size()]);
  }


  @Override
  public FileStatus[] listStatus(JobConf job) throws IOException {
    // Call the HoodieInputFormat::listStatus to obtain all latest parquet files, based on commit
    // timeline.
    return super.listStatus(job);
  }

  /**
   * Add a field to the existing fields projected
   */
  private static Configuration addProjectionField(Configuration conf, String fieldName,
      int fieldIndex) {
    String readColNames = conf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, "");
    String readColIds = conf.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, "");

    String readColNamesPrefix = readColNames + ",";
    if (readColNames == null || readColNames.isEmpty()) {
      readColNamesPrefix = "";
    }
    String readColIdsPrefix = readColIds + ",";
    if (readColIds == null || readColIds.isEmpty()) {
      readColIdsPrefix = "";
    }

    if (!readColNames.contains(fieldName)) {
      // If not already in the list - then add it
      conf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, readColNamesPrefix + fieldName);
      conf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, readColIdsPrefix + fieldIndex);
      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format(
            "Adding extra column " + fieldName + ", to enable log merging cols (%s) ids (%s) ",
            conf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR),
            conf.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR)));
      }
    }
    return conf;
  }

  private static synchronized Configuration addRequiredProjectionFields(Configuration configuration) {
    // Need this to do merge records in HoodieRealtimeRecordReader
    configuration = addProjectionField(configuration, HoodieRecord.RECORD_KEY_METADATA_FIELD,
        HOODIE_RECORD_KEY_COL_POS);
    configuration = addProjectionField(configuration, HoodieRecord.COMMIT_TIME_METADATA_FIELD,
        HOODIE_COMMIT_TIME_COL_POS);
    configuration = addProjectionField(configuration, HoodieRecord.PARTITION_PATH_METADATA_FIELD,
        HOODIE_PARTITION_PATH_COL_POS);
    if (!isReadColumnsSet) {
      READ_COLUMN_IDS = configuration.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR);
      READ_COLUMN_NAMES = configuration.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR);
      isReadColumnsSet = true;
    }
    return configuration;
  }

  @Override
  public RecordReader<NullWritable, ArrayWritable> getRecordReader(final InputSplit split,
      final JobConf job, final Reporter reporter) throws IOException {

    LOG.info("Before adding Hoodie columns, Projections :" + job
        .get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR) + ", Ids :"
        + job.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR));

    // Hive (across all versions) fails for queries like select count(`_hoodie_commit_time`) from table;
    // In this case, the projection fields gets removed. Looking at HiveInputFormat implementation, in some cases
    // hoodie additional projection columns are reset after calling setConf and only natural projections
    // (one found in select queries) are set. things would break because of this.
    // For e:g _hoodie_record_key would be missing and merge step would throw exceptions.
    // TO fix this, hoodie columns are appended late at the time record-reader gets built instead of construction time.
    this.conf = addRequiredProjectionFields(job);

    LOG.info("Creating record reader with readCols :" + job
        .get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR) + ", Ids :"
        + job.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR));
    // sanity check
    Preconditions.checkArgument(split instanceof HoodieRealtimeFileSplit,
        "HoodieRealtimeRecordReader can only work on HoodieRealtimeFileSplit and not with "
            + split);

    // Reset the original column ids and names
    job.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, READ_COLUMN_IDS);
    job.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, READ_COLUMN_NAMES);

    return new HoodieRealtimeRecordReader((HoodieRealtimeFileSplit) split, job,
        super.getRecordReader(split, job, reporter));
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
