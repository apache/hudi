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

package org.apache.hudi.utilities.sources.helpers;

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.utilities.sources.helpers.DFSPathSelector.Config.ROOT_INPUT_PATH_PROP;
import static org.apache.hudi.utilities.sources.helpers.DatePartitionPathSelector.Config.DATE_FORMAT;
import static org.apache.hudi.utilities.sources.helpers.DatePartitionPathSelector.Config.DATE_PARTITION_DEPTH;
import static org.apache.hudi.utilities.sources.helpers.DatePartitionPathSelector.Config.DEFAULT_DATE_FORMAT;
import static org.apache.hudi.utilities.sources.helpers.DatePartitionPathSelector.Config.DEFAULT_DATE_PARTITION_DEPTH;
import static org.apache.hudi.utilities.sources.helpers.DatePartitionPathSelector.Config.DEFAULT_LOOKBACK_DAYS;
import static org.apache.hudi.utilities.sources.helpers.DatePartitionPathSelector.Config.DEFAULT_PARTITIONS_LIST_PARALLELISM;
import static org.apache.hudi.utilities.sources.helpers.DatePartitionPathSelector.Config.LOOKBACK_DAYS;
import static org.apache.hudi.utilities.sources.helpers.DatePartitionPathSelector.Config.PARTITIONS_LIST_PARALLELISM;

/**
 * Custom dfs path selector used to list just the last few days provided there is a date based
 * partition.
 *
 * <p>This is useful for workloads where there are multiple partition fields and only recent
 * partitions are affected by new writes. Especially if the data sits in S3, listing all historical
 * data can be time expensive and unnecessary for the above type of workload.
 *
 * <p>The date based partition is expected to be of the format '<date string>=yyyy-mm-dd' or
 * 'yyyy-mm-dd'. The date partition can be at any level. For ex. the partition path can be of the
 * form `<basepath>/<partition-field1>/<date-based-partition>/<partition-field3>/` or
 * `<basepath>/<<date-based-partition>/`.
 *
 * <p>The date based partition format can be configured via this property
 * hoodie.deltastreamer.source.dfs.datepartitioned.date.format
 */
public class DatePartitionPathSelector extends DFSPathSelector {

  private static volatile Logger LOG = LogManager.getLogger(DatePartitionPathSelector.class);

  private final String dateFormat;
  private final int datePartitionDepth;
  private final int numPrevDaysToList;
  private final int partitionsListParallelism;

  /** Configs supported. */
  public static class Config {
    public static final String DATE_FORMAT = "hoodie.deltastreamer.source.dfs.datepartitioned.date.format";
    public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";

    public static final String DATE_PARTITION_DEPTH =
        "hoodie.deltastreamer.source.dfs.datepartitioned.selector.depth";
    public static final int DEFAULT_DATE_PARTITION_DEPTH = 0; // Implies no (date) partition

    public static final String LOOKBACK_DAYS =
        "hoodie.deltastreamer.source.dfs.datepartitioned.selector.lookback.days";
    public static final int DEFAULT_LOOKBACK_DAYS = 2;

    public static final String CURRENT_DATE =
        "hoodie.deltastreamer.source.dfs.datepartitioned.selector.currentdate";

    public static final String PARTITIONS_LIST_PARALLELISM =
        "hoodie.deltastreamer.source.dfs.datepartitioned.selector.parallelism";
    public static final int DEFAULT_PARTITIONS_LIST_PARALLELISM = 20;
  }

  public DatePartitionPathSelector(TypedProperties props, Configuration hadoopConf) {
    super(props, hadoopConf);
    /*
     * datePartitionDepth = 0 is same as basepath and there is no partition. In which case
     * this path selector would be a no-op and lists all paths under the table basepath.
     */
    dateFormat = props.getString(DATE_FORMAT, DEFAULT_DATE_FORMAT);
    datePartitionDepth = props.getInteger(DATE_PARTITION_DEPTH, DEFAULT_DATE_PARTITION_DEPTH);
    numPrevDaysToList = props.getInteger(LOOKBACK_DAYS, DEFAULT_LOOKBACK_DAYS);
    partitionsListParallelism = props.getInteger(PARTITIONS_LIST_PARALLELISM, DEFAULT_PARTITIONS_LIST_PARALLELISM);
  }

  @Override
  public Pair<Option<String>, String> getNextFilePathsAndMaxModificationTime(JavaSparkContext sparkContext,
                                                                             Option<String> lastCheckpointStr,
                                                                             long sourceLimit) {
    // If not specified the current date is assumed by default.
    LocalDate currentDate = LocalDate.parse(props.getString(Config.CURRENT_DATE, LocalDate.now().toString()));

    // obtain all eligible files under root folder.
    LOG.info(
        "Root path => "
            + props.getString(ROOT_INPUT_PATH_PROP)
            + " source limit => "
            + sourceLimit
            + " depth of day partition => "
            + datePartitionDepth
            + " num prev days to list => "
            + numPrevDaysToList
            + " from current date => "
            + currentDate);
    long lastCheckpointTime = lastCheckpointStr.map(Long::parseLong).orElse(Long.MIN_VALUE);
    HoodieSparkEngineContext context = new HoodieSparkEngineContext(sparkContext);
    SerializableConfiguration serializedConf = new SerializableConfiguration(fs.getConf());
    List<String> prunedPartitionPaths = pruneDatePartitionPaths(context, fs, props.getString(ROOT_INPUT_PATH_PROP), currentDate);

    List<FileStatus> eligibleFiles = context.flatMap(prunedPartitionPaths,
        path -> {
          FileSystem fs = new Path(path).getFileSystem(serializedConf.get());
          return listEligibleFiles(fs, new Path(path), lastCheckpointTime).stream();
        }, partitionsListParallelism);
    // sort them by modification time ascending.
    List<FileStatus> sortedEligibleFiles = eligibleFiles.stream()
        .sorted(Comparator.comparingLong(FileStatus::getModificationTime)).collect(Collectors.toList());

    // Filter based on checkpoint & input size, if needed
    long currentBytes = 0;
    long newCheckpointTime = lastCheckpointTime;
    List<FileStatus> filteredFiles = new ArrayList<>();
    for (FileStatus f : sortedEligibleFiles) {
      if (currentBytes + f.getLen() >= sourceLimit && f.getModificationTime() > newCheckpointTime) {
        // we have enough data, we are done
        // Also, we've read up to a file with a newer modification time
        // so that some files with the same modification time won't be skipped in next read
        break;
      }

      newCheckpointTime = f.getModificationTime();
      currentBytes += f.getLen();
      filteredFiles.add(f);
    }

    // no data to read
    if (filteredFiles.isEmpty()) {
      return new ImmutablePair<>(Option.empty(), String.valueOf(newCheckpointTime));
    }

    // read the files out.
    String pathStr = filteredFiles.stream().map(f -> f.getPath().toString()).collect(Collectors.joining(","));

    return new ImmutablePair<>(Option.ofNullable(pathStr), String.valueOf(newCheckpointTime));
  }

  /**
   * Prunes date level partitions to last few days configured by 'NUM_PREV_DAYS_TO_LIST' from
   * 'CURRENT_DATE'. Parallelizes listing by leveraging HoodieSparkEngineContext's methods.
   */
  public List<String> pruneDatePartitionPaths(HoodieSparkEngineContext context, FileSystem fs, String rootPath, LocalDate currentDate) {
    List<String> partitionPaths = new ArrayList<>();
    // get all partition paths before date partition level
    partitionPaths.add(rootPath);
    if (datePartitionDepth <= 0) {
      return partitionPaths;
    }
    SerializableConfiguration serializedConf = new SerializableConfiguration(fs.getConf());
    for (int i = 0; i < datePartitionDepth; i++) {
      partitionPaths = context.flatMap(partitionPaths, path -> {
        Path subDir = new Path(path);
        FileSystem fileSystem = subDir.getFileSystem(serializedConf.get());
        // skip files/dirs whose names start with (_, ., etc)
        FileStatus[] statuses = fileSystem.listStatus(subDir,
            file -> IGNORE_FILEPREFIX_LIST.stream().noneMatch(pfx -> file.getName().startsWith(pfx)));
        List<String> res = new ArrayList<>();
        for (FileStatus status : statuses) {
          res.add(status.getPath().toString());
        }
        return res.stream();
      }, partitionsListParallelism);
    }

    // Prune date partitions to last few days
    return context.getJavaSparkContext().parallelize(partitionPaths, partitionsListParallelism)
        .filter(s -> {
          LocalDate fromDate = currentDate.minusDays(numPrevDaysToList);
          String[] splits = s.split("/");
          String datePartition = splits[splits.length - 1];
          LocalDate partitionDate;
          DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(dateFormat);
          if (datePartition.contains("=")) {
            String[] moreSplit = datePartition.split("=");
            ValidationUtils.checkArgument(
                moreSplit.length == 2,
                "Partition Field (" + datePartition + ") not in expected format");
            partitionDate = LocalDate.parse(moreSplit[1], dateFormatter);
          } else {
            partitionDate = LocalDate.parse(datePartition, dateFormatter);
          }
          return (partitionDate.isEqual(fromDate) || partitionDate.isAfter(fromDate))
              && (partitionDate.isEqual(currentDate) || partitionDate.isBefore(currentDate));
        }).collect();
  }
}
