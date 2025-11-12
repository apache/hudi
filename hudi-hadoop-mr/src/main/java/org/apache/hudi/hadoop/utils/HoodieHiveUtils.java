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

package org.apache.hudi.hadoop.utils;

import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.utils.shims.HiveShim;
import org.apache.hudi.hadoop.utils.shims.HiveShims;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hive.common.util.HiveVersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class HoodieHiveUtils {

  public static final Logger LOG = LoggerFactory.getLogger(HoodieHiveUtils.class);

  public static final String HOODIE_INCREMENTAL_USE_DATABASE = "hoodie.incremental.use.database";
  public static final String HOODIE_CONSUME_MODE_PATTERN = "hoodie.%s.consume.mode";
  public static final String HOODIE_START_COMMIT_PATTERN = "hoodie.%s.consume.start.timestamp";
  public static final String HOODIE_MAX_COMMIT_PATTERN = "hoodie.%s.consume.max.commits";
  public static final String HOODIE_CONSUME_PENDING_COMMITS = "hoodie.%s.consume.pending.commits";
  public static final String HOODIE_CONSUME_COMMIT = "hoodie.%s.consume.commit";
  public static final Set<String> VIRTUAL_COLUMN_NAMES = CollectionUtils.createImmutableSet(
      "INPUT__FILE__NAME", "BLOCK__OFFSET__INSIDE__FILE", "ROW__OFFSET__INSIDE__BLOCK", "RAW__DATA__SIZE",
      "ROW__ID", "GROUPING__ID");
  /*
   * Boolean property to stop incremental reader when there is a pending compaction.
   * This is needed to prevent certain race conditions with RO views of MOR tables. only applicable for RO views.
   *
   * example timeline:
   *
   * t0 -> create bucket1.parquet
   * t1 -> create and append updates bucket1.log
   * t2 -> request compaction
   * t3 -> create bucket2.parquet
   *
   * if compaction at t2 takes a long time, incremental readers on RO tables can move to t3 and would skip updates in t1
   *
   * To workaround this problem, we want to stop returning data belonging to commits > t2.
   * After compaction is complete, incremental reader would see updates in t2, t3, so on.
   */
  public static final String HOODIE_STOP_AT_COMPACTION_PATTERN = "hoodie.%s.ro.stop.at.compaction";
  public static final String INCREMENTAL_SCAN_MODE = "INCREMENTAL";
  public static final String SNAPSHOT_SCAN_MODE = "SNAPSHOT";
  public static final String DEFAULT_SCAN_MODE = SNAPSHOT_SCAN_MODE;
  public static final int DEFAULT_MAX_COMMITS = 1;
  public static final int MAX_COMMIT_ALL = -1;
  public static final Pattern HOODIE_CONSUME_MODE_PATTERN_STRING = Pattern.compile("hoodie\\.(.*)\\.consume\\.mode");
  public static final String GLOBALLY_CONSISTENT_READ_TIMESTAMP = "last_replication_timestamp";

  private static final boolean IS_HIVE3 = isHive3();

  private static final HiveShim HIVE_SHIM = HiveShims.getInstance(IS_HIVE3);

  public static boolean shouldIncludePendingCommits(JobConf job, String tableName) {
    return job.getBoolean(String.format(HOODIE_CONSUME_PENDING_COMMITS, tableName), false);
  }

  public static Option<String> getMaxCommit(JobConf job, String tableName) {
    return Option.ofNullable(job.get(String.format(HOODIE_CONSUME_COMMIT, tableName)));
  }

  public static boolean stopAtCompaction(JobContext job, String tableName) {
    String compactionPropName = String.format(HOODIE_STOP_AT_COMPACTION_PATTERN, tableName);
    boolean stopAtCompaction = job.getConfiguration().getBoolean(compactionPropName, true);
    LOG.info("Read stop at compaction - " + stopAtCompaction);
    return stopAtCompaction;
  }

  public static Integer readMaxCommits(JobContext job, String tableName) {
    String maxCommitName = String.format(HOODIE_MAX_COMMIT_PATTERN, tableName);
    int maxCommits = job.getConfiguration().getInt(maxCommitName, DEFAULT_MAX_COMMITS);
    if (maxCommits == MAX_COMMIT_ALL) {
      maxCommits = Integer.MAX_VALUE;
    }
    LOG.info("Read max commits - " + maxCommits);
    return maxCommits;
  }

  public static String readStartCommitTime(JobContext job, String tableName) {
    String startCommitTimestampName = String.format(HOODIE_START_COMMIT_PATTERN, tableName);
    LOG.info("Read start commit time - " + job.getConfiguration().get(startCommitTimestampName));
    return job.getConfiguration().get(startCommitTimestampName);
  }

  /**
   * Gets the n'th parent for the Path. Assumes the path has at-least n components
   *
   * @param path
   * @param n
   * @return
   */
  public static Path getNthParent(Path path, int n) {
    Path parent = path;
    for (int i = 0; i < n; i++) {
      parent = parent.getParent();
    }
    return parent;
  }

  /**
   * Returns a list of tableNames for which hoodie.<tableName>.consume.mode is set to incremental else returns empty List
   *
   * @param job
   * @return
   */
  public static List<String> getIncrementalTableNames(JobContext job) {
    Map<String, String> tablesModeMap = job.getConfiguration()
        .getValByRegex(HOODIE_CONSUME_MODE_PATTERN_STRING.pattern());
    List<String> result = tablesModeMap.entrySet().stream().map(s -> {
      if (s.getValue().trim().equalsIgnoreCase(INCREMENTAL_SCAN_MODE)) {
        Matcher matcher = HOODIE_CONSUME_MODE_PATTERN_STRING.matcher(s.getKey());
        return (!matcher.find() ? null : matcher.group(1));
      }
      return null;
    }).filter(Objects::nonNull).collect(Collectors.toList());
    if (result == null) {
      // Returns an empty list instead of null.
      result = new ArrayList<>();
    }
    return result;
  }

  public static boolean isIncrementalUseDatabase(Configuration conf) {
    return conf.getBoolean(HOODIE_INCREMENTAL_USE_DATABASE, false);
  }

  public static boolean isHive3() {
    return HiveVersionInfo.getShortVersion().startsWith("3");
  }

  public static boolean isHive2() {
    return HiveVersionInfo.getShortVersion().startsWith("2");
  }

  /**
   * Get timestamp writeable object from long value.
   * Hive3 use TimestampWritableV2 to build timestamp objects and Hive2 use TimestampWritable.
   * So that we need to initialize timestamp according to the version of Hive.
   */
  public static Writable getTimestampWriteable(long value, boolean timestampMillis) {
    return HIVE_SHIM.getTimestampWriteable(value, timestampMillis);
  }

  /**
   * Get date writeable object from int value.
   * Hive3 use DateWritableV2 to build date objects and Hive2 use DateWritable.
   * So that we need to initialize date according to the version of Hive.
   */
  public static Writable getDateWriteable(int value) {
    return HIVE_SHIM.getDateWriteable(value);
  }

  public static int getDays(Object dateWritable) {
    return HIVE_SHIM.getDays(dateWritable);
  }

  public static long getMills(Object timestampWritable) {
    return HIVE_SHIM.getMills(timestampWritable);
  }
}
