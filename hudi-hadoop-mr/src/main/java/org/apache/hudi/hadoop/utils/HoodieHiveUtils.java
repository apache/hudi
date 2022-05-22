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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ImmutableTriple;
import org.apache.hudi.exception.HoodieException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class HoodieHiveUtils {

  public static final Logger LOG = LogManager.getLogger(HoodieHiveUtils.class);

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
      if (s.getValue().trim().toUpperCase().equals(INCREMENTAL_SCAN_MODE)) {
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

  public static final boolean SUPPORT_TIMESTAMP_WRITEABLE_V2;
  private static final Class TIMESTAMP_CLASS;
  private static final Method SET_TIME_IN_MILLIS;
  private static final Constructor TIMESTAMP_WRITEABLE_V2_CONSTRUCTOR;

  public static final boolean SUPPORT_DATE_WRITEABLE_V2;
  private static final Constructor DATE_WRITEABLE_V2_CONSTRUCTOR;

  static {
    // timestamp
    Option<ImmutableTriple<Class, Method, Constructor>> timestampTriple = Option.ofNullable(() -> {
      try {
        Class timestampClass = Class.forName("org.apache.hadoop.hive.common.type.Timestamp");
        Method setTimeInMillis = timestampClass.getDeclaredMethod("setTimeInMillis", long.class);
        Class twV2Class = Class.forName("org.apache.hadoop.hive.serde2.io.TimestampWritableV2");
        return ImmutableTriple.of(timestampClass, setTimeInMillis, twV2Class.getConstructor(timestampClass));
      } catch (ClassNotFoundException | NoSuchMethodException e) {
        LOG.trace("can not find hive3 timestampv2 class or method, use hive2 class!", e);
        return null;
      }
    });
    SUPPORT_TIMESTAMP_WRITEABLE_V2 = timestampTriple.isPresent();
    if (SUPPORT_TIMESTAMP_WRITEABLE_V2) {
      LOG.trace("use org.apache.hadoop.hive.serde2.io.TimestampWritableV2 to read hudi timestamp columns");
      ImmutableTriple<Class, Method, Constructor> triple = timestampTriple.get();
      TIMESTAMP_CLASS = triple.left;
      SET_TIME_IN_MILLIS = triple.middle;
      TIMESTAMP_WRITEABLE_V2_CONSTRUCTOR = triple.right;
    } else {
      LOG.trace("use org.apache.hadoop.hive.serde2.io.TimestampWritable to read hudi timestamp columns");
      TIMESTAMP_CLASS = null;
      SET_TIME_IN_MILLIS = null;
      TIMESTAMP_WRITEABLE_V2_CONSTRUCTOR = null;
    }

    // date
    Option<Constructor> dateConstructor = Option.ofNullable(() -> {
      try {
        Class dateV2Class = Class.forName("org.apache.hadoop.hive.serde2.io.DateWritableV2");
        return dateV2Class.getConstructor(int.class);
      } catch (ClassNotFoundException | NoSuchMethodException e) {
        LOG.trace("can not find hive3 datev2 class or method, use hive2 class!", e);
        return null;
      }
    });
    SUPPORT_DATE_WRITEABLE_V2 = dateConstructor.isPresent();
    if (SUPPORT_DATE_WRITEABLE_V2) {
      LOG.trace("use org.apache.hadoop.hive.serde2.io.DateWritableV2 to read hudi date columns");
      DATE_WRITEABLE_V2_CONSTRUCTOR = dateConstructor.get();
    } else {
      LOG.trace("use org.apache.hadoop.hive.serde2.io.DateWritable to read hudi date columns");
      DATE_WRITEABLE_V2_CONSTRUCTOR = null;
    }
  }

  /**
   * Get timestamp writeable object from long value.
   * Hive3 use TimestampWritableV2 to build timestamp objects and Hive2 use TimestampWritable.
   * So that we need to initialize timestamp according to the version of Hive.
   */
  public static Writable getTimestampWriteable(long value, boolean timestampMillis) {
    if (SUPPORT_TIMESTAMP_WRITEABLE_V2) {
      try {
        Object timestamp = TIMESTAMP_CLASS.newInstance();
        SET_TIME_IN_MILLIS.invoke(timestamp, timestampMillis ? value : value / 1000);
        return (Writable) TIMESTAMP_WRITEABLE_V2_CONSTRUCTOR.newInstance(timestamp);
      } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
        throw new HoodieException("can not create writable v2 class!", e);
      }
    } else {
      Timestamp timestamp = new Timestamp(timestampMillis ? value : value / 1000);
      return new TimestampWritable(timestamp);
    }
  }

  /**
   * Get date writeable object from int value.
   * Hive3 use DateWritableV2 to build date objects and Hive2 use DateWritable.
   * So that we need to initialize date according to the version of Hive.
   */
  public static Writable getDateWriteable(int value) {
    if (SUPPORT_DATE_WRITEABLE_V2) {
      try {
        return (Writable) DATE_WRITEABLE_V2_CONSTRUCTOR.newInstance(value);
      } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
        throw new HoodieException("can not create writable v2 class!", e);
      }
    } else {
      return new DateWritable(value);
    }
  }
}
