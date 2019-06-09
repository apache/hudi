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

package com.uber.hoodie.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class HoodieHiveUtil {

  public static final Logger LOG = LogManager.getLogger(HoodieHiveUtil.class);

  public static final String HOODIE_CONSUME_MODE_PATTERN = "hoodie.%s.consume.mode";
  public static final String HOODIE_START_COMMIT_PATTERN = "hoodie.%s.consume.start.timestamp";
  public static final String HOODIE_MAX_COMMIT_PATTERN = "hoodie.%s.consume.max.commits";
  public static final String INCREMENTAL_SCAN_MODE = "INCREMENTAL";
  public static final String LATEST_SCAN_MODE = "LATEST";
  public static final String DEFAULT_SCAN_MODE = LATEST_SCAN_MODE;
  public static final int DEFAULT_MAX_COMMITS = 1;
  public static final int MAX_COMMIT_ALL = -1;
  public static final int DEFAULT_LEVELS_TO_BASEPATH = 3;

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

  public static String readMode(JobContext job, String tableName) {
    String modePropertyName = String.format(HOODIE_CONSUME_MODE_PATTERN, tableName);
    String mode = job.getConfiguration().get(modePropertyName, DEFAULT_SCAN_MODE);
    LOG.info(modePropertyName + ": " + mode);
    return mode;
  }

  public static Path getNthParent(Path path, int n) {
    Path parent = path;
    for (int i = 0; i < n; i++) {
      parent = parent.getParent();
    }
    return parent;
  }
}
