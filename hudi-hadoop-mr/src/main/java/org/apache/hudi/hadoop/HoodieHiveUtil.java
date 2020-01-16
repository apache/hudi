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

package org.apache.hudi.hadoop;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
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
  public static final String SNAPSHOT_SCAN_MODE = "SNAPSHOT";
  public static final String DEFAULT_SCAN_MODE = SNAPSHOT_SCAN_MODE;
  public static final int DEFAULT_MAX_COMMITS = 1;
  public static final int MAX_COMMIT_ALL = -1;
  public static final int DEFAULT_LEVELS_TO_BASEPATH = 3;
  public static final Pattern HOODIE_CONSUME_MODE_PATTERN_STRING = Pattern.compile("hoodie\\.(.*)\\.consume\\.mode");

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

  public static Path getNthParent(Path path, int n) {
    Path parent = path;
    for (int i = 0; i < n; i++) {
      parent = parent.getParent();
    }
    return parent;
  }

  public static List<String> getIncrementalTableNames(JobContext job) {
    Map<String, String> tablesModeMap = job.getConfiguration()
        .getValByRegex(HOODIE_CONSUME_MODE_PATTERN_STRING.pattern());
    List<String> result = tablesModeMap.entrySet().stream().map(s -> {
      if (s.getValue().trim().equals(INCREMENTAL_SCAN_MODE)) {
        Matcher matcher = HOODIE_CONSUME_MODE_PATTERN_STRING.matcher(s.getKey());
        return (!matcher.find() ? null : matcher.group(1));
      }
      return null;
    }).filter(s -> s != null)
        .collect(Collectors.toList());
    if (result == null) {
      // Returns an empty list instead of null.
      result = new ArrayList<>();
    }
    return result;
  }
}
