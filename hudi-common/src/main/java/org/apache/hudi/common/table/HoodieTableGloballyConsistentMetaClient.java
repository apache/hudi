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

package org.apache.hudi.common.table;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/*
 * Uber specific version of HoodieTableMetaClient to make sure when a table level property is set
 * to indicate a commit timestamp that is present across DC make sure to limit the local .hoodie
 * timeline to upto that commit timestamp.
 *
 * Note: There is an assumption that this means every other commit
 * that is present upto this commit is present globally. This assumption makes it easier to just
 * trim the commit timeline at the head. Otherwise we will have to store the valid commit timeline
 * in the table as a property.
 *
 * Note: This object should not be cached between mapreduce jobs since the jobConf can change
 */
public class HoodieTableGloballyConsistentMetaClient extends HoodieTableMetaClient {
  private static final Logger LOG = LogManager.getLogger(HoodieTableGloballyConsistentMetaClient.class);

  private static final String DB_AND_TABLE_NAME_KEY = "table";
  public static final String GLOBALLY_CONSISTENT_READ_TIMESTAMP = "last_replication_timestamp";
  public static final String DISABLE_HOODIE_GLOBALLY_CONSISTENT_READS = "hoodie_disable_globally_consistent_reads";

  private final String globallyConsistentReadTimestamp;
  private final String dbTableName;

  private HoodieTableGloballyConsistentMetaClient(Configuration conf, String basePath,
      String commitTimestamp, String dbTableName) throws TableNotFoundException {
    // we don't want to load the timeline yet till we set the jobConf
    super(conf, basePath, false /*loadActiveTimelineOnLoad*/,
          ConsistencyGuardConfig.newBuilder().build(), Option.of(TimelineLayoutVersion.CURR_LAYOUT_VERSION),
          null);
    this.globallyConsistentReadTimestamp = commitTimestamp;
    this.dbTableName = dbTableName;
  }

  /**
   *
   * @param hadoopConf the hadoop configuration.
   * @param basePath the base path of the hoodie table.
   * @param jobConf the hive mapred or spark job conf that has the table property.
   * @return HoodieTableMetaClient or HoodieTableGloballyConsistentMetaClient
   */
  public static HoodieTableMetaClient mkMetaClient(
      Configuration hadoopConf, String basePath, JobConf jobConf) {
    final String timeStamp = jobConf.get(GLOBALLY_CONSISTENT_READ_TIMESTAMP);
    final String dbTableName = jobConf.get(DB_AND_TABLE_NAME_KEY);
    final Boolean disableGloballyConsistentRead = jobConf.getBoolean(
        DISABLE_HOODIE_GLOBALLY_CONSISTENT_READS, false);
    if (!disableGloballyConsistentRead && !StringUtils.isNullOrEmpty(timeStamp)) {
      LOG.info(String.format("creating a globally consistent timeline for table: %s for timestamp: %s",
          timeStamp, dbTableName));
      return new HoodieTableGloballyConsistentMetaClient(
          hadoopConf, basePath, timeStamp, dbTableName);
    } else {
      return HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(basePath).build();
    }
  }

  @Override
  public List<HoodieInstant> scanHoodieInstantsFromFileSystem(
      Path timelinePath, Set<String> includedExtensions,
      boolean applyLayoutVersionFilters) throws IOException {
    List<HoodieInstant> instants = super.scanHoodieInstantsFromFileSystem(
        timelinePath, includedExtensions, applyLayoutVersionFilters);

    if (!StringUtils.isNullOrEmpty(globallyConsistentReadTimestamp)) {
      final int size = instants.size();
      LOG.info(String.format(
          "filtering out entries from metadata folder that are less than commit ts %s for table: %s original size: %d",
          globallyConsistentReadTimestamp, dbTableName, size));
      instants = instants.stream()
          .filter(inst -> HoodieTimeline.compareTimestamps(inst.getTimestamp(),
              HoodieTimeline.LESSER_THAN_OR_EQUALS, globallyConsistentReadTimestamp))
          .collect(Collectors.toList());
      LOG.info(String.format("filtered out: %d entries from: %s", size - instants.size(),
          dbTableName));
    }
    return instants;
  }
}
