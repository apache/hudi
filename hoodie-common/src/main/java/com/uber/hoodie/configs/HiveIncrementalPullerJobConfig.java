/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.configs;

import com.beust.jcommander.Parameter;

public class HiveIncrementalPullerJobConfig extends AbstractJobConfig {

  @Parameter(names = {"--hiveUrl"})
  public String hiveJDBCUrl = "jdbc:hive2://localhost:10014/;transportMode=http;httpPath=hs2";
  @Parameter(names = {"--hiveUser"})
  public String hiveUsername = "hive";
  @Parameter(names = {"--hivePass"})
  public String hivePassword = "";
  @Parameter(names = {"--queue"})
  public String yarnQueueName = "hadoop-queue";
  @Parameter(names = {"--tmp"})
  public String hoodieTmpDir = "/app/hoodie/intermediate";
  @Parameter(names = {"--extractSQLFile"}, required = true)
  public String incrementalSQLFile;
  @Parameter(names = {"--sourceDb"}, required = true)
  public String sourceDb;
  @Parameter(names = {"--sourceTable"}, required = true)
  public String sourceTable;
  @Parameter(names = {"--targetDb"})
  public String targetDb;
  @Parameter(names = {"--targetTable"}, required = true)
  public String targetTable;
  @Parameter(names = {"--tmpdb"})
  public String tmpDb = "tmp";
  @Parameter(names = {"--fromCommitTime"})
  public String fromCommitTime;
  @Parameter(names = {"--maxCommits"})
  public int maxCommits = 3;
}
