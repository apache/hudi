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

package org.apache.hudi.hive;

import com.beust.jcommander.Parameter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Configs needed to sync data into Hive.
 */
public class HiveSyncConfig implements Serializable {

  @Parameter(names = {"--database"}, description = "name of the target database in Hive", required = true)
  public String databaseName;

  @Parameter(names = {"--table"}, description = "name of the target table in Hive", required = true)
  public String tableName;

  @Parameter(names = {"--user"}, description = "Hive username", required = true)
  public String hiveUser;

  @Parameter(names = {"--pass"}, description = "Hive password", required = true)
  public String hivePass;

  @Parameter(names = {"--jdbc-url"}, description = "Hive jdbc connect url", required = true)
  public String jdbcUrl;

  @Parameter(names = {"--base-path"}, description = "Basepath of hoodie table to sync", required = true)
  public String basePath;

  @Parameter(names = "--partitioned-by", description = "Fields in the schema partitioned by")
  public List<String> partitionFields = new ArrayList<>();

  @Parameter(names = "--partition-value-extractor", description = "Class which implements PartitionValueExtractor "
      + "to extract the partition values from HDFS path")
  public String partitionValueExtractorClass = SlashEncodedDayPartitionValueExtractor.class.getName();

  @Parameter(names = {"--assume-date-partitioning"}, description = "Assume standard yyyy/mm/dd partitioning, this"
      + " exists to support backward compatibility. If you use hoodie 0.3.x, do not set this parameter")
  public Boolean assumeDatePartitioning = false;

  @Parameter(names = {"--use-pre-apache-input-format"},
      description = "Use InputFormat under com.uber.hoodie package "
          + "instead of org.apache.hudi package. Use this when you are in the process of migrating from "
          + "com.uber.hoodie to org.apache.hudi. Stop using this after you migrated the table definition to "
          + "org.apache.hudi input format.")
  public Boolean usePreApacheInputFormat = false;

  @Parameter(names = {"--use-jdbc"}, description = "Hive jdbc connect url")
  public Boolean useJdbc = true;

  @Parameter(names = {"--skip-ro-suffix"}, description = "Skip the `_ro` suffix for Read optimized table, when registering")
  public Boolean skipROSuffix = false;

  @Parameter(names = {"--help", "-h"}, help = true)
  public Boolean help = false;

  public static HiveSyncConfig copy(HiveSyncConfig cfg) {
    HiveSyncConfig newConfig = new HiveSyncConfig();
    newConfig.basePath = cfg.basePath;
    newConfig.assumeDatePartitioning = cfg.assumeDatePartitioning;
    newConfig.databaseName = cfg.databaseName;
    newConfig.hivePass = cfg.hivePass;
    newConfig.hiveUser = cfg.hiveUser;
    newConfig.partitionFields = cfg.partitionFields;
    newConfig.partitionValueExtractorClass = cfg.partitionValueExtractorClass;
    newConfig.jdbcUrl = cfg.jdbcUrl;
    newConfig.tableName = cfg.tableName;
    newConfig.usePreApacheInputFormat = cfg.usePreApacheInputFormat;
    return newConfig;
  }

  @Override
  public String toString() {
    return "HiveSyncConfig{databaseName='" + databaseName + '\'' + ", tableName='" + tableName + '\''
        + ", hiveUser='" + hiveUser + '\'' + ", hivePass='" + hivePass + '\'' + ", jdbcUrl='" + jdbcUrl + '\''
        + ", basePath='" + basePath + '\'' + ", partitionFields=" + partitionFields + ", partitionValueExtractorClass='"
        + partitionValueExtractorClass + '\'' + ", assumeDatePartitioning=" + assumeDatePartitioning
        + ", usePreApacheInputFormat=" + usePreApacheInputFormat + ", useJdbc=" + useJdbc + ", help=" + help + '}';
  }
}
