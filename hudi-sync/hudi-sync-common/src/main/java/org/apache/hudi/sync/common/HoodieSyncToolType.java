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

package org.apache.hudi.sync.common;

import org.apache.hudi.common.config.EnumDescription;
import org.apache.hudi.common.config.EnumFieldDescription;

@EnumDescription("Sync Hudi with catalogs.")
public enum HoodieSyncToolType {

  @EnumFieldDescription("???")
  HIVE_GLOBAL("org.apache.hudi.hive.replication.GlobalHiveSyncTool"),

  @EnumFieldDescription("Sync a Hudi HDFS table with hive metastore.")
  HIVE("org.apache.hudi.hive.HiveSyncTool"),

  @EnumFieldDescription("Sync a Hudi table with a big query table.")
  GCP_BIG_QUERY("org.apache.hudi.gcp.bigquery.BigQuerySyncTool"),

  @EnumFieldDescription("Sync to a DataHub dataset.")
  DATAHUB("org.apache.hudi.sync.datahub.DataHubSyncTool"),

  @EnumFieldDescription("Currently Experimental. Implements syncing a Hudi Table with the AWS Glue Data Catalog.")
  AWS_GLUE("org.apache.hudi.aws.sync.AwsGlueCatalogSyncTool"),

  @EnumFieldDescription("Mainly used to sync hoodie tables to Alibaba Cloud AnalyticDB.")
  ADB("org.apache.hudi.sync.adb.AdbSyncTool"),


  @EnumFieldDescription("Uses the metastore sync tool set in `hoodie.meta.sync.client.tool.class`")
  CUSTOM("");

  public final String classPath;

  HoodieSyncToolType(String classPath) {
    this.classPath = classPath;
  }
}
