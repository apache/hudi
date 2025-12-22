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

package org.apache.hudi.internal;

import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.StorageConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;

/**
 * Base class for DefaultSource used by Spark datasource v2.
 */
public class BaseDefaultSource {

  protected SparkSession sparkSession = null;
  protected StorageConfiguration<Configuration> configuration = null;

  protected SparkSession getSparkSession() {
    if (sparkSession == null) {
      sparkSession = SparkSession.builder().getOrCreate();
    }
    return sparkSession;
  }

  protected StorageConfiguration<Configuration> getConfiguration() {
    if (configuration == null) {
      this.configuration = HadoopFSUtils.getStorageConf(
          getSparkSession().sparkContext().hadoopConfiguration());
    }
    return configuration;
  }
}
