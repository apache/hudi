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

package org.apache.hudi.spark3.internal;

import org.apache.hudi.config.HoodieWriteConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;

/**
 * Implementation of {@link WriteBuilder} for datasource "hudi.spark3.internal" to be used in datasource implementation
 * of bulk insert.
 */
public class HoodieDataSourceInternalBatchWriteBuilder implements WriteBuilder {

  private final String instantTime;
  private final HoodieWriteConfig writeConfig;
  private final StructType structType;
  private final SparkSession jss;
  private final Configuration hadoopConfiguration;

  public HoodieDataSourceInternalBatchWriteBuilder(String instantTime, HoodieWriteConfig writeConfig, StructType structType,
      SparkSession jss, Configuration hadoopConfiguration) {
    this.instantTime = instantTime;
    this.writeConfig = writeConfig;
    this.structType = structType;
    this.jss = jss;
    this.hadoopConfiguration = hadoopConfiguration;
  }

  @Override
  public BatchWrite buildForBatch() {
    return new HoodieDataSourceInternalBatchWrite(instantTime, writeConfig, structType, jss,
        hadoopConfiguration);
  }
}
