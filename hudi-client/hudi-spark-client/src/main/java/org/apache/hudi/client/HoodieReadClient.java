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

package org.apache.hudi.client;

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;

import org.apache.spark.sql.SQLContext;

/**
 * Provides an RDD based API for accessing/filtering Hoodie tables, based on keys.
 *
 * @deprecated This. Use {@link SparkRDDReadClient instead.}
 */
@Deprecated
public class HoodieReadClient<T extends HoodieRecordPayload<T>> extends SparkRDDReadClient<T> {

  public HoodieReadClient(HoodieSparkEngineContext context, String basePath) {
    super(context, basePath);
  }

  public HoodieReadClient(HoodieSparkEngineContext context, String basePath, SQLContext sqlContext) {
    super(context, basePath, sqlContext);
  }

  public HoodieReadClient(HoodieSparkEngineContext context, String basePath, SQLContext sqlContext, HoodieIndex.IndexType indexType) {
    super(context, basePath, sqlContext, indexType);
  }

  public HoodieReadClient(HoodieSparkEngineContext context, HoodieWriteConfig clientConfig) {
    super(context, clientConfig);
  }
}
