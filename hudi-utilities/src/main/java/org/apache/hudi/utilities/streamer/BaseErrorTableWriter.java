/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities.streamer;

import org.apache.hudi.ApiMaturityLevel;
import org.apache.hudi.PublicAPIClass;
import org.apache.hudi.PublicAPIMethod;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;

import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

/**
 * The class which handles error events while processing write records. All the
 * records which have a processing/write failure are triggered as error events to
 * BaseErrorTableWriter. The implementation of BaseErrorTableWriter processes
 * these error events through addErrorEvents API and commits them to the error table when
 * upsertAndCommit API is called.
 *
 * The writer can use the configs defined in HoodieErrorTableConfig to manage the error table.
 */
@PublicAPIClass(maturity = ApiMaturityLevel.EVOLVING)
public abstract class BaseErrorTableWriter<T extends ErrorEvent> implements Serializable {

  // The column name passed to Spark for option `columnNameOfCorruptRecord`. The record
  // is set to this column in case of an error
  public static String ERROR_TABLE_CURRUPT_RECORD_COL_NAME = "_corrupt_record";

  public BaseErrorTableWriter(HoodieStreamer.Config cfg, SparkSession sparkSession,
                              TypedProperties props, HoodieSparkEngineContext hoodieSparkContext, FileSystem fileSystem) {
  }

  public BaseErrorTableWriter(HoodieStreamer.Config cfg,
                              SparkSession sparkSession,
                              TypedProperties props,
                              HoodieSparkEngineContext hoodieSparkContext,
                              FileSystem fs,
                              Option<HoodieIngestionMetrics> metrics) {
  }

  /**
   * Processes input error events. These error events would be committed later through upsertAndCommit
   * API call.
   *
   * @param errorEvent Input error event RDD
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract void addErrorEvents(JavaRDD<T> errorEvent);

  /**
   * Fetches the error events RDD processed by the writer so far. This is a test API.
   */
  @VisibleForTesting
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract Option<JavaRDD<HoodieAvroIndexedRecord>> getErrorEvents(String baseTableInstantTime, Option<String> commitedInstantTime);

  /**
   * This API is called to commit the error events (failed Hoodie Records) processed by the writer so far.
   * These records are committed to a error table.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public abstract boolean upsertAndCommit(String baseTableInstantTime, Option<String> commitedInstantTime);

  public abstract JavaRDD<WriteStatus> upsert(String baseTableInstantTime, Option<String> commitedInstantTime);

  public abstract boolean commit(String errorTableInstantTime, JavaRDD<WriteStatus> writeStatuses);
}
