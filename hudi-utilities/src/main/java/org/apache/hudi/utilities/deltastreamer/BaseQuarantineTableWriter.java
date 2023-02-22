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

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.VisibleForTesting;

import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * The class which handles error events when delta streamer syncs data from source. All the
 * records which Delta streamer is not able to process are triggered as error events to
 * BaseQuarantineTableWriter. The implementation of BaseQuarantineTableWriter processes
 * these error events through addErrorEvents API and commits them to the quarantine table when
 * upsertAndCommit API is called.
 *
 * The writer can use the configs defined in HoodieQuarantineTableConfig to manage the quarantine table.
 */
public abstract class BaseQuarantineTableWriter<T extends QuarantineEvent> {

  // The column name passed to Spark for option `columnNameOfCorruptRecord`. The record
  // is set to this column in case of an error
  public static String QUARANTINE_TABLE_CURRUPT_RECORD_COL_NAME = "_corrupt_record";

  public BaseQuarantineTableWriter(HoodieDeltaStreamer.Config cfg, SparkSession sparkSession,
                                   TypedProperties props, JavaSparkContext jssc, FileSystem fs) {
  }

  /**
   * Processes input error events. These error events would be committed later through upsertAndCommit
   * API call.
   *
   * @param errorEvent Input error event RDD
   */
  public abstract void addErrorEvents(JavaRDD<T> errorEvent);

  /**
   * Fetches the error events RDD processed by the writer so far. This is a test API.
   */
  @VisibleForTesting
  public abstract Option<JavaRDD<HoodieAvroRecord>> getErrorEvents(String baseTableInstantTime, Option<String> commitedInstantTime);

  /**
   * This API is called to commit the error events (failed Hoodie Records) processed by the writer so far.
   * These records are committed to a quarantine table.
   */
  public abstract boolean upsertAndCommit(String baseTableInstantTime, Option<String> commitedInstantTime);

}
