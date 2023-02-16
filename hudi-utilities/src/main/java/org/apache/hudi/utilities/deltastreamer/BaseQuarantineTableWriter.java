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
import org.apache.hudi.config.HoodieWriteConfig;

import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public abstract class BaseQuarantineTableWriter<T extends QuarantineEvent> {

  String QUARANTINE_PAYLOAD_CLASS = "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload";

  public static String QUARANTINE_TABLE_CURRUPT_RECORD_COL_NAME = "_corrupt_record";

  public BaseQuarantineTableWriter(HoodieDeltaStreamer.Config cfg, SparkSession sparkSession,
                                   TypedProperties props, JavaSparkContext jssc, FileSystem fs) {
  }

  public abstract HoodieWriteConfig getQuarantineTableWriteConfig();

  public abstract HoodieDeltaStreamer.Config getSourceDeltaStreamerConfig();

  /***
   *
   * @param errorEvent
   */
  public abstract void addErrorEvents(JavaRDD<T> errorEvent);

  /***
   *
   * @param baseTableInstantTime
   * @param commitedInstantTime
   * @return
   */
  public abstract Option<JavaRDD<HoodieAvroRecord>> getErrorEvents(String baseTableInstantTime, Option<String> commitedInstantTime);

  public abstract String startCommit();

  /***
   *
   * @param instantTime
   * @param baseTableInstantTime
   * @param commitedInstantTime
   * @return
   */
  public abstract boolean upsertAndCommit(String instantTime, String baseTableInstantTime, Option<String> commitedInstantTime);

}
