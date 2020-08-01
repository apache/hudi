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

package org.apache.hudi.integ.testsuite;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Extends the {@link HoodieDeltaStreamer} to expose certain operations helpful in running the Test Suite.
 * This is done to achieve 2 things 1) Leverage some components of {@link HoodieDeltaStreamer} 2)
 * Piggyback on the suite to test {@link HoodieDeltaStreamer}
 */
public class HoodieDeltaStreamerWrapper extends HoodieDeltaStreamer {

  public HoodieDeltaStreamerWrapper(Config cfg, JavaSparkContext jssc) throws Exception {
    super(cfg, jssc);
  }

  public HoodieDeltaStreamerWrapper(Config cfg, JavaSparkContext jssc, FileSystem fs, HiveConf conf) throws Exception {
    super(cfg, jssc, fs, conf);
  }

  public JavaRDD<WriteStatus> upsert(Operation operation) throws
      Exception {
    cfg.operation = operation;
    return deltaSyncService.getDeltaSync().syncOnce().getRight();
  }

  public JavaRDD<WriteStatus> insert() throws Exception {
    return upsert(Operation.INSERT);
  }

  public JavaRDD<WriteStatus> bulkInsert() throws
      Exception {
    return upsert(Operation.BULK_INSERT);
  }

  public void scheduleCompact() throws Exception {
    // Since we don't support scheduleCompact() operation in delta-streamer, assume upsert without any data that will
    // trigger scheduling compaction
    upsert(Operation.UPSERT);
  }

  public JavaRDD<WriteStatus> compact() throws Exception {
    // Since we don't support compact() operation in delta-streamer, assume upsert without any data that will trigger
    // inline compaction
    return upsert(Operation.UPSERT);
  }

  public Pair<SchemaProvider, Pair<String, JavaRDD<HoodieRecord>>> fetchSource() throws Exception {
    return deltaSyncService.getDeltaSync().readFromSource(deltaSyncService.getDeltaSync().getCommitTimelineOpt());
  }

}
