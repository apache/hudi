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

import org.apache.hudi.HoodieWriteClient;
import org.apache.hudi.WriteStatus;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.io.Serializable;

/**
 * Run one round of compaction.
 */
public class Compactor implements Serializable {

  protected static volatile Logger log = LogManager.getLogger(Compactor.class);

  private transient HoodieWriteClient compactionClient;
  private transient JavaSparkContext jssc;

  public Compactor(HoodieWriteClient compactionClient, JavaSparkContext jssc) {
    this.jssc = jssc;
    this.compactionClient = compactionClient;
  }

  public void compact(HoodieInstant instant) throws IOException {
    log.info("Compactor executing compaction " + instant);
    JavaRDD<WriteStatus> res = compactionClient.compact(instant.getTimestamp());
    long numWriteErrors = res.collect().stream().filter(r -> r.hasErrors()).count();
    if (numWriteErrors != 0) {
      // We treat even a single error in compaction as fatal
      log.error("Compaction for instant (" + instant + ") failed with write errors. " + "Errors :" + numWriteErrors);
      throw new HoodieException(
          "Compaction for instant (" + instant + ") failed with write errors. " + "Errors :" + numWriteErrors);
    }
    // Commit compaction
    compactionClient.commitCompaction(instant.getTimestamp(), res, Option.empty());
  }
}
