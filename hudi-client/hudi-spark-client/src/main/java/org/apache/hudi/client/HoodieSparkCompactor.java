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

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;

import java.io.IOException;

public class HoodieSparkCompactor<T extends HoodieRecordPayload> extends AbstractCompactor<T,
    JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> {
  private static final Logger LOG = LogManager.getLogger(HoodieSparkCompactor.class);
  private transient HoodieEngineContext context;

  public HoodieSparkCompactor(AbstractHoodieWriteClient<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> compactionClient,
                              HoodieEngineContext context) {
    super(compactionClient);
    this.context = context;
  }

  @Override
  public void compact(HoodieInstant instant) throws IOException {
    LOG.info("Compactor executing compaction " + instant);
    SparkRDDWriteClient<T> writeClient = (SparkRDDWriteClient<T>) compactionClient;
    JavaRDD<WriteStatus> res = writeClient.compact(instant.getTimestamp());
    this.context.setJobStatus(this.getClass().getSimpleName(), "Collect compaction write status");
    long numWriteErrors = res.collect().stream().filter(WriteStatus::hasErrors).count();
    if (numWriteErrors != 0) {
      // We treat even a single error in compaction as fatal
      LOG.error("Compaction for instant (" + instant + ") failed with write errors. Errors :" + numWriteErrors);
      throw new HoodieException(
          "Compaction for instant (" + instant + ") failed with write errors. Errors :" + numWriteErrors);
    }
    // Commit compaction
    writeClient.commitCompaction(instant.getTimestamp(), res, Option.empty());
  }
}
