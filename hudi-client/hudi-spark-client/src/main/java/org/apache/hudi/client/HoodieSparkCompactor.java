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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;

@Slf4j
public class HoodieSparkCompactor<T> extends BaseCompactor<T,
    JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> {
  private final transient HoodieEngineContext context;

  public HoodieSparkCompactor(BaseHoodieWriteClient<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> compactionClient,
                              HoodieEngineContext context) {
    super(compactionClient);
    this.context = context;
  }

  @Override
  public void compact(String instantTime) {
    log.info("Compactor executing compaction {}", instantTime);
    SparkRDDWriteClient<T> writeClient = (SparkRDDWriteClient<T>) compactionClient;
    HoodieWriteMetadata<JavaRDD<WriteStatus>> compactionMetadata = writeClient.compact(instantTime);
    // Commit compaction
    writeClient.commitCompaction(instantTime, compactionMetadata, Option.empty());
  }
}
