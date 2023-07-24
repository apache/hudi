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

package org.apache.hudi.sink.bucket;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.StreamWriteFunction;
import org.apache.hudi.sink.clustering.update.strategy.FlinkConsistentBucketUpdateStrategy;

import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A stream write function with consistent bucket hash index.
 *
 * @param <I> the input type
 */
public class ConsistentBucketStreamWriteFunction<I> extends StreamWriteFunction<I> {

  private static final Logger LOG = LoggerFactory.getLogger(ConsistentBucketStreamWriteFunction.class);

  private transient FlinkConsistentBucketUpdateStrategy updateStrategy;

  /**
   * Constructs a ConsistentBucketStreamWriteFunction.
   *
   * @param config The config options
   */
  public ConsistentBucketStreamWriteFunction(Configuration config) {
    super(config);
  }

  @Override
  public void open(Configuration parameters) throws IOException {
    super.open(parameters);
    List<String> indexKeyFields = Arrays.asList(config.getString(FlinkOptions.INDEX_KEY_FIELD).split(","));
    this.updateStrategy = new FlinkConsistentBucketUpdateStrategy(this.writeClient, indexKeyFields);
  }

  @Override
  public void snapshotState() {
    super.snapshotState();
    updateStrategy.reset();
  }

  @Override
  protected List<WriteStatus> writeBucket(String instant, DataBucket bucket, List<HoodieRecord> records) {
    updateStrategy.initialize(this.writeClient);
    bucket.preWrite(records);
    Pair<List<Pair<List<HoodieRecord>, String>>, Set<HoodieFileGroupId>> recordListFgPair =
        updateStrategy.handleUpdate(Collections.singletonList(Pair.of(records, instant)));
    return recordListFgPair.getKey().stream().flatMap(
        recordsInstantPair -> writeFunction.apply(recordsInstantPair.getLeft(), recordsInstantPair.getRight()).stream()
    ).collect(Collectors.toList());
  }
}
