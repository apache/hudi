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

package org.apache.hudi.sink.bucket;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.ConsistentHashingNode;
import org.apache.hudi.common.model.HoodieConsistentHashingMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.index.bucket.ConsistentBucketIdentifier;
import org.apache.hudi.index.bucket.ConsistentBucketIndexUtils;
import org.apache.hudi.sink.StreamWriteFunction;
import org.apache.hudi.sink.utils.TimeWait;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A stream write function with consistent hashing bucket index.
 *
 * @param <I> the input type
 */
public class ConsistentBucketStreamWriteFunction<I> extends StreamWriteFunction<I> {

  private static final Logger LOG = LoggerFactory.getLogger(ConsistentBucketStreamWriteFunction.class);

  private List<String> indexKeyFields;
  private Map<String, ConsistentBucketIdentifier> partitionToIdentifier;
  /**
   * Cache mapping between node -> location, to avoid repeated creation of the same location objects.
   */
  private Map<ConsistentHashingNode, HoodieRecordLocation> nodeToRecordLocation;

  /**
   * Constructs a StreamingSinkFunction.
   *
   * @param config The config options
   */
  public ConsistentBucketStreamWriteFunction(Configuration config) {
    super(config);
  }

  @Override
  public void open(Configuration parameters) throws IOException {
    super.open(parameters);
    this.indexKeyFields = Arrays.asList(config.getString(FlinkOptions.INDEX_KEY_FIELD).split(","));

    this.partitionToIdentifier = new HashMap<>();
    this.nodeToRecordLocation = new HashMap<>();
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    super.initializeState(context);
  }

  @Override
  public void snapshotState() {
    super.snapshotState();
  }

  @Override
  public void processElement(I value, ProcessFunction<I, Object>.Context ctx, Collector<Object> out) throws Exception {
    HoodieRecord<?> record = (HoodieRecord<?>) value;
    final HoodieKey hoodieKey = record.getKey();
    final String partition = hoodieKey.getPartitionPath();

    ConsistentHashingNode node = getBucketIdentifier(partition).getBucket(hoodieKey, indexKeyFields);
    Preconditions.checkArgument(!StringUtils.isNullOrEmpty(node.getFileIdPrefix()),
        "Consistent hashing node has no file group, partition: " + partition + ", meta: "
            + partitionToIdentifier.get(partition).getMetadata().getFilename() + ", record_key: " + hoodieKey);

    record.unseal();
    record.setCurrentLocation(nodeToRecordLocation.computeIfAbsent(node,
        n -> new HoodieRecordLocation("U", FSUtils.createNewFileId(n.getFileIdPrefix(), 0))));
    record.seal();
    bufferRecord(record);
  }

  private ConsistentBucketIdentifier getBucketIdentifier(String partition) {
    return partitionToIdentifier.computeIfAbsent(partition, p -> {
      TimeWait timeWait = TimeWait.builder().timeout(30000).action("consistent hashing initialize").build();
      Option<HoodieConsistentHashingMetadata> metadataOption = ConsistentBucketIndexUtils.loadMetadata(this.metaClient, p);
      while (!metadataOption.isPresent()) {
        timeWait.waitFor();
        metadataOption = ConsistentBucketIndexUtils.loadMetadata(this.metaClient, p);
      }
      return new ConsistentBucketIdentifier(metadataOption.get());
    });
  }

}
