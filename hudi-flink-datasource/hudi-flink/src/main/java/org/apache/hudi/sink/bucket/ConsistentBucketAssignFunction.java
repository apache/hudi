/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.bucket;

import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.ConsistentHashingNode;
import org.apache.hudi.common.model.HoodieConsistentHashingMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.index.bucket.ConsistentBucketIdentifier;
import org.apache.hudi.index.bucket.ConsistentBucketIndexUtils;
import org.apache.hudi.sink.event.CreatePartitionMetadataEvent;
import org.apache.hudi.sink.utils.TimeWait;
import org.apache.hudi.util.FlinkWriteClients;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The function to tag each incoming record with a location of a file based on consistent bucket index.
 */
public class ConsistentBucketAssignFunction extends ProcessFunction<HoodieRecord, HoodieRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(ConsistentBucketAssignFunction.class);

  private final Configuration config;
  private final List<String> indexKeyFields;
  private Map<String, ConsistentBucketIdentifier> partitionToIdentifier;
  private transient HoodieFlinkWriteClient writeClient;
  private transient OperatorEventGateway eventGateway;

  public ConsistentBucketAssignFunction(Configuration conf) {
    this.config = conf;
    this.indexKeyFields = Arrays.asList(OptionsResolver.getIndexKeyField(conf).split(","));
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    try {
      this.writeClient = FlinkWriteClients.createWriteClient(this.config, getRuntimeContext());
      this.partitionToIdentifier = new HashMap<>();
    } catch (Throwable e) {
      LOG.error("Fail to initialize consistent bucket assigner", e);
      throw new RuntimeException(e);
    }
  }

  public void setOperatorEventGateway(OperatorEventGateway operatorEventGateway) {
    this.eventGateway = operatorEventGateway;
  }

  @Override
  public void processElement(HoodieRecord record, Context context, Collector<HoodieRecord> collector) throws Exception {
    final HoodieKey hoodieKey = record.getKey();
    final String partition = hoodieKey.getPartitionPath();

    final ConsistentHashingNode node = getBucketIdentifier(partition).getBucket(hoodieKey, indexKeyFields);
    Preconditions.checkArgument(
        StringUtils.nonEmpty(node.getFileIdPrefix()),
        "Consistent hashing node has no file group, partition: " + partition + ", meta: "
            + partitionToIdentifier.get(partition).getMetadata().getFilename() + ", record_key: " + hoodieKey);

    record.unseal();
    record.setCurrentLocation(new HoodieRecordLocation("U", FSUtils.createNewFileId(node.getFileIdPrefix(), 0)));
    record.seal();
    collector.collect(record);
  }

  private ConsistentBucketIdentifier getBucketIdentifier(String partition) {
    return partitionToIdentifier.computeIfAbsent(partition, p -> {
      TimeWait timeWait = TimeWait.builder().timeout(30000).action("consistent hashing initialize").build();
      Option<HoodieConsistentHashingMetadata> metadataOption = ConsistentBucketIndexUtils.loadMetadata(this.writeClient.getHoodieTable(), p);
      if (!metadataOption.isPresent()) {
        LOG.info("The hashing metadata of the partition {} does not exist, sending the create request to coordinator.", partition);
        this.eventGateway.sendEventToCoordinator(new CreatePartitionMetadataEvent(partition));
      }
      while (!metadataOption.isPresent()) {
        LOG.info("The hashing metadata of the partition {} has not been ready yet.", partition);
        timeWait.waitFor();
        metadataOption = ConsistentBucketIndexUtils.loadMetadata(this.writeClient.getHoodieTable(), p);
      }
      return new ConsistentBucketIdentifier(metadataOption.get());
    });
  }
}
