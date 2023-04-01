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

package org.apache.hudi.connect;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.connect.utils.KafkaConnectUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.table.FileIdPrefixProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConnectFileIdPrefixProvider extends FileIdPrefixProvider {

  public static final String KAFKA_CONNECT_PARTITION_ID = "hudi.kafka.connect.partition";
  private static final Logger LOG = LoggerFactory.getLogger(KafkaConnectFileIdPrefixProvider.class);

  private final String kafkaPartition;

  public KafkaConnectFileIdPrefixProvider(TypedProperties props) {
    super(props);
    if (!props.containsKey(KAFKA_CONNECT_PARTITION_ID)) {
      LOG.error("Fatal error due to Kafka Connect Partition Id is not set");
      throw new HoodieException("Kafka Connect Partition Key " + KAFKA_CONNECT_PARTITION_ID + " not provided");
    }
    this.kafkaPartition = props.getProperty(KAFKA_CONNECT_PARTITION_ID);
  }

  @Override
  public String createFilePrefix(String partitionPath) {
    // We use a combination of kafka partition and partition path as the file id, and then hash it
    // to generate a fixed sized hash.
    String rawFileIdPrefix = kafkaPartition + partitionPath;
    String hashedPrefix = KafkaConnectUtils.hashDigest(rawFileIdPrefix);
    LOG.info("CreateFileId for Kafka Partition " + kafkaPartition + " : " + partitionPath + " = " + rawFileIdPrefix
        + " === " + hashedPrefix);
    return hashedPrefix;
  }
}
