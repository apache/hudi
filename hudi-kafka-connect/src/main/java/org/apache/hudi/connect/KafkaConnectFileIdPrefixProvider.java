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

import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.table.FileIdPrefixProvider;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import java.util.Properties;

public class KafkaConnectFileIdPrefixProvider extends FileIdPrefixProvider {

  public static final String KAFKA_CONNECT_PARTITION_ID = "hudi.kafka.connect.partition";
  private static final Logger LOG = LogManager.getLogger(KafkaConnectFileIdPrefixProvider.class);

  private final String kafkaPartition;

  public KafkaConnectFileIdPrefixProvider(Properties props) {
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
    MessageDigest md;
    try {
      md = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      LOG.error("Fatal error selecting hash algorithm", e);
      throw new HoodieException(e);
    }

    byte[] digest = Objects.requireNonNull(md).digest(rawFileIdPrefix.getBytes(StandardCharsets.UTF_8));

    LOG.info("CreateFileId for Kafka Partition " + kafkaPartition + " : " + partitionPath + " = " + rawFileIdPrefix
        + " === " + StringUtils.toHexString(digest).toUpperCase());
    return StringUtils.toHexString(digest).toUpperCase();
  }
}
