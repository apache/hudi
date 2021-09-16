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

package org.apache.hudi.connect.utils;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.keygen.CustomAvroKeyGenerator;
import org.apache.hudi.keygen.CustomKeyGenerator;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Helper methods for Kafka.
 */
public class KafkaConnectUtils {

  private static final Logger LOG = LogManager.getLogger(KafkaConnectUtils.class);

  public static int getLatestNumPartitions(String bootstrapServers, String topicName) {
    Properties props = new Properties();
    props.put("bootstrap.servers", bootstrapServers);
    try {
      AdminClient client = AdminClient.create(props);
      DescribeTopicsResult result = client.describeTopics(Arrays.asList(topicName));
      Map<String, KafkaFuture<TopicDescription>> values = result.values();
      KafkaFuture<TopicDescription> topicDescription = values.get(topicName);
      int numPartitions = topicDescription.get().partitions().size();
      LOG.info(String.format("Latest number of partitions for topic %s is %s", topicName, numPartitions));
      return numPartitions;
    } catch (Exception exception) {
      throw new HoodieException("Fatal error fetching the latest partition of kafka topic name" + topicName, exception);
    }
  }

  /**
   * Returns the default Hadoop Configuration.
   * @return
   */
  public static Configuration getDefaultHadoopConf() {
    Configuration hadoopConf = new Configuration();
    hadoopConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    return hadoopConf;
  }

  /**
   * Extract the record fields.
   * @param keyGenerator key generator Instance of the keygenerator.
   * @return Returns the record key columns seprarated by comma.
   */
  public static String getRecordKeyColumns(KeyGenerator keyGenerator) {
    return String.join(",", keyGenerator.getRecordKeyFieldNames());
  }

  /**
   * Extract partition columns directly if an instance of class {@link BaseKeyGenerator},
   * else extract partition columns from the properties.
   *
   * @param keyGenerator key generator Instance of the keygenerator.
   * @param typedProperties properties from the config.
   * @return partition columns Returns the partition columns seprarated by comma.
   */
  public static String getPartitionColumns(KeyGenerator keyGenerator, TypedProperties typedProperties) {

    if (keyGenerator instanceof CustomKeyGenerator || keyGenerator instanceof CustomAvroKeyGenerator) {
      return ((BaseKeyGenerator) keyGenerator).getPartitionPathFields().stream().map(
          pathField -> Arrays.stream(pathField.split(CustomAvroKeyGenerator.SPLIT_REGEX))
              .findFirst().orElse("Illegal partition path field format: '$pathField' for ${c.getClass.getSimpleName}"))
          .collect(Collectors.joining(","));
    }

    if (keyGenerator instanceof BaseKeyGenerator) {
      return String.join(",", ((BaseKeyGenerator) keyGenerator).getPartitionPathFields());
    }

    return typedProperties.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key());
  }


  /**
   * Get the Metadata from the latest commit file.
   *
   * @param metaClient The {@link HoodieTableMetaClient} to get access to the meta data.
   * @return An Optional {@link HoodieCommitMetadata} containing the meta data from the latest commit file.
   */
  public static Option<HoodieCommitMetadata> getCommitMetadataForLatestInstant(HoodieTableMetaClient metaClient) {
    HoodieTimeline timeline = metaClient.getActiveTimeline().getCommitsTimeline()
        .filterCompletedInstants()
        .filter(instant -> (metaClient.getTableType() == HoodieTableType.COPY_ON_WRITE && instant.getAction().equals(HoodieActiveTimeline.COMMIT_ACTION))
            || (metaClient.getTableType() == HoodieTableType.MERGE_ON_READ && instant.getAction().equals(HoodieActiveTimeline.DELTA_COMMIT_ACTION))
        );
    Option<HoodieInstant> latestInstant = timeline.lastInstant();
    if (latestInstant.isPresent()) {
      try {
        byte[] data = timeline.getInstantDetails(latestInstant.get()).get();
        return Option.of(HoodieCommitMetadata.fromBytes(data, HoodieCommitMetadata.class));
      } catch (Exception e) {
        throw new HoodieException("Failed to read schema from commit metadata", e);
      }
    } else {
      return Option.empty();
    }
  }
}
