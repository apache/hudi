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

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SerializationUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.connect.ControlMessage;
import org.apache.hudi.connect.writers.KafkaConnectConfigs;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.keygen.CustomAvroKeyGenerator;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.storage.StorageConfiguration;

import com.google.protobuf.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;

/**
 * Helper methods for Kafka.
 */
public class KafkaConnectUtils {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaConnectUtils.class);
  private static final String HOODIE_CONF_PREFIX = "hoodie.";
  public static final String HADOOP_CONF_DIR = "HADOOP_CONF_DIR";
  public static final String HADOOP_HOME = "HADOOP_HOME";
  private static final List<Path> DEFAULT_HADOOP_CONF_FILES;

  static {
    DEFAULT_HADOOP_CONF_FILES = new ArrayList<>();
    try {
      String hadoopConfigPath = System.getenv(HADOOP_CONF_DIR);
      String hadoopHomePath = System.getenv(HADOOP_HOME);
      DEFAULT_HADOOP_CONF_FILES.addAll(getHadoopConfigFiles(hadoopConfigPath, hadoopHomePath));
      if (!DEFAULT_HADOOP_CONF_FILES.isEmpty()) {
        LOG.info(String.format("Found Hadoop default config files %s", DEFAULT_HADOOP_CONF_FILES));
      }
    } catch (IOException e) {
      LOG.error("An error occurred while getting the default Hadoop configuration. "
              + "Please use hadoop.conf.dir or hadoop.home to configure Hadoop environment variables", e);
    }
  }

  /**
   * Get hadoop config files by HADOOP_CONF_DIR or HADOOP_HOME
   */
  public static List<Path> getHadoopConfigFiles(String hadoopConfigPath, String hadoopHomePath)
          throws IOException {
    List<Path> hadoopConfigFiles = new ArrayList<>();
    if (!StringUtils.isNullOrEmpty(hadoopConfigPath)) {
      hadoopConfigFiles.addAll(walkTreeForXml(Paths.get(hadoopConfigPath)));
    }
    if (hadoopConfigFiles.isEmpty() && !StringUtils.isNullOrEmpty(hadoopHomePath)) {
      hadoopConfigFiles.addAll(walkTreeForXml(Paths.get(hadoopHomePath, "etc", "hadoop")));
    }
    return hadoopConfigFiles;
  }

  /**
   * Files walk to find xml
   */
  private static List<Path> walkTreeForXml(Path basePath) throws IOException {
    if (Files.notExists(basePath)) {
      return new ArrayList<>();
    }
    return Files.walk(basePath, FileVisitOption.FOLLOW_LINKS)
            .filter(path -> path.toFile().isFile())
            .filter(path -> path.toString().endsWith(".xml"))
            .collect(Collectors.toList());
  }

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
   * @return the default storage configuration.
   */
  public static StorageConfiguration<Configuration> getDefaultStorageConf(KafkaConnectConfigs connectConfigs) {
    Configuration hadoopConf = new Configuration();

    // add hadoop config files
    if (!StringUtils.isNullOrEmpty(connectConfigs.getHadoopConfDir())
            || !StringUtils.isNullOrEmpty(connectConfigs.getHadoopConfHome())) {
      try {
        List<Path> configFiles = getHadoopConfigFiles(connectConfigs.getHadoopConfDir(),
                connectConfigs.getHadoopConfHome());
        configFiles.forEach(f ->
                hadoopConf.addResource(new org.apache.hadoop.fs.Path(f.toAbsolutePath().toUri())));
      } catch (Exception e) {
        throw new HoodieException("Failed to read hadoop configuration!", e);
      }
    } else {
      DEFAULT_HADOOP_CONF_FILES.forEach(f ->
              hadoopConf.addResource(new org.apache.hadoop.fs.Path(f.toAbsolutePath().toUri())));
    }

    connectConfigs.getProps().keySet().stream().filter(prop -> {
      // In order to prevent printing unnecessary warn logs, here filter out the hoodie
      // configuration items before passing to hadoop/hive configs
      return !prop.toString().startsWith(HOODIE_CONF_PREFIX);
    }).forEach(prop -> {
      hadoopConf.set(prop.toString(), connectConfigs.getProps().get(prop.toString()).toString());
    });
    return HadoopFSUtils.getStorageConf(hadoopConf);
  }

  /**
   * Extract the record fields.
   *
   * @param keyGenerator key generator Instance of the keygenerator.
   * @return Returns the record key columns separated by comma.
   */
  public static String getRecordKeyColumns(KeyGenerator keyGenerator) {
    return String.join(",", keyGenerator.getRecordKeyFieldNames());
  }

  /**
   * Extract partition columns directly if an instance of class {@link BaseKeyGenerator},
   * else extract partition columns from the properties.
   *
   * @param keyGenerator    key generator Instance of the keygenerator.
   * @param typedProperties properties from the config.
   * @return partition columns Returns the partition columns separated by comma.
   */
  public static String getPartitionColumns(KeyGenerator keyGenerator, TypedProperties typedProperties) {
    if (keyGenerator instanceof CustomAvroKeyGenerator) {
      return ((BaseKeyGenerator) keyGenerator).getPartitionPathFields().stream().map(
          pathField -> Arrays.stream(pathField.split(CustomAvroKeyGenerator.SPLIT_REGEX))
              .findFirst().orElseGet(() -> "Illegal partition path field format: '$pathField' for ${c.getClass.getSimpleName}"))
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

  public static String hashDigest(String stringToHash) {
    MessageDigest md;
    try {
      md = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      LOG.error("Fatal error selecting hash algorithm", e);
      throw new HoodieException(e);
    }
    byte[] digest = Objects.requireNonNull(md).digest(getUTF8Bytes(stringToHash));
    return StringUtils.toHexString(digest).toUpperCase();
  }

  /**
   * Build Protobuf message containing the Hudi {@link WriteStatus}.
   *
   * @param writeStatuses The list of Hudi {@link WriteStatus}.
   * @return the protobuf message {@link org.apache.hudi.connect.ControlMessage.ConnectWriteStatus}
   * that wraps the Hudi {@link WriteStatus}.
   * @throws IOException thrown if the conversion failed.
   */
  public static ControlMessage.ConnectWriteStatus buildWriteStatuses(List<WriteStatus> writeStatuses) throws IOException {
    return ControlMessage.ConnectWriteStatus.newBuilder()
        .setSerializedWriteStatus(
            ByteString.copyFrom(
                SerializationUtils.serialize(writeStatuses)))
        .build();
  }

  /**
   * Unwrap the Hudi {@link WriteStatus} from the received Protobuf message.
   *
   * @param participantInfo The {@link ControlMessage.ParticipantInfo} that contains the
   *                        underlying {@link WriteStatus} sent by the participants.
   * @return the list of {@link WriteStatus} returned by Hudi on a write transaction.
   */
  public static List<WriteStatus> getWriteStatuses(ControlMessage.ParticipantInfo participantInfo) {
    ControlMessage.ConnectWriteStatus connectWriteStatus = participantInfo.getWriteStatus();
    return SerializationUtils.deserialize(connectWriteStatus.getSerializedWriteStatus().toByteArray());
  }
}
