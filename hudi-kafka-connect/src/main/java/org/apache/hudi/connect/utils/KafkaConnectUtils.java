package org.apache.hudi.connect.utils;

import org.apache.hudi.exception.HoodieException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 * Helper methods for Kafka.
 */
public class KafkaConnectUtils {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaConnectUtils.class);

  public static int getLatestNumPartitions(String bootstrapServers, String topicName) {
    Properties props = new Properties();
    props.put("bootstrap.servers", bootstrapServers);
    try {
      AdminClient client = AdminClient.create(props);
      DescribeTopicsResult result = client.describeTopics(Arrays.asList(topicName));
      Map<String, KafkaFuture<TopicDescription>> values = result.values();
      KafkaFuture<TopicDescription> topicDescription = values.get(topicName);
      int numPartitions = topicDescription.get().partitions().size();
      LOG.info("Latest number of partitions for topic {} is {}", topicName, numPartitions);
      return numPartitions;
    } catch (Exception exception) {
      throw new HoodieException("Fatal error fetching the latest partition of kafka topic name" + topicName, exception);
    }
  }
}
