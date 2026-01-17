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

package org.apache.hudi.utilities.sources;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV2;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.exception.HoodieReadFromSourceException;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.gcs.MessageBatch;
import org.apache.hudi.utilities.sources.helpers.gcs.MessageValidity;
import org.apache.hudi.utilities.sources.helpers.gcs.MetadataMessage;
import org.apache.hudi.utilities.sources.helpers.gcs.PubsubMessagesFetcher;

import com.google.pubsub.v1.ReceivedMessage;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hudi.common.util.ConfigUtils.getBooleanWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.getIntWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;
import static org.apache.hudi.utilities.config.CloudSourceConfig.ACK_MESSAGES;
import static org.apache.hudi.utilities.config.CloudSourceConfig.BATCH_SIZE_CONF;
import static org.apache.hudi.utilities.config.CloudSourceConfig.MAX_FETCH_TIME_PER_SYNC_SECS;
import static org.apache.hudi.utilities.config.CloudSourceConfig.MAX_NUM_MESSAGES_PER_SYNC;
import static org.apache.hudi.utilities.config.CloudSourceConfig.META_EVENTS_PER_PARTITION;
import static org.apache.hudi.utilities.config.GCSEventsSourceConfig.GOOGLE_PROJECT_ID;
import static org.apache.hudi.utilities.config.GCSEventsSourceConfig.PUBSUB_SUBSCRIPTION_ID;
import static org.apache.hudi.utilities.sources.helpers.gcs.MessageValidity.ProcessingDecision.DO_SKIP;

/*
 * An incremental source to fetch from a Google Cloud Pubsub topic (a subscription, to be precise),
 * and download them into a Hudi table. The messages are assumed to be of type Cloud Storage Pubsub Notification.
 *
 * You should set spark.driver.extraClassPath in spark-defaults.conf to
 * look like below WITHOUT THE NEWLINES (or give the equivalent as CLI options if in cluster mode):
 * (mysql-connector at the end is only needed if Hive Sync is enabled and Mysql is used for Hive Metastore).

 absolute_path_to/protobuf-java-3.21.1.jar:absolute_path_to/failureaccess-1.0.1.jar:
 absolute_path_to/31.1-jre/guava-31.1-jre.jar:
 absolute_path_to/mysql-connector-java-8.0.30.jar

This class can be invoked via spark-submit as follows. There's a bunch of optional hive sync flags at the end:
$ bin/spark-submit \
--driver-memory 4g \
--executor-memory 4g \
--packages com.google.cloud:google-cloud-pubsub:1.120.0 \
--class org.apache.hudi.utilities.streamer.HoodieStreamer \
absolute_path_to/hudi-utilities-bundle_2.12-0.13.0-SNAPSHOT.jar \
--source-class org.apache.hudi.utilities.sources.GcsEventsSource \
--op INSERT \
--hoodie-conf hoodie.datasource.write.recordkey.field="id" \
--source-ordering-field timeCreated \
--hoodie-conf hoodie.index.type=GLOBAL_BLOOM \
--filter-dupes \
--allow-commit-on-no-checkpoint-change \
--hoodie-conf hoodie.datasource.write.insert.drop.duplicates=true \
--hoodie-conf hoodie.combine.before.insert=true \
--hoodie-conf hoodie.datasource.write.partitionpath.field=bucket \
--hoodie-conf hoodie.streamer.source.gcs.project.id=infra-dev-358110 \
--hoodie-conf hoodie.streamer.source.gcs.subscription.id=gcs-obj-8-sub-1 \
--hoodie-conf hoodie.streamer.source.cloud.meta.ack=true \
--table-type COPY_ON_WRITE \
--target-base-path file:\/\/\/absolute_path_to/meta-gcs \
--target-table gcs_meta \
--continuous \
--source-limit 100 \
--min-sync-interval-seconds 100 \
--enable-hive-sync \
--hoodie-conf hoodie.datasource.hive_sync.partition_extractor_class=org.apache.hudi.hive.MultiPartKeysValueExtractor \
--hoodie-conf hoodie.datasource.write.hive_style_partitioning=true \
--hoodie-conf hoodie.datasource.hive_sync.database=default \
--hoodie-conf hoodie.datasource.hive_sync.table=gcs_meta \
--hoodie-conf hoodie.datasource.hive_sync.partition_fields=bucket \
*/
public class GcsEventsSource extends RowSource {

  private final PubsubMessagesFetcher pubsubMessagesFetcher;
  private final SchemaProvider schemaProvider;
  private final boolean ackMessages;
  private final int recordsPerPartition;

  private final List<String> messagesToAck = new ArrayList<>();

  private static final Checkpoint CHECKPOINT_VALUE_ZERO = new StreamerCheckpointV2("0");

  private static final Logger LOG = LoggerFactory.getLogger(GcsEventsSource.class);

  public GcsEventsSource(TypedProperties props, JavaSparkContext jsc, SparkSession spark,
                         SchemaProvider schemaProvider) {
    this(
            props, jsc, spark, schemaProvider,
            new PubsubMessagesFetcher(
                getStringWithAltKeys(props, GOOGLE_PROJECT_ID),
                getStringWithAltKeys(props, PUBSUB_SUBSCRIPTION_ID),
                getIntWithAltKeys(props, BATCH_SIZE_CONF),
                getIntWithAltKeys(props, MAX_NUM_MESSAGES_PER_SYNC),
                getIntWithAltKeys(props, MAX_FETCH_TIME_PER_SYNC_SECS))
    );
  }

  public GcsEventsSource(TypedProperties props, JavaSparkContext jsc, SparkSession spark,
                         SchemaProvider schemaProvider, PubsubMessagesFetcher pubsubMessagesFetcher) {
    super(props, jsc, spark, schemaProvider);

    this.pubsubMessagesFetcher = pubsubMessagesFetcher;
    this.ackMessages = getBooleanWithAltKeys(props, ACK_MESSAGES);
    this.schemaProvider = schemaProvider;
    this.recordsPerPartition = getIntWithAltKeys(props, META_EVENTS_PER_PARTITION);

    LOG.info("Created GcsEventsSource");
  }

  @Override
  protected Pair<Option<Dataset<Row>>, Checkpoint> fetchNextBatch(Option<Checkpoint> lastCheckpoint, long sourceLimit) {
    LOG.info("fetchNextBatch(): Input checkpoint: " + lastCheckpoint);
    MessageBatch messageBatch;
    try {
      messageBatch = fetchFileMetadata();
    } catch (HoodieException e) {
      throw e;
    } catch (Exception e) {
      throw new HoodieReadFromSourceException("Failed to fetch file metadata from GCS events source", e);
    }

    if (messageBatch.isEmpty()) {
      LOG.info("No new data. Returning empty batch with checkpoint value: " + CHECKPOINT_VALUE_ZERO);
      return Pair.of(Option.empty(), CHECKPOINT_VALUE_ZERO);
    }

    int numPartitions = (int) Math.ceil(
        (double) messageBatch.getMessages().size() / recordsPerPartition);
    Dataset<String> eventRecords = sparkSession.createDataset(messageBatch.getMessages(), Encoders.STRING()).repartition(numPartitions);

    LOG.info("Returning checkpoint value: " + CHECKPOINT_VALUE_ZERO);

    StructType sourceSchema = UtilHelpers.getSourceSchema(schemaProvider);
    if (sourceSchema != null) {
      return Pair.of(Option.of(sparkSession.read().schema(sourceSchema).json(eventRecords)), CHECKPOINT_VALUE_ZERO);
    } else {
      return Pair.of(Option.of(sparkSession.read().json(eventRecords)), CHECKPOINT_VALUE_ZERO);
    }
  }

  @Override
  public void onCommit(String lastCkptStr) {
    LOG.info("onCommit(): Checkpoint: " + lastCkptStr);

    if (ackMessages) {
      ackOutstandingMessages();
    } else {
      LOG.warn("Not acknowledging messages. Can result in repeated redeliveries.");
    }
  }

  MessageBatch fetchFileMetadata() {
    List<ReceivedMessage> receivedMessages = pubsubMessagesFetcher.fetchMessages();
    return processMessages(receivedMessages);
  }

  /**
   * Convert Pubsub messages into a batch of GCS file MetadataMsg objects, skipping those that
   * don't need to be processed.
   *
   * @param receivedMessages Pubsub messages
   * @return A batch of GCS file metadata messages
   */
  private MessageBatch processMessages(List<ReceivedMessage> receivedMessages) {
    List<String> messages = new ArrayList<>();
    long skippedMsgCount = 0;

    for (ReceivedMessage received : receivedMessages) {
      MetadataMessage message = new MetadataMessage(received.getMessage());
      String msgStr = message.toStringUtf8();

      logDetails(message, msgStr);

      messagesToAck.add(received.getAckId());

      MessageValidity messageValidity = message.shouldBeProcessed();
      if (messageValidity.getDecision() == DO_SKIP) {
        LOG.debug("Skipping message: {}", messageValidity.getDescription());
        skippedMsgCount++;
        continue;
      }

      messages.add(msgStr);
    }
    LOG.info("Messages received: {}, toBeProcessed: {}, skipped: {}", receivedMessages.size(), messages.size(), skippedMsgCount);
    return new MessageBatch(messages);
  }

  private void ackOutstandingMessages() {
    if (messagesToAck.isEmpty()) {
      return;
    }

    try {
      pubsubMessagesFetcher.sendAcks(messagesToAck);
      messagesToAck.clear();
    } catch (IOException e) {
      throw new HoodieReadFromSourceException("Error when acknowledging messages from Pubsub", e);
    }
  }

  private void logDetails(MetadataMessage message, String msgStr) {
    LOG.debug("eventType: {}, objectId: {}", message.getEventType(), message.getObjectId());
    LOG.debug("msg: {}", msgStr);
  }

}
