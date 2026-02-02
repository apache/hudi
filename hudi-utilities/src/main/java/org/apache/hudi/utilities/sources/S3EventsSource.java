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
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.config.S3SourceConfig;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.CloudObjectsSelector;
import org.apache.hudi.utilities.sources.helpers.S3EventsMetaSelector;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hudi.utilities.config.CloudSourceConfig.META_EVENTS_PER_PARTITION;

/**
 * This source provides capability to create the hudi table for S3 events metadata (eg. S3
 * put events data). It will use the SQS for receiving the object key events. This can be useful
 * to check S3 files activity over time. The hudi table created by this source is consumed by
 * {@link S3EventsHoodieIncrSource} to apply changes to the hudi table corresponding to user data.
 */
public class S3EventsSource extends RowSource implements Closeable {

  private final S3EventsMetaSelector pathSelector;
  private final SchemaProvider schemaProvider;
  private final List<CloudObjectsSelector.MessageTracker> processedMessages = new ArrayList<>();
  private final int recordsPerPartition;
  SqsClient sqs;

  public S3EventsSource(
      TypedProperties props,
      JavaSparkContext sparkContext,
      SparkSession sparkSession,
      SchemaProvider schemaProvider) {
    super(props, sparkContext, sparkSession, schemaProvider);
    this.pathSelector = S3EventsMetaSelector.createSourceSelector(props);
    this.sqs = this.pathSelector.createAmazonSqsClient();
    this.schemaProvider = schemaProvider;
    this.recordsPerPartition = ConfigUtils.getIntWithAltKeys(props, META_EVENTS_PER_PARTITION);
  }

  /**
   * Fetches next events from the queue.
   *
   * @param lastCkptStr The last checkpoint instant string, empty if first run.
   * @param sourceLimit Limit on the size of data to fetch. For {@link S3EventsSource},
   *                    {@link S3SourceConfig#S3_SOURCE_QUEUE_MAX_MESSAGES_PER_BATCH} is used.
   * @return A pair of dataset of event records and the next checkpoint instant string
   */
  @Override
  public Pair<Option<Dataset<Row>>, Checkpoint> fetchNextBatch(Option<Checkpoint> lastCkptStr, long sourceLimit) {
    Pair<List<String>, Checkpoint> selectPathsWithLatestSqsMessage =
        pathSelector.getNextEventsFromQueue(sqs, lastCkptStr, processedMessages);
    if (selectPathsWithLatestSqsMessage.getLeft().isEmpty()) {
      return Pair.of(Option.empty(), selectPathsWithLatestSqsMessage.getRight());
    } else {
      int numPartitions = (int) Math.ceil(
          (double) selectPathsWithLatestSqsMessage.getLeft().size() / recordsPerPartition);
      Dataset<String> eventRecords = sparkSession.createDataset(selectPathsWithLatestSqsMessage.getLeft(), Encoders.STRING()).repartition(numPartitions);
      StructType sourceSchema = UtilHelpers.getSourceSchema(schemaProvider);
      if (sourceSchema != null) {
        return Pair.of(
            Option.of(sparkSession.read().schema(sourceSchema).json(eventRecords)),
            selectPathsWithLatestSqsMessage.getRight());
      } else {
        return Pair.of(
            Option.of(sparkSession.read().json(eventRecords)),
            selectPathsWithLatestSqsMessage.getRight());
      }
    }
  }

  @Override
  public void close() throws IOException {
    // close resource
    this.sqs.close();
  }

  @Override
  public void onCommit(String lastCkptStr) {
    pathSelector.deleteProcessedMessages(sqs, pathSelector.queueUrl, processedMessages);
    processedMessages.clear();
  }
}
