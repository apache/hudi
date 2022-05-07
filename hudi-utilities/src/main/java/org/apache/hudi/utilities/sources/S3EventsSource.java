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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.S3EventsMetaSelector;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

/**
 * This source provides capability to create the hudi table for S3 events metadata (eg. S3
 * put events data). It will use the SQS for receiving the object key events. This can be useful
 * to check S3 files activity over time. The hudi table created by this source is consumed by
 * {@link S3EventsHoodieIncrSource} to apply changes to the hudi table corresponding to user data.
 */
public class S3EventsSource extends RowSource {

  private final S3EventsMetaSelector pathSelector;
  private final List<Message> processedMessages = new ArrayList<>();
  AmazonSQS sqs;

  public S3EventsSource(
      TypedProperties props,
      JavaSparkContext sparkContext,
      SparkSession sparkSession,
      SchemaProvider schemaProvider) {
    super(props, sparkContext, sparkSession, schemaProvider);
    this.pathSelector = S3EventsMetaSelector.createSourceSelector(props);
    this.sqs = this.pathSelector.createAmazonSqsClient();
  }

  /**
   * Fetches next events from the queue.
   *
   * @param lastCkptStr The last checkpoint instant string, empty if first run.
   * @param sourceLimit Limit on the size of data to fetch. For {@link S3EventsSource},
   *                    {@link org.apache.hudi.utilities.sources.helpers.CloudObjectsSelector.Config#S3_SOURCE_QUEUE_MAX_MESSAGES_PER_BATCH} is used.
   * @return A pair of dataset of event records and the next checkpoint instant string
   */
  @Override
  public Pair<Option<Dataset<Row>>, String> fetchNextBatch(Option<String> lastCkptStr, long sourceLimit) {
    Pair<List<String>, String> selectPathsWithLatestSqsMessage =
        pathSelector.getNextEventsFromQueue(sqs, lastCkptStr, processedMessages);
    if (selectPathsWithLatestSqsMessage.getLeft().isEmpty()) {
      return Pair.of(Option.empty(), selectPathsWithLatestSqsMessage.getRight());
    } else {
      Dataset<String> eventRecords = sparkSession.createDataset(selectPathsWithLatestSqsMessage.getLeft(), Encoders.STRING());
      return Pair.of(
          Option.of(sparkSession.read().json(eventRecords)),
          selectPathsWithLatestSqsMessage.getRight());
    }
  }

  @Override
  public void onCommit(String lastCkptStr) {
    pathSelector.deleteProcessedMessages(sqs, pathSelector.queueUrl, processedMessages);
    processedMessages.clear();
  }
}
