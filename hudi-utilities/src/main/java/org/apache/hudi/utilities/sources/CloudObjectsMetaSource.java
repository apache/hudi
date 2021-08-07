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
import org.apache.hudi.utilities.sources.helpers.CloudObjectsMetaSelector;

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
 * This source provides capability to create the hoodie table for cloudObject Metadata (eg. s3
 * events data). It will use the cloud queue for receiving the object key events. This can be useful
 * for check cloud file activity over time and consuming this to create other hoodie table from
 * cloud object data.
 */
public class CloudObjectsMetaSource extends RowSource {

  private final CloudObjectsMetaSelector pathSelector;
  private final List<Message> processedMessages = new ArrayList<>();
  AmazonSQS sqs;

  /**
   * Cloud Objects Meta Source Class.
   */
  public CloudObjectsMetaSource(
      TypedProperties props,
      JavaSparkContext sparkContext,
      SparkSession sparkSession,
      SchemaProvider schemaProvider) {
    super(props, sparkContext, sparkSession, schemaProvider);
    this.pathSelector = CloudObjectsMetaSelector.createSourceSelector(props);
    this.sqs = this.pathSelector.createAmazonSqsClient();
  }

  @Override
  public Pair<Option<Dataset<Row>>, String> fetchNextBatch(
      Option<String> lastCkptStr, long sourceLimit) {

    Pair<List<String>, String> selectPathsWithLatestSqsMessage =
        pathSelector.getNextEventsFromQueue(sqs, sparkContext, lastCkptStr, processedMessages);
    if (selectPathsWithLatestSqsMessage.getLeft().isEmpty()) {
      return Pair.of(Option.empty(), selectPathsWithLatestSqsMessage.getRight());
    } else {
      return Pair.of(
          Option.of(fromEventRecords(selectPathsWithLatestSqsMessage.getLeft())),
          selectPathsWithLatestSqsMessage.getRight());
    }
  }

  private Dataset<Row> fromEventRecords(List<String> jsonData) {
    Dataset<String> anotherPeopleDataset = sparkSession.createDataset(jsonData, Encoders.STRING());
    return sparkSession.read().json(anotherPeopleDataset);
  }

  @Override
  public void onCommit(String lastCkptStr) {
    pathSelector.onCommitDeleteProcessedMessages(sqs, pathSelector.queueUrl, processedMessages);
    processedMessages.clear();
  }
}
