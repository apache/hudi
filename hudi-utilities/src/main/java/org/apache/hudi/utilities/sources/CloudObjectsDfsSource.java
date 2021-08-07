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
import org.apache.hudi.utilities.sources.helpers.CloudObjectsDfsSelector;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

/**
 * This source provides capability to create the hoodie table from cloudObject data (eg. s3 events).
 * It will primarily use cloud queue to fetch new object information and update hoodie table with
 * cloud object data.
 */
public class CloudObjectsDfsSource extends RowSource {

  private final CloudObjectsDfsSelector pathSelector;
  private final List<Message> processedMessages = new ArrayList<>();
  AmazonSQS sqs;

  /**
   * Cloud Objects Dfs Source Class Constructor.
   */
  public CloudObjectsDfsSource(
      TypedProperties props,
      JavaSparkContext sparkContext,
      SparkSession sparkSession,
      SchemaProvider schemaProvider) {
    super(props, sparkContext, sparkSession, schemaProvider);
    this.pathSelector = CloudObjectsDfsSelector.createSourceSelector(props);
    sqs = this.pathSelector.createAmazonSqsClient();
  }

  @Override
  public Pair<Option<Dataset<Row>>, String> fetchNextBatch(
      Option<String> lastCkptStr, long sourceLimit) {

    Pair<Option<String>, String> selectPathsWithLatestQueueMessages =
        pathSelector.getNextFilePathsFromQueue(sqs, lastCkptStr, processedMessages);
    return selectPathsWithLatestQueueMessages
        .getLeft()
        .map(
            pathStr ->
                Pair.of(
                    Option.of(fromParquetFiles(pathStr)),
                    selectPathsWithLatestQueueMessages.getRight()))
        .orElseGet(() -> Pair.of(Option.empty(), selectPathsWithLatestQueueMessages.getRight()));
  }

  private Dataset<Row> fromParquetFiles(String pathStr) {
    System.out.println(pathStr);
    return sparkSession.read().parquet(pathStr.split(","));
  }

  @Override
  public void onCommit(String lastCkptStr) {
    pathSelector.onCommitDeleteProcessedMessages(sqs, pathSelector.queueUrl, processedMessages);
    processedMessages.clear();
  }
}
