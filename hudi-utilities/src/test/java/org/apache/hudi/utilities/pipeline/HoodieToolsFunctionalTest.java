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

package org.apache.hudi.utilities.pipeline;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.util.List;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;

/**
 * Base functional test harness for Hudi utility "tools" that operate against a Spark-backed Hudi
 * table. Boots a local {@link JavaSparkContext} and provides helpers to seed a source table with
 * one or more commits.
 */
public abstract class HoodieToolsFunctionalTest extends HoodieCommonTestHarness {
  protected JavaSparkContext jsc;
  protected transient HoodieSparkEngineContext context;

  @BeforeEach
  public void setUp() throws IOException {
    // Initialize Spark context
    jsc = new JavaSparkContext("local[3]", "test-hoodie-operation");
    context = new HoodieSparkEngineContext(jsc);

    // Initialize table
    setTableName("test_db.test_table");
    initPath();

    // Initialize HoodieTableMetaClient
    initMetaClient();
  }

  @AfterEach
  public void tearDown() {
    if (jsc != null) {
      jsc.stop();
    }
  }

  public void setupTable(boolean isMetadataEnabled) {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(isMetadataEnabled).build())
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .build();

    setupTable(config);
  }

  public void setupTable(HoodieWriteConfig config) {
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(context, config)) {
      String newCommitTime = client.startCommit();
      int numRecords = 10;

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, numRecords);
      JavaRDD<HoodieRecord> writeRecords = context.getJavaSparkContext().parallelize(records, 1);
      List<WriteStatus> result = client.upsert(writeRecords, newCommitTime).collect();
      assertNoWriteErrors(result);
      client.commit(newCommitTime, context.getJavaSparkContext().parallelize(result));
    }
  }

  public void setupTableWithMultipleCommits(int numCommits) {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .build();
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();

    for (int i = 0; i < numCommits; i++) {
      try (SparkRDDWriteClient client = new SparkRDDWriteClient(context, config)) {
        String commitTime = client.startCommit();
        List<HoodieRecord> records = dataGen.generateInserts(commitTime, 10);
        JavaRDD<HoodieRecord> writeRecords = context.getJavaSparkContext().parallelize(records, 1);
        List<WriteStatus> result = client.upsert(writeRecords, commitTime).collect();
        assertNoWriteErrors(result);
        client.commit(commitTime, context.getJavaSparkContext().parallelize(result));
      }
    }
  }

  public String createPath(String tableName) {
    try {
      java.nio.file.Path basePath = tempDir.resolve(tableName);
      java.nio.file.Files.createDirectories(basePath);
      return basePath.toAbsolutePath().toString();
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
  }

}
