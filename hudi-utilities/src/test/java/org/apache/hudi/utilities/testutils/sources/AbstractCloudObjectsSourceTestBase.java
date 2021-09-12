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

package org.apache.hudi.utilities.testutils.sources;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;
import org.apache.hudi.utilities.sources.Source;
import org.apache.hudi.utilities.testutils.CloudObjectTestUtils;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import com.amazonaws.services.sqs.AmazonSQS;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.List;

/**
 * An abstract test base for {@link Source} using CloudObjects as the file system.
 */
public abstract class AbstractCloudObjectsSourceTestBase extends UtilitiesTestBase {

  protected FilebasedSchemaProvider schemaProvider;
  protected String dfsRoot;
  protected String fileSuffix;
  protected HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
  protected boolean useFlattenedSchema = false;
  protected String sqsUrl = "test-queue";
  protected String regionName = "us-east-1";
  @Mock
  protected AmazonSQS sqs;

  @BeforeAll
  public static void initClass() throws Exception {
    UtilitiesTestBase.initClass();
  }

  @AfterAll
  public static void cleanupClass() {
    UtilitiesTestBase.cleanupClass();
  }

  @BeforeEach
  public void setup() throws Exception {
    super.setup();
    schemaProvider = new FilebasedSchemaProvider(Helpers.setupSchemaOnDFS(), jsc);
    MockitoAnnotations.initMocks(this);
  }

  @AfterEach
  public void teardown() throws Exception {
    super.teardown();
  }

  /**
   * Prepares the specific {@link Source} to test, by passing in necessary configurations.
   *
   * @return A {@link Source} using DFS as the file system.
   */
  protected abstract Source prepareCloudObjectSource();

  /**
   * Writes test data, i.e., a {@link List} of {@link HoodieRecord}, to a file on DFS.
   *
   * @param records Test data.
   * @param path    The path in {@link Path} of the file to write.
   */
  protected abstract void writeNewDataToFile(List<HoodieRecord> records, Path path)
      throws IOException;

  /**
   * Generates a batch of test data and writes the data to a file.
   *
   * @param filename    The name of the file.
   * @param instantTime The commit time.
   * @param n           The number of records to generate.
   * @return The file path.
   */
  protected Path generateOneFile(String filename, String instantTime, int n) throws IOException {
    Path path = new Path(dfsRoot, filename + fileSuffix);
    writeNewDataToFile(dataGenerator.generateInserts(instantTime, n, useFlattenedSchema), path);
    return path;
  }

  public void generateMessageInQueue(String filename) {
    Path path = null;
    if (filename != null) {
      path = new Path(dfsRoot, filename + fileSuffix);
    }
    CloudObjectTestUtils.setMessagesInQueue(sqs, path);
  }
}
