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

package org.apache.hudi.utilities.sources.helpers;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.testutils.HoodieClientTestHarness;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.apache.spark.sql.RowFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

public class TestCloudObjectsSelectorCommon extends HoodieClientTestHarness {

  @BeforeEach
  void setUp() {
    initSparkContexts();
  }

  @AfterEach
  public void teardown() throws Exception {
    cleanupResources();
  }

  @Test
  public void emptyMetadataReturnsEmptyOption() {
    Option<Dataset<Row>> result = CloudObjectsSelectorCommon.loadAsDataset(sparkSession, Collections.emptyList(), new TypedProperties(), "json");
    Assertions.assertFalse(result.isPresent());
  }

  @Test
  public void filesFromMetadataRead() {
    List<CloudObjectMetadata> input = Collections.singletonList(new CloudObjectMetadata("src/test/resources/data/partitioned/country=US/state=CA/data.json", 1));
    Option<Dataset<Row>> result = CloudObjectsSelectorCommon.loadAsDataset(sparkSession, input, new TypedProperties(), "json");
    Assertions.assertTrue(result.isPresent());
    Assertions.assertEquals(1, result.get().count());
    Row expected = RowFactory.create("some data");
    Assertions.assertEquals(Collections.singletonList(expected), result.get().collectAsList());
  }

  @Test
  public void partitionValueAddedToRow() {
    List<CloudObjectMetadata> input = Collections.singletonList(new CloudObjectMetadata("src/test/resources/data/partitioned/country=US/state=CA/data.json", 1));

    TypedProperties properties = new TypedProperties();
    properties.put("hoodie.deltastreamer.source.cloud.data.partition.fields.from.path", "country,state");
    Option<Dataset<Row>> result = CloudObjectsSelectorCommon.loadAsDataset(sparkSession, input, properties, "json");
    Assertions.assertTrue(result.isPresent());
    Assertions.assertEquals(1, result.get().count());
    Row expected = RowFactory.create("some data", "US", "CA");
    Assertions.assertEquals(Collections.singletonList(expected), result.get().collectAsList());
  }

  @Test
  public void partitionKeyNotPresentInPath() {
    List<CloudObjectMetadata> input = Collections.singletonList(new CloudObjectMetadata("src/test/resources/data/partitioned/country=US/state=CA/data.json", 1));
    TypedProperties properties = new TypedProperties();
    properties.put("hoodie.deltastreamer.source.cloud.data.partition.fields.from.path", "unknown");
    Option<Dataset<Row>> result = CloudObjectsSelectorCommon.loadAsDataset(sparkSession, input, properties, "json");
    Assertions.assertTrue(result.isPresent());
    Assertions.assertEquals(1, result.get().count());
    Row expected = RowFactory.create("some data", null);
    Assertions.assertEquals(Collections.singletonList(expected), result.get().collectAsList());
  }
}
