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

package org.apache.hudi.bigquery;

import org.junit.jupiter.api.Test;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
public class TestBigQuerySyncConfig {
  @Test
  public void testCopy() {
    BigQuerySyncConfig bigQuerySyncConfig = new BigQuerySyncConfig();
    List<String> partitions = Arrays.asList("a", "b");
    bigQuerySyncConfig.partitionFields = partitions;
    bigQuerySyncConfig.basePath = "/tmp";
    bigQuerySyncConfig.projectId = "test-project";
    bigQuerySyncConfig.datasetName = "test";
    bigQuerySyncConfig.tableName = "test";
    bigQuerySyncConfig.sourceUri = "gs://test-bucket/dwh/table_name/*";
    bigQuerySyncConfig.sourceUriPrefix = "gs://test-bucket/dwh/table_name/dt={DATE}";

    BigQuerySyncConfig copied = BigQuerySyncConfig.copy(bigQuerySyncConfig);

    assertEquals(copied.partitionFields, bigQuerySyncConfig.partitionFields);
    assertEquals(copied.basePath, bigQuerySyncConfig.basePath);
    assertEquals(copied.projectId, bigQuerySyncConfig.projectId);
    assertEquals(copied.datasetName, bigQuerySyncConfig.datasetName);
    assertEquals(copied.tableName, bigQuerySyncConfig.tableName);
    assertEquals(copied.sourceUri, bigQuerySyncConfig.sourceUri);
    assertEquals(copied.sourceUriPrefix, bigQuerySyncConfig.sourceUriPrefix);
  }
}
