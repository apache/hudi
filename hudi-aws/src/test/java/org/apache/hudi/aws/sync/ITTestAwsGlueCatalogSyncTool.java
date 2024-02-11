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

package org.apache.hudi.aws.sync;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class ITTestAwsGlueCatalogSyncTool extends ITTestGlueUtil {

  @Test
  public void testWhenCreatePartitionsShouldExistsInGlue() throws IOException, ExecutionException, InterruptedException {
    setupPartitions("driver");

    hudiJavaClient = clientCOW();
    String newCommitTime = hudiJavaClient.startCommit();
    hudiJavaClient.insert(getHoodieRecords(newCommitTime, 1, "driver1"), newCommitTime);
    hudiJavaClient.insert(getHoodieRecords(newCommitTime, 1, "driver2"), newCommitTime);

    getAwsGlueCatalogSyncTool().syncHoodieTable();

    Assertions.assertTrue(glueClient.getDatabase(d ->
        d.name(DB_NAME)).get().database().name().equals(DB_NAME));
    Assertions.assertTrue(glueClient.getTable(t ->
        t.databaseName(DB_NAME).name(TABLE_NAME)).get().table().name().equals(TABLE_NAME));
    Assertions.assertEquals(2, glueClient.getPartitions(p ->
        p.databaseName(DB_NAME).tableName(TABLE_NAME)).get().partitions().size());
  }

  @Test
  public void testWhenCreateNestedTableShouldExistsInGlue() throws IOException, ExecutionException, InterruptedException {
    setupPartitions("driver");
    setDataGenerator(HoodieNestedDataGenerator.class);

    hudiJavaClient = clientCOW();
    String newCommitTime = hudiJavaClient.startCommit();
    hudiJavaClient.insert(getHoodieRecords(newCommitTime, 1, "driver1"), newCommitTime);

    getAwsGlueCatalogSyncTool().syncHoodieTable();

    Assertions.assertTrue(glueClient.getDatabase(d ->
        d.name(DB_NAME)).get().database().name().equals(DB_NAME));
    Assertions.assertTrue(glueClient.getTable(t ->
        t.databaseName(DB_NAME).name(TABLE_NAME)).get().table().name().equals(TABLE_NAME));
    Assertions.assertEquals(1, glueClient.getPartitions(p ->
        p.databaseName(DB_NAME).tableName(TABLE_NAME)).get().partitions().size());
  }

}
