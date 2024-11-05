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

package org.apache.hudi.hive.util;

import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.sync.common.model.FieldSchema;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.apache.hudi.hive.HiveSyncConfig.HIVE_SYNC_FILTER_PUSHDOWN_MAX_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestPartitionFilterGenerator {

  PartitionFilterGenerator partitionFilterGenerator = new PartitionFilterGenerator();
  @Test
  public void testPushDownFilters() {
    Properties props = new Properties();
    HiveSyncConfig config = new HiveSyncConfig(props);
    List<FieldSchema> partitionFieldSchemas = new ArrayList<>(4);
    partitionFieldSchemas.add(new FieldSchema("date", "date"));
    partitionFieldSchemas.add(new FieldSchema("year", "string"));
    partitionFieldSchemas.add(new FieldSchema("month", "int"));
    partitionFieldSchemas.add(new FieldSchema("day", "bigint"));

    List<String> writtenPartitions = new ArrayList<>();
    writtenPartitions.add("2022-09-01/2022/9/1");
    assertEquals("(((date = 2022-09-01 AND year = \"2022\") AND month = 9) AND day = 1)",
        partitionFilterGenerator.generatePushDownFilter(writtenPartitions, partitionFieldSchemas, config));

    writtenPartitions.add("2022-09-02/2022/9/2");
    assertEquals(
        "((((date = 2022-09-01 AND year = \"2022\") AND month = 9) AND day = 1) OR (((date = 2022-09-02 AND year = \"2022\") AND month = 9) AND day = 2))",
        partitionFilterGenerator.generatePushDownFilter(writtenPartitions, partitionFieldSchemas, config));

    // If there are incompatible types to convert as filters inside partition
    partitionFieldSchemas.clear();
    writtenPartitions.clear();
    partitionFieldSchemas.add(new FieldSchema("date", "date"));
    partitionFieldSchemas.add(new FieldSchema("finished", "boolean"));

    writtenPartitions.add("2022-09-01/true");
    assertEquals("date = 2022-09-01",
        partitionFilterGenerator.generatePushDownFilter(writtenPartitions, partitionFieldSchemas, config));
    writtenPartitions.add("2022-09-02/true");
    assertEquals("(date = 2022-09-01 OR date = 2022-09-02)",
        partitionFilterGenerator.generatePushDownFilter(writtenPartitions, partitionFieldSchemas, config));

    // If no compatible types matched to convert as filters
    partitionFieldSchemas.clear();
    writtenPartitions.clear();
    partitionFieldSchemas.add(new FieldSchema("finished", "boolean"));

    writtenPartitions.add("true");
    assertEquals("",
        partitionFilterGenerator.generatePushDownFilter(writtenPartitions, partitionFieldSchemas, config));
    writtenPartitions.add("false");
    assertEquals("",
        partitionFilterGenerator.generatePushDownFilter(writtenPartitions, partitionFieldSchemas, config));
  }

  @Test
  public void testPushDownFilterIfExceedLimit() {
    Properties props = new Properties();
    props.put(HIVE_SYNC_FILTER_PUSHDOWN_MAX_SIZE.key(), "0");
    HiveSyncConfig config = new HiveSyncConfig(props);
    List<FieldSchema> partitionFieldSchemas = new ArrayList<>(4);
    partitionFieldSchemas.add(new FieldSchema("date", "date"));
    partitionFieldSchemas.add(new FieldSchema("year", "string"));
    partitionFieldSchemas.add(new FieldSchema("month", "int"));
    partitionFieldSchemas.add(new FieldSchema("day", "bigint"));

    List<String> writtenPartitions = new ArrayList<>();
    writtenPartitions.add("2022-09-01/2022/9/1");

    assertEquals("(((date = 2022-09-01 AND year = \"2022\") AND month = 9) AND day = 1)",
        partitionFilterGenerator.generatePushDownFilter(writtenPartitions, partitionFieldSchemas, config));

    writtenPartitions.add("2022-09-02/2022/9/2");
    writtenPartitions.add("2022-09-03/2022/9/2");
    writtenPartitions.add("2022-09-04/2022/9/2");
    assertEquals(
        "((((date >= 2022-09-01 AND date <= 2022-09-04) AND year = \"2022\") AND month = 9) AND (day >= 1 AND day <= 2))",
        partitionFilterGenerator.generatePushDownFilter(writtenPartitions, partitionFieldSchemas, config));

    // If there are incompatible types to convert as filters inside partition
    partitionFieldSchemas.clear();
    writtenPartitions.clear();
    partitionFieldSchemas.add(new FieldSchema("date", "date"));
    partitionFieldSchemas.add(new FieldSchema("finished", "boolean"));

    writtenPartitions.add("2022-09-01/true");
    assertEquals("date = 2022-09-01",
        partitionFilterGenerator.generatePushDownFilter(writtenPartitions, partitionFieldSchemas, config));
    writtenPartitions.add("2022-09-02/true");
    writtenPartitions.add("2022-09-03/false");
    writtenPartitions.add("2022-09-04/false");
    assertEquals("(date >= 2022-09-01 AND date <= 2022-09-04)",
        partitionFilterGenerator.generatePushDownFilter(writtenPartitions, partitionFieldSchemas, config));

    // If no compatible types matched to convert as filters
    partitionFieldSchemas.clear();
    writtenPartitions.clear();
    partitionFieldSchemas.add(new FieldSchema("finished", "boolean"));

    writtenPartitions.add("true");
    assertEquals("",
        partitionFilterGenerator.generatePushDownFilter(writtenPartitions, partitionFieldSchemas, config));
    writtenPartitions.add("false");
    writtenPartitions.add("false");
    writtenPartitions.add("false");
    assertEquals("",
        partitionFilterGenerator.generatePushDownFilter(writtenPartitions, partitionFieldSchemas, config));
  }
}
