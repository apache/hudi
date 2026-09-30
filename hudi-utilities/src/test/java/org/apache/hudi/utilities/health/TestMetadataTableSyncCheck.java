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

package org.apache.hudi.utilities.health;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.metadata.MetadataPartitionType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the skip paths of {@link MetadataTableSyncCheck}, which need no metadata table and so no
 * Spark session. The verdicts over a real metadata table are covered by
 * {@link TestMetadataTableSyncCheckFunctional}.
 */
public class TestMetadataTableSyncCheck {

  @TempDir
  Path tempDir;

  private String basePath;
  private final MetadataTableSyncCheck check = new MetadataTableSyncCheck();

  @BeforeEach
  public void setUp() {
    basePath = tempDir.resolve("table").toString();
  }

  private TypedProperties propsWithMetadataEnabled(boolean enabled) {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieMetadataConfig.ENABLE.key(), String.valueOf(enabled));
    return props;
  }

  private HealthCheckResult runCheck(HoodieTableMetaClient metaClient, TypedProperties props) throws Exception {
    return check.check(new HealthCheckContext(HoodieTableMetaClient.reload(metaClient), props, false));
  }

  private HoodieTableMetaClient tableWithoutMetadataTable() throws Exception {
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);
    HoodieTestTable.of(metaClient).addCommit("20260101100000000");
    return metaClient;
  }

  @Test
  public void tableWithoutMetadataTableIsSkipped() throws Exception {
    // given a table that has never had a metadata table
    HoodieTableMetaClient metaClient = tableWithoutMetadataTable();

    // when the check runs with the metadata table enabled for the writer
    HealthCheckResult result = runCheck(metaClient, propsWithMetadataEnabled(true));

    // then there is nothing to compare, and the check says so rather than failing
    assertEquals(HealthStatus.SKIPPED, result.getStatus());
    assertTrue(result.getSummary().contains("no metadata table"), result.getSummary());
  }

  @Test
  public void metadataTableDisabledForWriterIsSkipped() throws Exception {
    // given any table
    HoodieTableMetaClient metaClient = tableWithoutMetadataTable();

    // when the writer is configured with the metadata table off
    HealthCheckResult result = runCheck(metaClient, propsWithMetadataEnabled(false));

    // then the check does not apply, and the reason names the property
    assertEquals(HealthStatus.SKIPPED, result.getStatus());
    assertTrue(result.getSummary().contains(HoodieMetadataConfig.ENABLE.key()), result.getSummary());
  }

  @Test
  public void missingEnablePropertySkipsNamingIt() throws Exception {
    // given any table
    HoodieTableMetaClient metaClient = tableWithoutMetadataTable();

    // when the check runs without knowing whether the writer uses the metadata table
    HealthCheckResult result = runCheck(metaClient, new TypedProperties());

    // then it declines to judge and says which property to supply
    assertEquals(HealthStatus.SKIPPED, result.getStatus());
    assertTrue(result.getSummary().contains(HoodieMetadataConfig.ENABLE.key()), result.getSummary());
    assertTrue(result.getSummary().contains("--apply-all-defaults"), result.getSummary());
  }

  @Test
  public void smallTableSamplesEveryPartitionInSortedOrder() {
    // given fewer partitions than the sample bound, listed in arbitrary order
    List<String> partitions = Arrays.asList("2024/03/02", "2024/03/01", "2024/03/03");

    // when the sample is taken
    List<String> sampled = MetadataTableSyncCheck.samplePartitions(partitions);

    // then every partition is compared, in a stable order
    assertEquals(Arrays.asList("2024/03/01", "2024/03/02", "2024/03/03"), sampled);
  }

  @Test
  public void largeTableSamplesTheNewestPartitionsPlusAFewOfTheOldest() {
    // given a date-partitioned table with many more partitions than the sample bound
    List<String> partitions = new ArrayList<>();
    for (int day = 1; day <= 60; day++) {
      partitions.add(String.format("2024/03/%02d", day));
    }
    Collections.shuffle(partitions, new Random(7));

    // when the sample is taken
    List<String> sampled = MetadataTableSyncCheck.samplePartitions(partitions);

    // then it is the most recently written partitions -- where drift originates -- plus the oldest few for coverage
    assertEquals(MetadataTableSyncCheck.MAX_PARTITIONS_TO_COMPARE + MetadataTableSyncCheck.OLDEST_PARTITIONS_TO_INCLUDE, sampled.size());
    assertEquals(Arrays.asList("2024/03/01", "2024/03/02"), sampled.subList(0, 2));
    assertEquals("2024/03/41", sampled.get(2));
    assertEquals("2024/03/60", sampled.get(sampled.size() - 1));
  }

  @Test
  public void metadataTableRegisteredButMissingOnStorageIsSkipped() throws Exception {
    // given a table whose hoodie.properties claims a FILES partition, but with no metadata table directory
    HoodieTableMetaClient metaClient = tableWithoutMetadataTable();
    Properties registration = new Properties();
    registration.setProperty(HoodieTableConfig.TABLE_METADATA_PARTITIONS.key(), MetadataPartitionType.FILES.getPartitionPath());
    HoodieTableConfig.update(metaClient.getStorage(), metaClient.getMetaPath(), registration);

    // when the check runs
    HealthCheckResult result = runCheck(metaClient, propsWithMetadataEnabled(true));

    // then it skips with the path it looked for, instead of failing to open the metadata table
    assertEquals(HealthStatus.SKIPPED, result.getStatus());
    assertTrue(result.getSummary().contains("does not exist"), result.getSummary());
  }
}
