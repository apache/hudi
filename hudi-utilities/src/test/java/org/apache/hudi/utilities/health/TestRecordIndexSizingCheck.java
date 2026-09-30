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
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.metadata.MetadataPartitionType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the paths of {@link RecordIndexSizingCheck} that need no metadata table on disk: the check
 * declining to run. Verdicts on a real record index are covered by
 * {@link TestRecordIndexSizingCheckWithSpark}, which has to write one.
 */
public class TestRecordIndexSizingCheck {

  @TempDir
  Path tempDir;

  private String basePath;
  private final RecordIndexSizingCheck check = new RecordIndexSizingCheck();

  @BeforeEach
  public void setUp() {
    basePath = tempDir.resolve("table").toString();
  }

  private HoodieTableMetaClient tableWithRecordIndexListed() throws Exception {
    // Only the table configuration says the partition exists; there is no metadata table behind it.
    // Enough for the check's fail-fast paths, which must not touch the metadata table.
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);
    HoodieTestTable.of(metaClient).addCommit("001");
    metaClient.getTableConfig().setMetadataPartitionState(
        metaClient, MetadataPartitionType.RECORD_INDEX.getPartitionPath(), true);
    return HoodieTableMetaClient.reload(metaClient);
  }

  private TypedProperties globalSizingProps() {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_MIN_FILE_GROUP_COUNT_PROP.key(), "2");
    props.setProperty(HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_MAX_FILE_GROUP_COUNT_PROP.key(), "2");
    props.setProperty(HoodieMetadataConfig.RECORD_INDEX_MAX_FILE_GROUP_SIZE_BYTES_PROP.key(), "1073741824");
    props.setProperty(HoodieMetadataConfig.RECORD_INDEX_GROWTH_FACTOR_PROP.key(), "2.0");
    return props;
  }

  @Test
  public void tableWithoutRecordIndexIsSkippedAndSaysSo() throws Exception {
    // given a table whose metadata table has no record index partition
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);
    HoodieTestTable.of(metaClient).addCommit("001");

    // when the check runs with every sizing property supplied
    HealthCheckResult result = check.check(new HealthCheckContext(metaClient, globalSizingProps(), false));

    // then it does not apply, and the reason names the partition it looked for
    assertFalse(check.appliesTo(new HealthCheckContext(metaClient, globalSizingProps(), false)));
    assertEquals(HealthStatus.SKIPPED, result.getStatus());
    assertTrue(result.getSummary().contains(MetadataPartitionType.RECORD_INDEX.getPartitionPath()),
        "expected the skip reason to name the record index partition, got: " + result.getSummary());
  }

  @Test
  public void missingSizingConfigSkipsNamingTheFirstMissingProperty() throws Exception {
    // given a table with a record index and no writer configuration supplied
    HoodieTableMetaClient metaClient = tableWithRecordIndexListed();

    // when the check runs
    HealthCheckResult result = check.check(new HealthCheckContext(metaClient, new TypedProperties(), false));

    // then it declines to judge and names the global minimum file-group count first
    assertEquals(HealthStatus.SKIPPED, result.getStatus());
    assertTrue(result.getSummary().contains(
        HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_MIN_FILE_GROUP_COUNT_PROP.key()), result.getSummary());
  }

  @Test
  public void eachSizingPropertyIsRequiredInTurn() throws Exception {
    // given a record index and a properties set missing only the growth factor
    HoodieTableMetaClient metaClient = tableWithRecordIndexListed();
    TypedProperties props = globalSizingProps();
    props.remove(HoodieMetadataConfig.RECORD_INDEX_GROWTH_FACTOR_PROP.key());

    // when the check runs
    HealthCheckResult result = check.check(new HealthCheckContext(metaClient, props, false));

    // then that is the property it asks for
    assertEquals(HealthStatus.SKIPPED, result.getStatus());
    assertTrue(result.getSummary().contains(HoodieMetadataConfig.RECORD_INDEX_GROWTH_FACTOR_PROP.key()),
        result.getSummary());
  }

  @Test
  public void partitionedRecordIndexRequiresThePartitionedSizingKeys() throws Exception {
    // given a writer that enables the partitioned record index but only supplies the global sizing keys
    HoodieTableMetaClient metaClient = tableWithRecordIndexListed();
    TypedProperties props = globalSizingProps();
    props.setProperty(HoodieMetadataConfig.RECORD_LEVEL_INDEX_ENABLE_PROP.key(), "true");

    // when the check runs
    HealthCheckResult result = check.check(new HealthCheckContext(metaClient, props, false));

    // then it asks for the partitioned key, since that is what the writer sizes with
    assertEquals(HealthStatus.SKIPPED, result.getStatus());
    assertTrue(result.getSummary().contains(
        HoodieMetadataConfig.RECORD_LEVEL_INDEX_MIN_FILE_GROUP_COUNT_PROP.key()), result.getSummary());
  }

  @Test
  public void maxFileGroupSizeBelowOneRecordIsRejectedRatherThanDividedByZero() throws Exception {
    // given a maximum file-group size smaller than a single index record
    HoodieTableMetaClient metaClient = tableWithRecordIndexListed();
    TypedProperties props = globalSizingProps();
    props.setProperty(HoodieMetadataConfig.RECORD_INDEX_MAX_FILE_GROUP_SIZE_BYTES_PROP.key(), "10");

    // when the check runs
    HealthCheckResult result = check.check(new HealthCheckContext(metaClient, props, false));

    // then it skips with the offending value echoed, before ever opening the metadata table
    assertEquals(HealthStatus.SKIPPED, result.getStatus());
    assertTrue(result.getSummary().contains(HoodieMetadataConfig.RECORD_INDEX_MAX_FILE_GROUP_SIZE_BYTES_PROP.key()),
        result.getSummary());
    assertEquals("10", result.getEffectiveConfigs()
        .get(HoodieMetadataConfig.RECORD_INDEX_MAX_FILE_GROUP_SIZE_BYTES_PROP.key()));
  }
}
