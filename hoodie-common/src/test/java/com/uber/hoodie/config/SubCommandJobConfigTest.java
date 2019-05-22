/*
 *  Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.uber.hoodie.config;

import static org.hamcrest.CoreMatchers.both;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertEquals;

import com.uber.hoodie.configs.*;
import com.uber.hoodie.configs.AbstractCommandConfig;
import com.uber.hoodie.configs.HoodieCompactionAdminToolJobConfig.Operation;
import com.uber.hoodie.exception.InvalidJobConfigException;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class SubCommandJobConfigTest extends AbstractCommandConfig {

  enum SubCommandEnum {
    ROLLBACK, DEDUPLICATE, ROLLBACK_TO_SAVEPOINT, SAVEPOINT, IMPORT, UPSERT, COMPACT_SCHEDULE, COMPACT_RUN,
    COMPACT_UNSCHEDULE_PLAN, COMPACT_UNSCHEDULE_FILE, COMPACT_VALIDATE, COMPACT_REPAIR;
  }

  @Rule
  public final ExpectedException thrown = ExpectedException.none();
  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();

  @Before
  public void setUpStreams() {
    System.setOut(new PrintStream(outContent));
  }

  @After
  public void restoreStreams() {
    System.setOut(System.out);
  }

  private Map<String, AbstractCommandConfig> commandConfigMap = new HashMap<>();

  {
    commandConfigMap.put(SubCommandEnum.ROLLBACK.name(), new HoodieCommitRollbackJobConfig());
    commandConfigMap.put(SubCommandEnum.DEDUPLICATE.name(), new HoodieDeduplicatePartitionJobConfig());
    commandConfigMap.put(SubCommandEnum.ROLLBACK_TO_SAVEPOINT.name(), new HoodieRollbackToSavePointJobConfig());

    commandConfigMap.put(SubCommandEnum.IMPORT.name(), getHdfsParquetImportConfig(SubCommandEnum.IMPORT.name()));
    commandConfigMap.put(SubCommandEnum.UPSERT.name(), getHdfsParquetImportConfig(SubCommandEnum.UPSERT.name()));

    commandConfigMap.put(SubCommandEnum.COMPACT_RUN.name(), new HoodieCompactorJobConfig());
    commandConfigMap.put(SubCommandEnum.COMPACT_SCHEDULE.name(), new HoodieCompactorJobConfig());

    commandConfigMap.put(SubCommandEnum.COMPACT_REPAIR.name(), getCompactionConfig(Operation.REPAIR));
    commandConfigMap.put(SubCommandEnum.COMPACT_VALIDATE.name(), getCompactionConfig(Operation.VALIDATE));
    commandConfigMap.put(SubCommandEnum.COMPACT_UNSCHEDULE_FILE.name(), getCompactionConfig(Operation.UNSCHEDULE_FILE));
    commandConfigMap.put(SubCommandEnum.COMPACT_UNSCHEDULE_PLAN.name(), getCompactionConfig(Operation.UNSCHEDULE_PLAN));
  }


  @Test
  public void testRollbackShouldPrintHelp() {
    String[] configs = "ROLLBACK --help".split(" ");
    SubCommandJobConfigTest config = new SubCommandJobConfigTest();
    config.parseJobConfig(configs, commandConfigMap);
    Assert.assertThat(outContent.toString(), both(startsWith("Usage")).and(containsString("--commit-time")));
  }

  @Test
  public void testRollbackShouldThrowException() throws InvalidJobConfigException {
    String[] configs = "ROLLBACK --commit-time ct".split(" ");
    SubCommandJobConfigTest config = new SubCommandJobConfigTest();

    thrown.expect(InvalidJobConfigException.class);
    thrown.expectMessage(containsString("[--base-path | -bp]"));
    config.parseJobConfig(configs, commandConfigMap);
  }

  @Test
  public void testDeduplicateShouldParse() {
    String[] configs = "ROLLBACK --commit-time ct --base-path bp".split(" ");
    SubCommandJobConfigTest config = new SubCommandJobConfigTest();
    config.parseJobConfig(configs, commandConfigMap);
    HoodieCommitRollbackJobConfig configHCR = (HoodieCommitRollbackJobConfig) commandConfigMap.get("ROLLBACK");
    assertEquals("ct", configHCR.commitTime);
    assertEquals("bp", configHCR.basePath);
  }

  @Test
  public void testDeduplicateShouldPrintHelp() {
    String[] configs = "DEDUPLICATE --help".split(" ");
    SubCommandJobConfigTest config = new SubCommandJobConfigTest();
    config.parseJobConfig(configs, commandConfigMap);
    Assert.assertThat(outContent.toString(),
        both(startsWith("Usage")).and(containsString(" --duplicated-partition-path")));
  }

  @Test
  public void testDeduplicateShouldThrowException() throws InvalidJobConfigException {
    String[] configs = "DEDUPLICATE --duplicated-partition-path dpp ".split(" ");
    SubCommandJobConfigTest config = new SubCommandJobConfigTest();

    thrown.expect(InvalidJobConfigException.class);
    thrown.expectMessage(containsString("[--base-path | -bp]"));
    config.parseJobConfig(configs, commandConfigMap);
  }

  @Test
  public void testRollbackShouldParse() {
    String[] configs = "DEDUPLICATE --duplicated-partition-path ddp --repaired-output-path rop --base-path bp"
        .split(" ");
    SubCommandJobConfigTest config = new SubCommandJobConfigTest();
    config.parseJobConfig(configs, commandConfigMap);
    HoodieDeduplicatePartitionJobConfig configDDP = (HoodieDeduplicatePartitionJobConfig) commandConfigMap
        .get("DEDUPLICATE");
    assertEquals("ddp", configDDP.duplicatedPartitionPath);
    assertEquals("rop", configDDP.repairedOutputPath);
    assertEquals("bp", configDDP.basePath);
  }

  @Test
  public void testHadoopCompactShouldParse() {
    Boolean temp = true;
    String configString = "COMPACT_SCHEDULE --base-path bp "
        + "--table-name tn --instant-time it --parallelism 1 --schema-file  "
        + "--spark-memory sm --retry 0 --schedule true --strategy s";
    String[] configs = configString.split(" ");
    SubCommandJobConfigTest config = new SubCommandJobConfigTest();
    config.parseJobConfig(configs, commandConfigMap);
    HoodieCompactorJobConfig configHC = (HoodieCompactorJobConfig) commandConfigMap
        .get("COMPACT_SCHEDULE");
    assertEquals("bp", configHC.basePath);
    assertEquals("tn", configHC.tableName);
    assertEquals("it", configHC.compactionInstantTime);
    assertEquals(1, configHC.parallelism);
    assertEquals("", configHC.schemaFile);
    assertEquals("sm", configHC.sparkMemory);
    assertEquals(0, configHC.retry);
    assertEquals(0, configHC.retry);
    assertEquals(true, configHC.runSchedule);
    assertEquals("s", configHC.strategyClassName);
  }


  private HDFSParquetImporterJobConfig getHdfsParquetImportConfig(String command) {
    HDFSParquetImporterJobConfig cfg = new HDFSParquetImporterJobConfig();
    cfg.command = command;
    return cfg;
  }

  private HoodieCompactionAdminToolJobConfig getCompactionConfig(
      HoodieCompactionAdminToolJobConfig.Operation operation) {
    HoodieCompactionAdminToolJobConfig cfg = new HoodieCompactionAdminToolJobConfig();
    cfg.operation = operation;
    return cfg;
  }
}


