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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.either;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertEquals;

import com.uber.hoodie.configs.HDFSParquetImporterJobConfig;
import com.uber.hoodie.exception.InvalidJobConfigException;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class JobConfigsTest {

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

  @Test
  public void testPIJCShouldPrintHelp() {
    String[] configs = getConfigArray("--help");
    HDFSParquetImporterJobConfig config = new HDFSParquetImporterJobConfig();
    config.parseJobConfig(configs);
    Assert.assertThat(outContent.toString(), either(startsWith("Usage:")).or(containsString("--command ")));
  }

  @Test
  public void testHPIJCShouldThrowException() throws InvalidJobConfigException {
    String[] configs = getConfigArray("--command upsert --parallelism 2 "
        + "--partition-key-field pkf --retry 1 --row-key-field rkf --schema-file sf "
        + "--spark-memory 2 --src-path sp --table-name tn --table-type tt");
    HDFSParquetImporterJobConfig config = new HDFSParquetImporterJobConfig();

    thrown.expect(InvalidJobConfigException.class);
    thrown.expectMessage(containsString("[--target-path | -tp]"));
    config.parseJobConfig(configs);
  }

  @Test
  public void testHPIJCShouldParse() {
    String[] configs = getConfigArray("--command upsert --parallelism 1 "
        + "--partition-key-field pkf --retry 1 --row-key-field rkf --schema-file sf "
        + "--spark-memory 2GB --src-path sp --table-name tn --table-type tt --target-path tp");
    HDFSParquetImporterJobConfig config = new HDFSParquetImporterJobConfig();
    config.parseJobConfig(configs);

    assertEquals("upsert", config.command);
    assertEquals(1, config.parallelism);
    assertEquals(1, config.retry);
    assertEquals("2GB", config.sparkMemory);
    assertEquals("pkf", config.partitionKey);
    assertEquals("rkf", config.rowKey);
    assertEquals("sf", config.schemaFile);
    assertEquals("sp", config.srcPath);
    assertEquals("tn", config.tableName);
    assertEquals("tp", config.targetPath);
  }

  private static String[] getConfigArray(String configString) {
    return configString.split(" ");
  }
}
