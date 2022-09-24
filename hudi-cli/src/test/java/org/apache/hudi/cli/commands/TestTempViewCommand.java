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

package org.apache.hudi.cli.commands;

import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.functional.CLIFunctionalTestHarness;
import org.apache.hudi.cli.testutils.MockCommandLineInput;
import org.apache.hudi.cli.utils.SparkTempViewProvider;
import org.apache.hudi.cli.utils.TempViewProvider;
import org.apache.hudi.exception.HoodieException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.shell.Shell;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("functional")
@SpringBootTest(properties = {"spring.shell.interactive.enabled=false", "spring.shell.command.script.enabled=false"})
public class TestTempViewCommand extends CLIFunctionalTestHarness {

  @Autowired
  private Shell shell;
  private TempViewProvider tempViewProvider;
  private final String tableName = tableName();

  @BeforeEach
  public void init() {
    List<List<Comparable>> rows = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      rows.add(Arrays.asList(new Comparable[] {"c1", "c2", "c3"}));
    }
    tempViewProvider = new SparkTempViewProvider(jsc(), sqlContext());
    tempViewProvider.createOrReplace(tableName, Arrays.asList("t1", "t2", "t3"), rows);
    HoodieCLI.tempViewProvider = tempViewProvider;
  }

  @AfterEach
  public void cleanUpTempView() {
    tempViewProvider.close();
    HoodieCLI.tempViewProvider = null;
  }

  @Test
  public void testQueryWithException() {
    Object result = shell.evaluate((MockCommandLineInput) () ->
            String.format("temp query --sql 'select * from %s'", "table_non_exist"));
    assertEquals(TempViewCommand.QUERY_FAIL, result.toString());
  }

  @Test
  public void testQuery() {
    Object result = shell.evaluate((MockCommandLineInput) () ->
            String.format("temp query --sql 'select * from %s'", tableName));
    assertEquals(TempViewCommand.QUERY_SUCCESS, result.toString());
  }

  @Test
  public void testShowAll() {
    Object result = shell.evaluate(() -> "temps show");
    assertEquals(TempViewCommand.SHOW_SUCCESS, result.toString());
  }

  @Test
  public void testDelete() {
    Object result = shell.evaluate(() -> String.format("temp delete --view %s", tableName));
    assertTrue(result.toString().endsWith("successfully!"));

    // after delete, we can not access table yet.
    assertThrows(HoodieException.class, () -> HoodieCLI.getTempViewProvider().runQuery("select * from " + tableName));
  }
}
