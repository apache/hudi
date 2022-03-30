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

import org.apache.hudi.cli.HoodiePrintHelper;
import org.apache.hudi.cli.functional.CLIFunctionalTestHarness;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.shell.core.CommandResult;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test Cases for {@link SparkEnvCommand}.
 */
@Tag("functional")
public class TestSparkEnvCommand extends CLIFunctionalTestHarness {

  /**
   * Test Cases for set and get spark env.
   */
  @Test
  public void testSetAndGetSparkEnv() {
    // First, be empty
    CommandResult cr = shell().executeCommand("show envs all");
    String nullResult = HoodiePrintHelper.print(new String[] {"key", "value"}, new String[0][2]);
    nullResult = removeNonWordAndStripSpace(nullResult);
    String got = removeNonWordAndStripSpace(cr.getResult().toString());
    assertEquals(nullResult, got);

    // Set SPARK_HOME
    cr = shell().executeCommand("set --conf SPARK_HOME=/usr/etc/spark");
    assertTrue(cr.isSuccess());

    //Get
    cr = shell().executeCommand("show env --key SPARK_HOME");
    String result = HoodiePrintHelper.print(new String[] {"key", "value"}, new String[][] {new String[] {"SPARK_HOME", "/usr/etc/spark"}});
    result = removeNonWordAndStripSpace(result);
    got = removeNonWordAndStripSpace(cr.getResult().toString());
    assertEquals(result, got);
  }
}
