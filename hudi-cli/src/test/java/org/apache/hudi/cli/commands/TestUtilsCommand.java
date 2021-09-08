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

import org.apache.hudi.cli.functional.CLIFunctionalTestHarness;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.shell.core.CommandResult;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class for {@link org.apache.hudi.cli.commands.UtilsCommand}.
 */
@Tag("functional")
public class TestUtilsCommand extends CLIFunctionalTestHarness {

  /**
   * Test case for success load class.
   */
  @Test
  public void testLoadClass() {
    String name = HoodieTable.class.getName();
    CommandResult cr = shell().executeCommand(String.format("utils loadClass --class %s", name));
    assertAll("Command runs success",
        () -> assertTrue(cr.isSuccess()),
        () -> assertNotNull(cr.getResult().toString()),
        () -> assertTrue(cr.getResult().toString().startsWith("file:")));
  }

  /**
   * Test case for class not found.
   */
  @Test
  public void testLoadClassNotFound() {
    String name = "test.class.NotFound";
    CommandResult cr = shell().executeCommand(String.format("utils loadClass --class %s", name));

    assertAll("Command runs success",
        () -> assertTrue(cr.isSuccess()),
        () -> assertNotNull(cr.getResult().toString()),
        () -> assertEquals(cr.getResult().toString(), String.format("Class %s not found!", name)));
  }

  /**
   * Test case for load null class.
   */
  @Test
  public void testLoadClassNull() {
    String name = "";
    CommandResult cr = shell().executeCommand(String.format("utils loadClass --class %s", name));

    assertAll("Command runs success",
        () -> assertTrue(cr.isSuccess()),
        () -> assertNotNull(cr.getResult().toString()),
        () -> assertEquals("Class to be loaded can not be null!", cr.getResult().toString()));
  }
}
