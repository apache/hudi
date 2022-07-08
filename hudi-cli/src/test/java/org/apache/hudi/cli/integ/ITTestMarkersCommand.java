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

package org.apache.hudi.cli.integ;

import org.apache.hadoop.fs.Path;
import org.apache.hudi.cli.commands.TableCommand;
import org.apache.hudi.cli.testutils.HoodieCLIIntegrationTestBase;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.testutils.FileCreateUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.shell.core.CommandResult;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test class for {@link org.apache.hudi.cli.commands.MarkersCommand}.
 * <p/>
 * A command use SparkLauncher need load jars under lib which generate during mvn package.
 * Use integration test instead of unit test.
 */
public class ITTestMarkersCommand extends HoodieCLIIntegrationTestBase {

  private String tablePath;

  @BeforeEach
  public void init() throws IOException {
    String tableName = "test_table";
    tablePath = basePath + Path.SEPARATOR + tableName;

    // Create table and connect
    new TableCommand().createTable(
        tablePath, "test_table", HoodieTableType.COPY_ON_WRITE.name(),
        "", TimelineLayoutVersion.VERSION_1, "org.apache.hudi.common.model.HoodieAvroPayload");
  }

  /**
   * Test case of command 'marker delete'.
   */
  @Test
  public void testDeleteMarker() throws IOException {
    // generate markers
    String instantTime1 = "101";

    FileCreateUtils.createMarkerFile(tablePath, "partA", instantTime1, "f0", IOType.APPEND);
    FileCreateUtils.createMarkerFile(tablePath, "partA", instantTime1, "f1", IOType.APPEND);

    assertEquals(2, FileCreateUtils.getTotalMarkerFileCount(tablePath, "partA", instantTime1, IOType.APPEND));

    CommandResult cr = getShell().executeCommand(
        String.format("marker delete --commit %s --sparkMaster %s", instantTime1, "local"));
    assertTrue(cr.isSuccess());

    assertEquals(0, FileCreateUtils.getTotalMarkerFileCount(tablePath, "partA", instantTime1, IOType.APPEND));
  }
}
