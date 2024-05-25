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

package org.apache.hudi.integ.command;

import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieTableType;

import org.apache.hudi.integ.HoodieTestHiveBase;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test class for HoodieSyncCommand in hudi-cli module.
 */
public class ITTestHoodieSyncCommand extends HoodieTestHiveBase {

  private static final String HUDI_CLI_TOOL = HOODIE_WS_ROOT + "/hudi-cli/hudi-cli.sh";
  private static final String SYNC_VALIDATE_COMMANDS = HOODIE_WS_ROOT + "/docker/demo/sync-validate.commands";

  @Test
  public void testValidateSync() throws Exception {
    String hiveTableName = "docker_hoodie_sync_valid_test";
    String hiveTableName2 = "docker_hoodie_sync_valid_test_2";

    generateDataByHoodieJavaApp(
        hiveTableName, HoodieTableType.COPY_ON_WRITE.name(), PartitionType.SINGLE_KEY_PARTITIONED, "overwrite", hiveTableName);

    syncHoodieTable(hiveTableName2, "INSERT");

    generateDataByHoodieJavaApp(
        hiveTableName, HoodieTableType.COPY_ON_WRITE.name(), PartitionType.SINGLE_KEY_PARTITIONED, "append", hiveTableName);

    TestExecStartResultCallback result =
        executeCommandStringInDocker(ADHOC_1_CONTAINER, HUDI_CLI_TOOL + " script --file " + SYNC_VALIDATE_COMMANDS, true);

    String expected = String.format("Count difference now is (count(%s) - count(%s) == %d. Catch up count is %d",
        hiveTableName, hiveTableName2, 100, 200);
    assertTrue(result.getStdout().toString().contains(expected));

    dropHiveTables(hiveTableName, HoodieTableType.COPY_ON_WRITE.name());
    dropHiveTables(hiveTableName2, HoodieTableType.COPY_ON_WRITE.name());
  }

  private void syncHoodieTable(String hiveTableName, String op) throws Exception {
    StringBuilder cmdBuilder = new StringBuilder("spark-submit")
        .append(" --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer ").append(HUDI_UTILITIES_BUNDLE)
        .append(" --table-type COPY_ON_WRITE ")
        .append(" --base-file-format ").append(HoodieFileFormat.PARQUET.toString())
        .append(" --source-class org.apache.hudi.utilities.sources.HoodieIncrSource --source-ordering-field timestamp ")
        .append(" --target-base-path ").append(getHDFSPath(hiveTableName))
        .append(" --target-table ").append(hiveTableName)
        .append(" --op ").append(op)
        .append(" --props file:///var/hoodie/ws/docker/demo/config/hoodie-incr.properties")
        .append(" --enable-hive-sync");
    executeCommandStringInDocker(ADHOC_1_CONTAINER, cmdBuilder.toString(), true);
  }
}
