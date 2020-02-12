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

import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * CLI command to query/delete temp views.
 */
@Component
public class TempViewCommand implements CommandMarker {

  private static final String EMPTY_STRING = "";

  @CliCommand(value = "temp_query", help = "query against created temp view")
  public String query(
          @CliOption(key = {"sql"}, mandatory = true, help = "select query to run against view") final String sql)
          throws IOException {

    HoodieCLI.getTempViewProvider().runQuery(sql);
    return EMPTY_STRING;
  }

  @CliCommand(value = "temp_delete", help = "Delete view name")
  public String delete(
          @CliOption(key = {"view"}, mandatory = true, help = "view name") final String tableName)
          throws IOException {

    HoodieCLI.getTempViewProvider().deleteTable(tableName);
    return EMPTY_STRING;
  }
}
