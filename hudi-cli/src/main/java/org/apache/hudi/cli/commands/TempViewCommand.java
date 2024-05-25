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
import org.apache.hudi.exception.HoodieException;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

/**
 * CLI command to query/delete temp views.
 */
@ShellComponent
public class TempViewCommand {

  public static final String QUERY_SUCCESS = "Query ran successfully!";
  public static final String QUERY_FAIL = "Query ran failed!";
  public static final String SHOW_SUCCESS = "Show all views name successfully!";

  @ShellMethod(key = {"temp_query", "temp query"}, value = "query against created temp view")
  public String query(
      @ShellOption(value = {"--sql"}, help = "select query to run against view") final String sql) {

    try {
      HoodieCLI.getTempViewProvider().runQuery(sql);
      return QUERY_SUCCESS;
    } catch (HoodieException ex) {
      return QUERY_FAIL;
    }

  }

  @ShellMethod(key = {"temps_show", "temps show"}, value = "Show all views name")
  public String showAll() {

    try {
      HoodieCLI.getTempViewProvider().showAllViews();
      return SHOW_SUCCESS;
    } catch (HoodieException ex) {
      return "Show all views name failed!";
    }
  }

  @ShellMethod(key = {"temp_delete", "temp delete"}, value = "Delete view name")
  public String delete(
      @ShellOption(value = {"--view"}, help = "view name") final String tableName) {

    try {
      HoodieCLI.getTempViewProvider().deleteTable(tableName);
      return String.format("Delete view %s successfully!", tableName);
    } catch (HoodieException ex) {
      return String.format("Delete view %s failed!", tableName);
    }
  }
}
