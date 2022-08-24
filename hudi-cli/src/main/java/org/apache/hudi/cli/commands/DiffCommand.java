/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.cli.commands;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

@Component
public class DiffCommand implements CommandMarker {

  private static final Logger LOG = LogManager.getLogger(DiffCommand.class);

  @CliCommand(value = "diff file", help = "Check how file differs across range of commits")
  public String diffFile(
      @CliOption(key = {"fileId"}, help = "File ID to diff across range of commits", mandatory = true) String fileId,
      @CliOption(key = {"includeExtraMetadata"}, help = "Include extra metadata", unspecifiedDefaultValue = "false") final boolean includeExtraMetadata,
      @CliOption(key = {"startTs"}, help = "start time for compactions, default: now - 10 days") String startTs,
      @CliOption(key = {"endTs"}, help = "end time for compactions, default: now - 1 day") String endTs,
      @CliOption(key = {"limit"}, help = "Limit compactions", unspecifiedDefaultValue = "-1") final Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") final String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") final boolean descending,
      @CliOption(key = {"headeronly"}, help = "Print Header Only", unspecifiedDefaultValue = "false") final boolean headerOnly) {
    // TODO: implement me
    return null;
  }

  @CliCommand(value = "diff partition", help = "Check how file differs across range of commits")
  public String diffPartition(
      @CliOption(key = {"partitionPath"}, help = "Relative partition path to diff across range of commits", mandatory = true) String partitionPath,
      @CliOption(key = {"includeExtraMetadata"}, help = "Include extra metadata", unspecifiedDefaultValue = "false") final boolean includeExtraMetadata,
      @CliOption(key = {"startTs"}, help = "start time for compactions, default: now - 10 days") String startTs,
      @CliOption(key = {"endTs"}, help = "end time for compactions, default: now - 1 day") String endTs,
      @CliOption(key = {"limit"}, help = "Limit compactions", unspecifiedDefaultValue = "-1") final Integer limit,
      @CliOption(key = {"sortBy"}, help = "Sorting Field", unspecifiedDefaultValue = "") final String sortByField,
      @CliOption(key = {"desc"}, help = "Ordering", unspecifiedDefaultValue = "false") final boolean descending,
      @CliOption(key = {"headeronly"}, help = "Print Header Only", unspecifiedDefaultValue = "false") final boolean headerOnly) {
    // TODO: implement me
    return null;
  }
}
