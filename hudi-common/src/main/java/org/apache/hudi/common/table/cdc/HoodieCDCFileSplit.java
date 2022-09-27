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

package org.apache.hudi.common.table.cdc;

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.util.Option;

import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * This contains all the information that retrieve the change data at a single file group and
 * at a single commit.
 * <p>
 * For `cdcInferCase` = {@link HoodieCDCInferCase#BASE_FILE_INSERT}, `cdcFile` is a current version of
 * the base file in the group, and `beforeFileSlice` is None.
 * For `cdcInferCase` = {@link HoodieCDCInferCase#BASE_FILE_DELETE}, `cdcFile` is null,
 * `beforeFileSlice` is the previous version of the base file in the group.
 * For `cdcInferCase` = {@link HoodieCDCInferCase#AS_IS}, `cdcFile` is a log file with cdc blocks.
 * when enable the supplemental logging, both `beforeFileSlice` and `afterFileSlice` are None,
 * otherwise these two are the previous and current version of the base file.
 * For `cdcInferCase` = {@link HoodieCDCInferCase#LOG_FILE}, `cdcFile` is a normal log file and
 * `beforeFileSlice` is the previous version of the file slice.
 * For `cdcInferCase` = {@link HoodieCDCInferCase#REPLACE_COMMIT}, `cdcFile` is null,
 * `beforeFileSlice` is the current version of the file slice.
 */
public class HoodieCDCFileSplit implements Serializable, Comparable<HoodieCDCFileSplit> {
  /**
   * The instant time at which the changes happened.
   */
  private final String instant;

  /**
   * Flag that decides to how to retrieve the change data. More details see: `HoodieCDCLogicalFileType`.
   */
  private final HoodieCDCInferCase cdcInferCase;

  /**
   * The file that the change data can be parsed from.
   */
  private final List<String> cdcFiles;

  /**
   * THe file slice that are required when retrieving the before data.
   */
  private final Option<FileSlice> beforeFileSlice;

  /**
   * The file slice that are required when retrieving the after data.
   */
  private final Option<FileSlice> afterFileSlice;

  public HoodieCDCFileSplit(String instant, HoodieCDCInferCase cdcInferCase, String cdcFile) {
    this(instant, cdcInferCase, cdcFile, Option.empty(), Option.empty());
  }

  public HoodieCDCFileSplit(String instant, HoodieCDCInferCase cdcInferCase, List<String> cdcFiles) {
    this(instant, cdcInferCase, cdcFiles, Option.empty(), Option.empty());
  }

  public HoodieCDCFileSplit(
      String instant,
      HoodieCDCInferCase cdcInferCase,
      String cdcFile,
      Option<FileSlice> beforeFileSlice,
      Option<FileSlice> afterFileSlice) {
    this(instant, cdcInferCase, Collections.singletonList(cdcFile), beforeFileSlice, afterFileSlice);
  }

  public HoodieCDCFileSplit(
      String instant,
      HoodieCDCInferCase cdcInferCase,
      List<String> cdcFileS,
      Option<FileSlice> beforeFileSlice,
      Option<FileSlice> afterFileSlice) {
    this.instant = instant;
    this.cdcInferCase = cdcInferCase;
    this.cdcFiles = cdcFileS;
    this.beforeFileSlice = beforeFileSlice;
    this.afterFileSlice = afterFileSlice;
  }

  public String getInstant() {
    return this.instant;
  }

  public HoodieCDCInferCase getCdcInferCase() {
    return this.cdcInferCase;
  }

  public List<String> getCdcFiles() {
    return this.cdcFiles;
  }

  public Option<FileSlice> getBeforeFileSlice() {
    return this.beforeFileSlice;
  }

  public Option<FileSlice> getAfterFileSlice() {
    return this.afterFileSlice;
  }

  @Override
  public int compareTo(@NotNull HoodieCDCFileSplit o) {
    return this.instant.compareTo(o.instant);
  }
}
