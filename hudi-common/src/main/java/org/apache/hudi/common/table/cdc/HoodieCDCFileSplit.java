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

import java.io.Serializable;

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
public class HoodieCDCFileSplit implements Serializable {

  /**
   * * the change type, which decide to how to retrieve the change data. more details see: `HoodieCDCLogicalFileType#`
   */
  private final HoodieCDCInferCase cdcInferCase;

  /**
   * the file that the change data can be parsed from.
   */
  private final String cdcFile;

  /**
   * the file slice that are required when retrieve the before data.
   */
  private final Option<FileSlice> beforeFileSlice;

  /**
   * the file slice that are required when retrieve the after data.
   */
  private final Option<FileSlice> afterFileSlice;

  public HoodieCDCFileSplit(HoodieCDCInferCase cdcInferCase, String cdcFile) {
    this(cdcInferCase, cdcFile, Option.empty(), Option.empty());
  }

  public HoodieCDCFileSplit(
      HoodieCDCInferCase cdcInferCase,
      String cdcFile,
      Option<FileSlice> beforeFileSlice,
      Option<FileSlice> afterFileSlice) {
    this.cdcInferCase = cdcInferCase;
    this.cdcFile = cdcFile;
    this.beforeFileSlice = beforeFileSlice;
    this.afterFileSlice = afterFileSlice;
  }

  public HoodieCDCInferCase getCdcInferCase() {
    return this.cdcInferCase;
  }

  public String getCdcFile() {
    return this.cdcFile;
  }

  public Option<FileSlice> getBeforeFileSlice() {
    return this.beforeFileSlice;
  }

  public Option<FileSlice> getAfterFileSlice() {
    return this.afterFileSlice;
  }
}
