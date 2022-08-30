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
 *
 * For [[cdcFileType]] = [[CDCFileTypeEnum.ADD_BASE_FILE]], [[cdcFile]] is a current version of
 *   the base file in the group, and [[beforeFileSlice]] is None.
 * For [[cdcFileType]] = [[CDCFileTypeEnum.REMOVE_BASE_FILE]], [[cdcFile]] is null,
 *   [[beforeFileSlice]] is the previous version of the base file in the group.
 * For [[cdcFileType]] = [[CDCFileTypeEnum.CDC_LOG_FILE]], [[cdcFile]] is a log file with cdc blocks.
 *   when enable the supplemental logging, both [[beforeFileSlice]] and [[afterFileSlice]] are None,
 *   otherwise these two are the previous and current version of the base file.
 * For [[cdcFileType]] = [[CDCFileTypeEnum.MOR_LOG_FILE]], [[cdcFile]] is a normal log file and
 *   [[beforeFileSlice]] is the previous version of the file slice.
 * For [[cdcFileType]] = [[CDCFileTypeEnum.REPLACED_FILE_GROUP]], [[cdcFile]] is null,
 *   [[beforeFileSlice]] is the current version of the file slice.
 */
public class CDCFileSplit implements Serializable {

  /**
   * * the change type, which decide to how to retrieve the change data. more details see: [[CDCFileTypeEnum]]
   * */
  private HoodieCDCLogicalFileType cdcFileType;

  /**
   * the file that the change data can be parsed from.
   */
  private String cdcFile;

  /**
   * the file slice that are required when retrieve the pre_image data.
   */
  private Option<FileSlice> beforeFileSlice;

  /**
   * the file slice that are required when retrieve the post_image data.
   */
  private Option<FileSlice> afterFileSlice;

  public CDCFileSplit(HoodieCDCLogicalFileType cdcFileType, String cdcFile) {
    this(cdcFileType, cdcFile, Option.empty(), Option.empty());
  }

  public CDCFileSplit(
      HoodieCDCLogicalFileType cdcFileType,
      String cdcFile,
      Option<FileSlice> beforeFileSlice,
      Option<FileSlice> afterFileSlice) {
    this.cdcFileType = cdcFileType;
    this.cdcFile = cdcFile;
    this.beforeFileSlice = beforeFileSlice;
    this.afterFileSlice = afterFileSlice;
  }

  public HoodieCDCLogicalFileType getCdcFileType() {
    return this.cdcFileType;
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
