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

package org.apache.hudi.cdc

import org.apache.hudi.common.model.FileSlice
import org.apache.hudi.common.table.cdc.CDCFileTypeEnum

/**
 * This contains all the information that retrieve the change data at a single file group and
 * at a single commit.
 *
 * For [[cdcFileType]] = [[CDCFileTypeEnum.ADD_BASE_File]], [[cdcFile]] is a current version of
 *   the base file in the group, and [[beforeFileSlice]] is None.
 * For [[cdcFileType]] = [[CDCFileTypeEnum.REMOVE_BASE_File]], [[cdcFile]] is null,
 *   [[beforeFileSlice]] is the previous version of the base file in the group.
 * For [[cdcFileType]] = [[CDCFileTypeEnum.CDC_LOG_FILE]], [[cdcFile]] is a log file with cdc blocks.
 *   when enable the supplemental logging, both [[beforeFileSlice]] and [[afterFileSlice]] are None,
 *   otherwise these two are the previous and current version of the base file.
 * For [[cdcFileType]] = [[CDCFileTypeEnum.MOR_LOG_FILE]], [[cdcFile]] is a normal log file and
 *   [[beforeFileSlice]] is the previous version of the file slice.
 * For [[cdcFileType]] = [[CDCFileTypeEnum.REPLACED_FILE_GROUP]], [[cdcFile]] is null,
 *   [[beforeFileSlice]] is the current version of the file slice.
 *
 * @param cdcFileType the change type, which decide to how to retrieve the change data.
 *                    more details see: [[CDCFileTypeEnum]]
 * @param cdcFile the file that the change data can be parsed from.
 * @param beforeFileSlice the other files that are required when retrieve the change data.
 */
case class ChangeFileForSingleFileGroupAndCommit(
  cdcFileType: CDCFileTypeEnum,
  cdcFile: String,
  beforeFileSlice: Option[FileSlice] = None,
  afterFileSlice: Option[FileSlice] = None
) {
  assert(cdcFileType != null && (cdcFile != null || beforeFileSlice.nonEmpty))
}
