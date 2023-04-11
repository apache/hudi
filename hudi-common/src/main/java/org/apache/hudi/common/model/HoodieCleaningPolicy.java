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

package org.apache.hudi.common.model;

import org.apache.hudi.common.config.EnumDescription;
import org.apache.hudi.common.config.EnumFieldDescription;

/**
 * Hoodie cleaning policies.
 */
@EnumDescription("Cleaning policy to be used. The cleaner service deletes older file "
    + "slices files to re-claim space. Long running query plans may often refer to older "
    + "file slices and will break if those are cleaned, before the query has had a chance "
    + "to run. So, it is good to make sure that the data is retained for more than the "
    + "maximum query execution time. ")
public enum HoodieCleaningPolicy {

  @EnumFieldDescription("The KEEP_LATEST_FILE_VERSIONS cleaning "
      + "policy is used, which keeps the last N versions of the file slices written, "
      + "determined by \"hoodie.cleaner.fileversions.retained\"; ")
  KEEP_LATEST_FILE_VERSIONS,

  @EnumFieldDescription("The KEEP_LATEST_COMMITS cleaning policy is "
      + "used, which keeps the file slices written by the last N commits, determined by "
      + "\"hoodie.cleaner.commits.retained\"; ")
  KEEP_LATEST_COMMITS,

  @EnumFieldDescription("The KEEP_LATEST_BY_HOURS cleaning policy is "
      + "used, which keeps the file slices written in the last N hours based on the commit "
      + "time, determined by \"hoodie.cleaner.hours.retained\"; ")
  KEEP_LATEST_BY_HOURS
}
