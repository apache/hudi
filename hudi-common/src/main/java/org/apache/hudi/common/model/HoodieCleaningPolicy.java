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

import org.apache.hudi.common.config.EnumDefault;
import org.apache.hudi.common.config.EnumDescription;
import org.apache.hudi.common.config.EnumFieldDescription;

/**
 * Hoodie cleaning policies.
 */
@EnumDescription("Policy to be used by the cleaner for deleting older file slices to re-claim space.")
public enum HoodieCleaningPolicy {

  @EnumFieldDescription("Retain the most recent N file slices in each file group, determined by `hoodie.cleaner.fileversions.retained`.")
  KEEP_LATEST_FILE_VERSIONS,

  @EnumDefault
  @EnumFieldDescription("Retain the file slices written by the last N commits, determined by `hoodie.cleaner.commits.retained`.")
  KEEP_LATEST_COMMITS,

  @EnumFieldDescription("Retain commits from the last N hours, determined by `hoodie.cleaner.hours.retained`.")
  KEEP_LATEST_BY_HOURS
}
