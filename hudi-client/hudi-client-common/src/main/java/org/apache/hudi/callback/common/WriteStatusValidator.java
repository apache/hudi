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

package org.apache.hudi.callback.common;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.util.Option;

/**
 * WriteStatus validator to assist caller to process errors if any. Caller can dictate if to proceed with the commit or not by means of the return
 * value of the method {@link WriteStatusValidator#validate}.
 *
 * <p>Sometimes callers invoke the dag just to process if there are any errors before proceeding with the commit.
 * This hook function is introduced for avoiding additional dag triggers from the callers side.
 */
public interface WriteStatusValidator {

  /**
   * Validates the given write status.
   *
   * @param totalRecords        Total records in this inflight commit.
   * @param totalErroredRecords Total error records in this inflight commit.
   * @param writeStatusesOpt    List of {@link WriteStatus} for the data table writes for this inflight commit.
   *
   * @return True if the commit can proceed
   */
  boolean validate(long totalRecords, long totalErroredRecords, Option<HoodieData<WriteStatus>> writeStatusesOpt);
}