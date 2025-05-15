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

import java.util.List;

/**
 * WriteStatus handler callback to assist caller to process errors if any. Caller can dictate if we wanted to proceed with the commit or not by means of the return
 * value of the call back (processWriteStatuses). We noticed that sometimes callers invoke the dag just to process it there are any errors before proceeding with the commit.
 * With this callback, we are avoiding additional dag triggers from the callers side.
 */
public interface WriteStatusHandlerCallback {

  /**
   * Callback for processing the write status
   *
   * @param totalRecords total records in this inflight commit.
   * @param totalErroredRecords total error records in this infight commit.
   * @param leanWriteStatuses List of {@link WriteStatus} for the data table writes for this inflight commit.
   * @return True if the commit can proceed
   */
  boolean processWriteStatuses(long totalRecords, long totalErroredRecords, HoodieData<WriteStatus> leanWriteStatuses);
}