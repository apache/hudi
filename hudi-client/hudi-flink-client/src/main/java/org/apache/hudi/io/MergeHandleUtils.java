/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.io;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieValidationException;

/**
 * Utilities for flink merge handle.
 */
public class MergeHandleUtils {
  /**
   * Calculate the rollover number for base files during multiple mini-batches writing during one checkpoint interval.
   */
  protected static int calcRollNumberForBaseFile(String oldFileName, String writeToken) {
    if (StringUtils.isNullOrEmpty(oldFileName)) {
      throw new HoodieValidationException("Unexpected old file name: " + oldFileName + "in the merge handle");
    }
    String oldFileToken = FSUtils.getWriteTokenFromBaseFile(oldFileName);
    int rollNumber;
    if (oldFileToken.equals(writeToken)) {
      rollNumber = 0;
    } else if (oldFileToken.contains(writeToken)) {
      int prevRollNumber = Integer.parseInt(oldFileToken.substring(oldFileToken.indexOf(writeToken) + writeToken.length() + 1));
      rollNumber = prevRollNumber + 1;
    } else {
      throw new HoodieValidationException("Unexpected old file name: " + oldFileName +  "in the merge handle");
    }
    return rollNumber;
  }
}
