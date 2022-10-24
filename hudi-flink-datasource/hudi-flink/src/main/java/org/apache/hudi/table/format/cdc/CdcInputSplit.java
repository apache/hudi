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

package org.apache.hudi.table.format.cdc;

import org.apache.hudi.common.table.cdc.HoodieCDCFileSplit;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;

import java.util.Arrays;

/**
 * Represents an input split of source, actually a data bucket.
 */
public class CdcInputSplit extends MergeOnReadInputSplit {
  private static final long serialVersionUID = 1L;

  private HoodieCDCFileSplit[] changes;

  public CdcInputSplit(
      int splitNum,
      String tablePath,
      long maxCompactionMemoryInBytes,
      String fileId,
      HoodieCDCFileSplit[] changes) {
    super(splitNum, null, Option.empty(), "", tablePath,
        maxCompactionMemoryInBytes, "", null, fileId);
    this.changes = changes;
  }

  public HoodieCDCFileSplit[] getChanges() {
    return changes;
  }

  public void setChanges(HoodieCDCFileSplit[] changes) {
    this.changes = changes;
  }

  @Override
  public String toString() {
    return "CdcInputSplit{"
        + "changes=" + Arrays.toString(changes)
        + ", fileId='" + fileId + '\''
        + '}';
  }
}
