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

package org.apache.hudi.hadoop.realtime;

import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.util.Option;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.mapred.FileSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link FileSplit} implementation that holds
 * <ol>
 *   <li>Split corresponding to the base file</li>
 *   <li>List of {@link HoodieLogFile} that holds the delta to be merged (upon reading)</li>
 * </ol>
 * <p>
 * This split is correspondent to a single file-slice in the Hudi terminology.
 * <p>
 * NOTE: If you're adding fields here you need to make sure that you appropriately de-/serialize them
 *       in {@link #readFromInput(DataInput)} and {@link #writeToOutput(DataOutput)}
 */
@Setter
public class HoodieRealtimeFileSplit extends FileSplit implements RealtimeSplit {
  /**
   * List of delta log-files holding updated records for this base-file
   */
  @Getter
  private List<HoodieLogFile> deltaLogFiles = new ArrayList<>();
  /**
   * Base path of the table this path belongs to
   */
  @Getter
  private String basePath;
  /**
   * Latest commit instant available at the time of the query in which all of the files
   * pertaining to this split are represented
   */
  @Getter
  private String maxCommitTime;
  /**
   * Marks whether this path produced as part of Incremental Query
   */
  private boolean belongsToIncrementalQuery = false;
  /**
   * Virtual key configuration of the table this split belongs to
   */
  @Getter
  private Option<HoodieVirtualKeyInfo> virtualKeyInfo = Option.empty();

  public HoodieRealtimeFileSplit() {
  }

  public HoodieRealtimeFileSplit(FileSplit baseSplit, HoodieRealtimePath path) throws IOException {
    this(baseSplit,
        path.getBasePath(),
        path.getDeltaLogFiles(),
        path.getMaxCommitTime(),
        path.getBelongsToIncrementalQuery(),
        path.getVirtualKeyInfo());
  }

  /**
   * @VisibleInTesting
   */
  public HoodieRealtimeFileSplit(FileSplit baseSplit,
                                 String basePath,
                                 List<HoodieLogFile> deltaLogFiles,
                                 String maxCommitTime,
                                 boolean belongsToIncrementalQuery,
                                 Option<HoodieVirtualKeyInfo> virtualKeyInfo)
      throws IOException {
    super(baseSplit.getPath(), baseSplit.getStart(), baseSplit.getLength(), baseSplit.getLocations());
    this.deltaLogFiles = deltaLogFiles;
    this.basePath = basePath;
    this.maxCommitTime = maxCommitTime;
    this.belongsToIncrementalQuery = belongsToIncrementalQuery;
    this.virtualKeyInfo = virtualKeyInfo;
  }

  @Override
  public boolean getBelongsToIncrementalQuery() {
    return belongsToIncrementalQuery;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    writeToOutput(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    readFromInput(in);
  }

  @Override
  public String toString() {
    return "HoodieRealtimeFileSplit{DataPath=" + getPath() + ", deltaLogPaths=" + getDeltaLogPaths()
        + ", maxCommitTime='" + maxCommitTime + '\'' + ", basePath='" + basePath + '\'' + '}';
  }
}
