/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.callback.common;

import org.apache.hudi.ApiMaturityLevel;
import org.apache.hudi.PublicAPIClass;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.util.Option;

import lombok.Getter;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Base callback message, which contains commitTime and tableName only for now.
 */
@PublicAPIClass(maturity = ApiMaturityLevel.EVOLVING)
public class HoodieWriteCommitCallbackMessage implements Serializable {

  private static final long serialVersionUID = -3033643980627719561L;

  /**
   * CommitTime for one batch write, this is required.
   */
  @Getter
  private final String commitTime;

  /**
   * Table name this batch commit to.
   */
  @Getter
  private final String tableName;

  /**
   * BathPath the table located.
   */
  @Getter
  private final String basePath;

  /**
   * Statistics about Hoodie write operation.
   */
  @Getter
  private final List<HoodieWriteStat> hoodieWriteStat;

  /**
   * Action Type of the commit.
   */
  @Getter
  private final Option<String> commitActionType;

  /**
   * Extra metadata in the commit.
   */
  @Getter
  private final Option<Map<String, String>> extraMetadata;

  public HoodieWriteCommitCallbackMessage(String commitTime,
                                          String tableName,
                                          String basePath,
                                          List<HoodieWriteStat> hoodieWriteStat) {
    this(commitTime, tableName, basePath, hoodieWriteStat, Option.empty(), Option.empty());
  }

  public HoodieWriteCommitCallbackMessage(String commitTime,
                                          String tableName,
                                          String basePath,
                                          List<HoodieWriteStat> hoodieWriteStat,
                                          Option<String> commitActionType,
                                          Option<Map<String, String>> extraMetadata) {
    this.commitTime = commitTime;
    this.tableName = tableName;
    this.basePath = basePath;
    this.hoodieWriteStat = hoodieWriteStat;
    this.commitActionType = commitActionType;
    this.extraMetadata = extraMetadata;
  }
}
