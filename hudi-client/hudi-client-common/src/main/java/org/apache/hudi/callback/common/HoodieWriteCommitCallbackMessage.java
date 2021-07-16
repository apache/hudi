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

import java.io.Serializable;
import java.util.List;

/**
 * Base callback message, which contains commitTime and tableName only for now.
 */
@PublicAPIClass(maturity = ApiMaturityLevel.EVOLVING)
public class HoodieWriteCommitCallbackMessage implements Serializable {

  private static final long serialVersionUID = -3033643980627719561L;

  /**
   * CommitTime for one batch write, this is required.
   */
  private final String commitTime;

  /**
   * Table name this batch commit to.
   */
  private final String tableName;

  /**
   * BathPath the table located.
   */
  private final String basePath;

  /**
   * Statistics about Hoodie write operation.
   */
  private final List<HoodieWriteStat> hoodieWriteStat;

  public HoodieWriteCommitCallbackMessage(String commitTime, String tableName, String basePath, List<HoodieWriteStat> hoodieWriteStat) {
    this.commitTime = commitTime;
    this.tableName = tableName;
    this.basePath = basePath;
    this.hoodieWriteStat = hoodieWriteStat;
  }

  public String getCommitTime() {
    return commitTime;
  }

  public String getTableName() {
    return tableName;
  }

  public String getBasePath() {
    return basePath;
  }

  public List<HoodieWriteStat> getHoodieWriteStat() {
    return hoodieWriteStat;
  }
}
