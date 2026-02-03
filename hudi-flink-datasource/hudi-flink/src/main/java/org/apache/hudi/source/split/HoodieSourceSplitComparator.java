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

package org.apache.hudi.source.split;

import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;

import static org.apache.hudi.common.fs.FSUtils.getCommitTime;

/**
 * Commit time based Hoodie source split comparator.
 * <p>
 * This comparator orders splits by their latest commit time in ascending order.
 * When two splits have the same latest commit time, they are ordered by the commit
 * time extracted from their base file path.
 */
public class HoodieSourceSplitComparator implements SerializableComparator<HoodieSourceSplit> {

  @Override
  public int compare(HoodieSourceSplit o1, HoodieSourceSplit o2) {
    ValidationUtils.checkArgument(
        !StringUtils.isNullOrEmpty(o1.getLatestCommit()),
        "The latest commit field of split can't be null or empty: " + o1);

    ValidationUtils.checkArgument(
        !StringUtils.isNullOrEmpty(o2.getLatestCommit()),
        "The latest commit field of split can't be null or empty: " + o2);

    int commitComparison = CharSequence.compare(o1.getLatestCommit(), o2.getLatestCommit());
    if (commitComparison == 0) {
      String commitTime1 = o1.getBasePath()
          .map(path -> getCommitTime(path))
          .orElse("");
      String commitTime2 = o2.getBasePath()
          .map(path -> getCommitTime(path))
          .orElse("");
      return CharSequence.compare(commitTime1, commitTime2);
    }
    return commitComparison;
  }
}
