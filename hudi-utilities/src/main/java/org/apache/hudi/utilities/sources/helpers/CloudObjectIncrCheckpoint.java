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

package org.apache.hudi.utilities.sources.helpers;

import org.apache.hudi.common.util.Option;

import static org.apache.hudi.common.util.StringUtils.isNullOrEmpty;
import static org.apache.hudi.utilities.sources.helpers.IncrSourceHelper.DEFAULT_BEGIN_TIMESTAMP;

/**
 * This POJO is used to craft checkpoints that supports size based batching
 * This object will be use by object based Hudi incr sources (s3/gcs)
 */
public class CloudObjectIncrCheckpoint {

  private final String commit;
  private final String key;

  public CloudObjectIncrCheckpoint(String commit, String key) {
    this.commit = commit;
    this.key = key;
  }

  public String getCommit() {
    return commit;
  }

  public String getKey() {
    return key;
  }

  public static CloudObjectIncrCheckpoint fromString(Option<String> lastCheckpoint) {
    if (lastCheckpoint.isPresent()) {
      Option<String[]> splitResult = lastCheckpoint.map(str -> str.split("#", 2));
      if (splitResult.isPresent() && splitResult.get().length == 2) {
        String[] split = splitResult.get();
        return new CloudObjectIncrCheckpoint(split[0], split[1]);
      } else {
        return new CloudObjectIncrCheckpoint(lastCheckpoint.get(), null);
      }
    }
    return new CloudObjectIncrCheckpoint(DEFAULT_BEGIN_TIMESTAMP, null);
  }

  @Override
  public String toString() {
    if (isNullOrEmpty(commit) && isNullOrEmpty(key)) {
      return DEFAULT_BEGIN_TIMESTAMP;
    } else if (isNullOrEmpty(key)) {
      return commit;
    }
    return commit + "#" + key;
  }
}
