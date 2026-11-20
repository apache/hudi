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

package org.apache.hudi.utilities.util;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.storage.StoragePath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A partition path in the one form two sides can be compared in: relative to the table root, no
 * scheme, no authority, no leading or trailing slash.
 *
 * <p>Hudi metadata mixes absolute and relative forms for the same partition, and a raw-string
 * comparison silently answers "not found" for every file in a partition spelled the other way.
 * Where that answer feeds a dangling verdict it would condemn the whole partition in one run, so
 * every comparison of a partition key goes through here.
 */
public final class PartitionPaths {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionPaths.class);

  private PartitionPaths() {
  }

  /**
   * <p>The normalisation itself is {@link FSUtils#getRelativePartitionPath}, the helper the rest of
   * the engine uses, rather than a private rewrite of it -- the prefix test below is only there to
   * decide whether the string is a path below the table at all, because that helper throws when it
   * is not and a relative key is the ordinary case. Anything it cannot place is trimmed and returned
   * as it stands, which is the pre-existing raw-string comparison and no worse than it.
   *
   * <p>Never throws: an unparseable partition string falls back to itself.
   */
  public static String relative(StoragePath basePath, String partition) {
    if (partition == null || partition.isEmpty()) {
      // The non-partitioned table's partition, and the only value that legitimately normalises to
      // nothing. Kept distinct from a failure so it can still match a commit's own "" key.
      return "";
    }
    try {
      StoragePath candidate = new StoragePath(partition);
      String bare = FSUtils.getPathWithoutSchemeAndAuthority(candidate).toString();
      String bareBase = FSUtils.getPathWithoutSchemeAndAuthority(basePath).toString();
      // Equality or a whole path segment, never a bare prefix: "/tmp/table" is not a parent of
      // "/tmp/table2", and substring arithmetic on that pair produces a plausible-looking wrong
      // answer rather than an error.
      if (bare.equals(bareBase) || bare.startsWith(bareBase + StoragePath.SEPARATOR)) {
        return FSUtils.getRelativePartitionPath(basePath, candidate);
      }
      return trimSeparators(bare);
    } catch (Exception ex) {
      LOG.warn("Could not normalise partition path {}; comparing it as it stands", partition, ex);
      return trimSeparators(partition);
    }
  }

  private static String trimSeparators(String path) {
    int from = 0;
    int to = path.length();
    while (from < to && path.charAt(from) == '/') {
      from++;
    }
    while (to > from && path.charAt(to - 1) == '/') {
      to--;
    }
    return path.substring(from, to);
  }
}
