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

package org.apache.hudi.util;

import org.apache.hudi.common.util.StringUtils;

import java.util.List;
import java.util.function.Predicate;

public class PartitionPathFilterUtil {

  public static Predicate<String> relativePathPrefixPredicate(List<String> relativePathPrefixes) {
    return path -> relativePathPrefixes.stream().anyMatch(relativePathPrefix ->
        // Partition paths stored in metadata table do not have the slash at the end.
        // If the relativePathPrefix is empty, return all partition paths;
        // else if the relative path prefix is the same as the path, this is an exact match;
        // else, we need to make sure the path is a subdirectory of relativePathPrefix, by
        // checking if the path starts with relativePathPrefix appended by a slash ("/").
        StringUtils.isNullOrEmpty(relativePathPrefix)
            || path.equals(relativePathPrefix) || path.startsWith(relativePathPrefix + "/"));
  }
}
