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

package org.apache.hudi.hadoop.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;

public class HudiHiveFileFormatUtils {

  /**
   * Get the list of aliases from the opeerator tree that are needed for the path
   *
   * @param pathToAliases mapping from path to aliases
   * @param dir           The path to look for
   **/
  public static List<String> doGetAliasesFromPath(
      Map<Path, ArrayList<String>> pathToAliases,
      Path dir) {
    if (pathToAliases == null) {
      return new ArrayList<String>();
    }
    Path path = getMatchingPath(pathToAliases, dir);
    return pathToAliases.get(path);
  }

  private static boolean foundAlias(Map<Path, ArrayList<String>> pathToAliases,
      Path path) {
    List<String> aliases = pathToAliases.get(path);
    if ((aliases == null) || (aliases.isEmpty())) {
      return false;
    }
    return true;
  }

  private static Path getMatchingPath(Map<Path, ArrayList<String>> pathToAliases,
      Path dir) {
    // First find the path to be searched
    Path path = dir;
    if (foundAlias(pathToAliases, path)) {
      return path;
    }

    Path dirPath = Path.getPathWithoutSchemeAndAuthority(dir);
    if (foundAlias(pathToAliases, dirPath)) {
      return dirPath;
    }

    while (path != null && dirPath != null) {
      path = path.getParent();
      dirPath = dirPath.getParent();
      //first try full match
      if (foundAlias(pathToAliases, path)) {
        return path;
      }
      if (foundAlias(pathToAliases, dirPath)) {
        return dirPath;
      }
    }
    return null;
  }

  /**
   * Get the list of operators from the operator tree that are needed for the path
   *
   * @param pathToAliases mapping from path to aliases
   * @param aliasToWork   The operator tree to be invoked for a given alias
   * @param dir           The path to look for
   **/
  public static List<Operator<? extends OperatorDesc>> doGetWorksFromPath(

      Map<Path, ArrayList<String>> pathToAliases,
      Map<String, Operator<? extends OperatorDesc>> aliasToWork, Path dir) {
    List<Operator<? extends OperatorDesc>> opList =
        new ArrayList<Operator<? extends OperatorDesc>>();

    List<String> aliases = doGetAliasesFromPath(pathToAliases, dir);
    for (String alias : aliases) {
      opList.add(aliasToWork.get(alias));
    }
    return opList;
  }

  public static PartitionDesc getPartitionDescFromPathRecursively(
      Map<Path, PartitionDesc> pathToPartitionInfo, Path dir,
      Map<Map<Path, PartitionDesc>, Map<Path, PartitionDesc>> cacheMap)
      throws IOException {
    return getPartitionDescFromPathRecursively(pathToPartitionInfo, dir,
        cacheMap, false);
  }

  public static PartitionDesc getPartitionDescFromPathRecursively(
      Map<Path, PartitionDesc> pathToPartitionInfo, Path dir,
      Map<Map<Path, PartitionDesc>, Map<Path, PartitionDesc>> cacheMap, boolean ignoreSchema)
      throws IOException {

    PartitionDesc part = doGetPartitionDescFromPath(pathToPartitionInfo, dir);

    if (part == null
        && (ignoreSchema
        || (dir.toUri().getScheme() == null || dir.toUri().getScheme().trim().equals(""))
        || FileUtils.pathsContainNoScheme(pathToPartitionInfo.keySet()))) {

      Map<Path, PartitionDesc> newPathToPartitionInfo = null;
      if (cacheMap != null) {
        newPathToPartitionInfo = cacheMap.get(pathToPartitionInfo);
      }

      if (newPathToPartitionInfo == null) { // still null
        newPathToPartitionInfo = populateNewPartitionDesc(pathToPartitionInfo);

        if (cacheMap != null) {
          cacheMap.put(pathToPartitionInfo, newPathToPartitionInfo);
        }
      }
      part = doGetPartitionDescFromPath(newPathToPartitionInfo, dir);
    }
    if (part != null) {
      return part;
    } else {
      throw new IOException("cannot find dir = " + dir.toString()
          + " in pathToPartitionInfo: " + pathToPartitionInfo.keySet());
    }
  }

  private static Map<Path, PartitionDesc> populateNewPartitionDesc(
      Map<Path, PartitionDesc> pathToPartitionInfo) {
    Map<Path, PartitionDesc> newPathToPartitionInfo = new HashMap<>();
    for (Map.Entry<Path, PartitionDesc> entry : pathToPartitionInfo.entrySet()) {
      PartitionDesc partDesc = entry.getValue();
      Path pathOnly = Path.getPathWithoutSchemeAndAuthority(entry.getKey());
      newPathToPartitionInfo.put(pathOnly, partDesc);
    }
    return newPathToPartitionInfo;
  }

  private static PartitionDesc doGetPartitionDescFromPath(
      Map<Path, PartitionDesc> pathToPartitionInfo, Path dir) {

    // We first do exact match, and then do prefix matching. The latter is due to input dir
    // could be /dir/ds='2001-02-21'/part-03 where part-03 is not part of partition
    Path path = getParentRegardlessOfScheme(dir, pathToPartitionInfo.keySet());

    if (path == null) {
      // FIXME: old implementation returned null; exception maybe?
      return null;
    }
    return pathToPartitionInfo.get(path);
  }

  public static Path getParentRegardlessOfScheme(Path path, Collection<Path> candidates) {
    Path schemalessPath = Path.getPathWithoutSchemeAndAuthority(path);

    for (; path != null && schemalessPath != null;
        path = path.getParent(), schemalessPath = schemalessPath.getParent()) {
      if (candidates.contains(path)) {
        return path;
      }
      if (candidates.contains(schemalessPath)) {
        return schemalessPath;
      }
    }
    // exception?
    return null;
  }

}
