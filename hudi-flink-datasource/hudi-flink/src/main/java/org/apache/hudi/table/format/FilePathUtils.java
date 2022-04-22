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

package org.apache.hudi.table.format;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.configuration.FlinkOptions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Reference the Flink {@link org.apache.flink.table.utils.PartitionPathUtils}
 * but supports simple partition path besides the Hive style.
 */
public class FilePathUtils {

  private static final Pattern HIVE_PARTITION_NAME_PATTERN = Pattern.compile("([^/]+)=([^/]+)");

  private static final BitSet CHAR_TO_ESCAPE = new BitSet(128);

  static {
    for (char c = 0; c < ' '; c++) {
      CHAR_TO_ESCAPE.set(c);
    }

    /*
     * ASCII 01-1F are HTTP control characters that need to be escaped.
     * \u000A and \u000D are \n and \r, respectively.
     */
    char[] clist = new char[] {'\u0001', '\u0002', '\u0003', '\u0004',
        '\u0005', '\u0006', '\u0007', '\u0008', '\u0009', '\n', '\u000B',
        '\u000C', '\r', '\u000E', '\u000F', '\u0010', '\u0011', '\u0012',
        '\u0013', '\u0014', '\u0015', '\u0016', '\u0017', '\u0018', '\u0019',
        '\u001A', '\u001B', '\u001C', '\u001D', '\u001E', '\u001F',
        '"', '#', '%', '\'', '*', '/', ':', '=', '?', '\\', '\u007F', '{',
        '[', ']', '^'};

    for (char c : clist) {
      CHAR_TO_ESCAPE.set(c);
    }
  }

  private static boolean needsEscaping(char c) {
    return c < CHAR_TO_ESCAPE.size() && CHAR_TO_ESCAPE.get(c);
  }

  /**
   * Make partition path from partition spec.
   *
   * @param partitionKVs  The partition key value mapping
   * @param hivePartition Whether the partition path is with Hive style,
   *                      e.g. {partition key} = {partition value}
   * @param sepSuffix     Whether to append the path separator as suffix
   * @return an escaped, valid partition name
   */
  public static String generatePartitionPath(
      LinkedHashMap<String, String> partitionKVs,
      boolean hivePartition,
      boolean sepSuffix) {
    if (partitionKVs.isEmpty()) {
      return "";
    }
    StringBuilder suffixBuf = new StringBuilder();
    int i = 0;
    for (Map.Entry<String, String> e : partitionKVs.entrySet()) {
      if (i > 0) {
        suffixBuf.append(Path.SEPARATOR);
      }
      if (hivePartition) {
        suffixBuf.append(escapePathName(e.getKey()));
        suffixBuf.append('=');
      }
      suffixBuf.append(escapePathName(e.getValue()));
      i++;
    }
    if (sepSuffix) {
      suffixBuf.append(Path.SEPARATOR);
    }
    return suffixBuf.toString();
  }

  /**
   * Escapes a path name.
   *
   * @param path The path to escape.
   * @return An escaped path name.
   */
  private static String escapePathName(String path) {
    if (path == null || path.length() == 0) {
      throw new TableException("Path should not be null or empty: " + path);
    }

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < path.length(); i++) {
      char c = path.charAt(i);
      if (needsEscaping(c)) {
        sb.append('%');
        sb.append(String.format("%1$02X", (int) c));
      } else {
        sb.append(c);
      }
    }
    return sb.toString();
  }

  /**
   * Generates partition key value mapping from path.
   *
   * @param currPath      Partition file path
   * @param hivePartition Whether the partition path is with Hive style
   * @param partitionKeys Partition keys
   * @return Sequential partition specs.
   */
  public static LinkedHashMap<String, String> extractPartitionKeyValues(
      Path currPath,
      boolean hivePartition,
      String[] partitionKeys) {
    LinkedHashMap<String, String> fullPartSpec = new LinkedHashMap<>();
    if (partitionKeys.length == 0) {
      return fullPartSpec;
    }
    List<String[]> kvs = new ArrayList<>();
    int curDepth = 0;
    do {
      String component = currPath.getName();
      final String[] kv = new String[2];
      if (hivePartition) {
        Matcher m = HIVE_PARTITION_NAME_PATTERN.matcher(component);
        if (m.matches()) {
          String k = unescapePathName(m.group(1));
          String v = unescapePathName(m.group(2));
          kv[0] = k;
          kv[1] = v;
        }
      } else {
        kv[0] = partitionKeys[partitionKeys.length - 1 - curDepth];
        kv[1] = unescapePathName(component);
      }
      kvs.add(kv);
      currPath = currPath.getParent();
      curDepth++;
    } while (currPath != null && !currPath.getName().isEmpty() && curDepth < partitionKeys.length);

    // reverse the list since we checked the part from leaf dir to table's base dir
    for (int i = kvs.size(); i > 0; i--) {
      fullPartSpec.put(kvs.get(i - 1)[0], kvs.get(i - 1)[1]);
    }

    return fullPartSpec;
  }

  public static String unescapePathName(String path) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < path.length(); i++) {
      char c = path.charAt(i);
      if (c == '%' && i + 2 < path.length()) {
        int code = -1;
        try {
          code = Integer.parseInt(path.substring(i + 1, i + 3), 16);
        } catch (Exception ignored) {
          // do nothing
        }
        if (code >= 0) {
          sb.append((char) code);
          i += 2;
          continue;
        }
      }
      sb.append(c);
    }
    return sb.toString();
  }

  /**
   * Search all partitions in this path.
   *
   * @param fs            File system
   * @param path          Search path
   * @param hivePartition Whether the partition path is with Hive style
   * @param partitionKeys Partition keys
   * @return all partition key value mapping in sequence of the given path
   */
  public static List<Tuple2<LinkedHashMap<String, String>, Path>> searchPartKeyValueAndPaths(
      FileSystem fs,
      Path path,
      boolean hivePartition,
      String[] partitionKeys) {
    // expectLevel start from 0, E.G. base_path/level0/level1/level2
    FileStatus[] generatedParts = getFileStatusRecursively(path, partitionKeys.length, fs);
    List<Tuple2<LinkedHashMap<String, String>, Path>> ret = new ArrayList<>();
    for (FileStatus part : generatedParts) {
      ret.add(
          new Tuple2<>(
              extractPartitionKeyValues(part.getPath(), hivePartition, partitionKeys),
              part.getPath()));
    }
    return ret;
  }

  public static FileStatus[] getFileStatusRecursively(Path path, int expectLevel, Configuration conf) {
    return getFileStatusRecursively(path, expectLevel, FSUtils.getFs(path.toString(), conf));
  }

  public static FileStatus[] getFileStatusRecursively(Path path, int expectLevel, FileSystem fs) {
    ArrayList<FileStatus> result = new ArrayList<>();

    try {
      FileStatus fileStatus = fs.getFileStatus(path);
      listStatusRecursively(fs, fileStatus, 0, expectLevel, result);
    } catch (IOException ignore) {
      return new FileStatus[0];
    }

    return result.toArray(new FileStatus[0]);
  }

  private static void listStatusRecursively(
      FileSystem fs,
      FileStatus fileStatus,
      int level,
      int expectLevel,
      List<FileStatus> results) throws IOException {
    if (expectLevel == level && !isHiddenFile(fileStatus)) {
      results.add(fileStatus);
      return;
    }

    if (fileStatus.isDirectory() && !isHiddenFile(fileStatus)) {
      for (FileStatus stat : fs.listStatus(fileStatus.getPath())) {
        listStatusRecursively(fs, stat, level + 1, expectLevel, results);
      }
    }
  }

  private static boolean isHiddenFile(FileStatus fileStatus) {
    String name = fileStatus.getPath().getName();
    // the log files is hidden file
    return name.startsWith("_") || (name.startsWith(".") && !name.contains(".log."));
  }

  /**
   * Returns the partition path key and values as a list of map, each map item in the list
   * is a mapping of the partition key name to its actual partition value. For example, say
   * there is a file path with partition keys [key1, key2, key3]:
   *
   * <p><pre>
   *   -- file:/// ... key1=val1/key2=val2/key3=val3
   *   -- file:/// ... key1=val4/key2=val5/key3=val6
   * </pre>
   *
   * <p>The return list should be [{key1:val1, key2:val2, key3:val3}, {key1:val4, key2:val5, key3:val6}].
   *
   * @param path           The base path
   * @param hadoopConf     The hadoop configuration
   * @param partitionKeys  The partition key list
   * @param defaultParName The default partition name for nulls
   * @param hivePartition  Whether the partition path is in Hive style
   */
  public static List<Map<String, String>> getPartitions(
      Path path,
      Configuration hadoopConf,
      List<String> partitionKeys,
      String defaultParName,
      boolean hivePartition) {
    try {
      return FilePathUtils
          .searchPartKeyValueAndPaths(
              FSUtils.getFs(path.toString(), hadoopConf),
              path,
              hivePartition,
              partitionKeys.toArray(new String[0]))
          .stream()
          .map(tuple2 -> tuple2.f0)
          .map(spec -> {
            LinkedHashMap<String, String> ret = new LinkedHashMap<>();
            spec.forEach((k, v) -> ret.put(k, defaultParName.equals(v) ? null : v));
            return ret;
          })
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new TableException("Fetch partitions fail.", e);
    }
  }

  /**
   * Reorder the partition key value mapping based on the given partition keys sequence.
   *
   * @param partitionKVs  The partition key and value mapping
   * @param partitionKeys The partition key list
   */
  public static LinkedHashMap<String, String> validateAndReorderPartitions(
      Map<String, String> partitionKVs,
      List<String> partitionKeys) {
    LinkedHashMap<String, String> map = new LinkedHashMap<>();
    for (String k : partitionKeys) {
      if (!partitionKVs.containsKey(k)) {
        throw new TableException("Partition keys are: " + partitionKeys
            + ", incomplete partition spec: " + partitionKVs);
      }
      map.put(k, partitionKVs.get(k));
    }
    return map;
  }

  /**
   * Returns all the file paths that is the parents of the data files.
   *
   * @param path          The base path
   * @param conf          The Flink configuration
   * @param hadoopConf    The hadoop configuration
   * @param partitionKeys The partition key list
   */
  public static Path[] getReadPaths(
      Path path,
      org.apache.flink.configuration.Configuration conf,
      Configuration hadoopConf,
      List<String> partitionKeys) {
    if (partitionKeys.isEmpty()) {
      return new Path[] {path};
    } else {
      final String defaultParName = conf.getString(FlinkOptions.PARTITION_DEFAULT_NAME);
      final boolean hivePartition = conf.getBoolean(FlinkOptions.HIVE_STYLE_PARTITIONING);
      List<Map<String, String>> partitionPaths =
          getPartitions(path, hadoopConf, partitionKeys, defaultParName, hivePartition);
      return partitionPath2ReadPath(path, partitionKeys, partitionPaths, hivePartition);
    }
  }

  /**
   * Transforms the given partition key value mapping to read paths.
   *
   * @param path           The base path
   * @param partitionKeys  The partition key list
   * @param partitionPaths The partition key value mapping
   * @param hivePartition  Whether the partition path is in Hive style
   * @see #getReadPaths
   */
  public static Path[] partitionPath2ReadPath(
      Path path,
      List<String> partitionKeys,
      List<Map<String, String>> partitionPaths,
      boolean hivePartition) {
    return partitionPaths.stream()
        .map(m -> validateAndReorderPartitions(m, partitionKeys))
        .map(kvs -> FilePathUtils.generatePartitionPath(kvs, hivePartition, true))
        .map(n -> new Path(path, n))
        .toArray(Path[]::new);
  }

  /**
   * Transforms the given partition key value mapping to relative partition paths.
   *
   * @param partitionKeys  The partition key list
   * @param partitionPaths The partition key value mapping
   * @param hivePartition  Whether the partition path is in Hive style
   * @see #getReadPaths
   */
  public static Set<String> toRelativePartitionPaths(
      List<String> partitionKeys,
      List<Map<String, String>> partitionPaths,
      boolean hivePartition) {
    return partitionPaths.stream()
        .map(m -> validateAndReorderPartitions(m, partitionKeys))
        .map(kvs -> FilePathUtils.generatePartitionPath(kvs, hivePartition, false))
        .collect(Collectors.toSet());
  }

  /**
   * Transforms the array of Hadoop paths to Flink paths.
   */
  public static org.apache.flink.core.fs.Path[] toFlinkPaths(Path[] paths) {
    return Arrays.stream(paths)
        .map(FilePathUtils::toFlinkPath)
        .toArray(org.apache.flink.core.fs.Path[]::new);
  }

  /**
   * Transforms the Hadoop path to Flink path.
   */
  public static org.apache.flink.core.fs.Path toFlinkPath(Path path) {
    return new org.apache.flink.core.fs.Path(path.toUri());
  }

  /**
   * Extracts the partition keys with given configuration.
   *
   * @param conf The flink configuration
   * @return array of the partition fields
   */
  public static String[] extractPartitionKeys(org.apache.flink.configuration.Configuration conf) {
    if (FlinkOptions.isDefaultValueDefined(conf, FlinkOptions.HIVE_STYLE_PARTITIONING)) {
      return new String[0];
    }
    return conf.getString(FlinkOptions.HIVE_STYLE_PARTITIONING).split(",");
  }
}
