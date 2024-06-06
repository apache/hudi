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

package org.apache.hudi.storage;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.inline.InLineFSUtils;

/**
 * Methods borrowed from FSUtils
 * */
public class StorageIOUtils {

  public static final Pattern LOG_FILE_PATTERN =
      Pattern.compile("^\\.(.+)_(.*)\\.(log|archive)\\.(\\d+)(_((\\d+)-(\\d+)-(\\d+))(.cdc)?)?");

  private static final String LOG_FILE_EXTENSION = ".log";

  public static boolean isBaseFile(StoragePath path) {
    String extension = getFileExtension(path.getName());
    return HoodieFileFormat.BASE_FILE_EXTENSIONS.contains(extension);
  }

  public static boolean isLogFile(StoragePath logPath) {
    String scheme = logPath.toUri().getScheme();
    return isLogFile(InLineFSUtils.SCHEME.equals(scheme)
        ? InLineFSUtils.getOuterFilePathFromInlinePath(logPath).getName() : logPath.getName());
  }

  public static boolean isLogFile(String fileName) {
    if (fileName.contains(LOG_FILE_EXTENSION)) {
      Matcher matcher = LOG_FILE_PATTERN.matcher(fileName);
      return matcher.find();
    }
    return false;
  }

  public static boolean isDataFile(StoragePath path) {
    return isBaseFile(path) || isLogFile(path);
  }

  public static String getFileExtension(String fullName) {
    Objects.requireNonNull(fullName);
    String fileName = new File(fullName).getName();
    int dotIndex = fileName.lastIndexOf('.');
    return dotIndex == -1 ? "" : fileName.substring(dotIndex);
  }

  public static String getFileIdFromLogPath(StoragePath path) {
    Matcher matcher = LOG_FILE_PATTERN.matcher(path.getName());
    if (!matcher.find()) {
      throw new HoodieException("Invalid path " + path + " of type LogFile");
    }
    return matcher.group(1);
  }

  public static String getFileId(String fullFileName) {
    return fullFileName.split("_", 2)[0];
  }

  public static String getFileIdFromFilePath(StoragePath filePath) {
    if (isLogFile(filePath)) {
      return getFileIdFromLogPath(filePath);
    }
    return getFileId(filePath.getName());
  }

  public static String getRelativePartitionPath(StoragePath basePath, StoragePath fullPartitionPath) {
    basePath = getPathWithoutSchemeAndAuthority(basePath);
    fullPartitionPath = getPathWithoutSchemeAndAuthority(fullPartitionPath);

    String fullPartitionPathStr = fullPartitionPath.toString();
    String basePathString = basePath.toString();

    if (!fullPartitionPathStr.startsWith(basePathString)) {
      throw new IllegalArgumentException("Partition path \"" + fullPartitionPathStr
          + "\" does not belong to base-path \"" + basePath + "\"");
    }

    // Partition-Path could be empty for non-partitioned tables
    return fullPartitionPathStr.length() == basePathString.length() ? ""
        : fullPartitionPathStr.substring(basePathString.length() + 1);
  }

  public static StoragePath getPathWithoutSchemeAndAuthority(StoragePath path) {
    return path.getPathWithoutSchemeAndAuthority();
  }

  /**
   * Hoodie file format. Borrowed from hudi-common
   */
  public enum HoodieFileFormat {

    PARQUET(".parquet"),

    HOODIE_LOG(".log"),

    HFILE(".hfile"),

    ORC(".orc");

    public static final Set<String> BASE_FILE_EXTENSIONS = Arrays.stream(HoodieFileFormat.values())
        .map(HoodieFileFormat::getFileExtension)
        .filter(x -> !x.equals(HoodieFileFormat.HOODIE_LOG.getFileExtension()))
        .collect(Collectors.toCollection(HashSet::new));

    private final String extension;

    HoodieFileFormat(String extension) {
      this.extension = extension;
    }

    public String getFileExtension() {
      return extension;
    }
  }
}
