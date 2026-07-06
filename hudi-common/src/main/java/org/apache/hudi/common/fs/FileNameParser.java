/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.fs;

import org.apache.hudi.common.table.cdc.HoodieCDCUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.storage.StoragePath;

import lombok.Getter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parser for Hudi-generated base and log file names.
 *
 * <p>The parser accepts either a file name or a full path and always decodes only the last path component.
 * A non-matching input returns {@link Option#empty()} instead of throwing.</p>
 */
public final class FileNameParser {
  // Inline log files are of this pattern - .b5068208-e1a4-11e6-bf01-fe55135034f3_20170101134598.log.1_1-0-1
  // Inline archive log files are of this pattern - .commits_.archive.1_1-0-1
  // Native log files are of this pattern - b5068208-e1a4-11e6-bf01-fe55135034f3_1-0-1_20170101134598_1.log.parquet
  // For native log files, the file extension is log/deletes/cdc and the suffix is the native file format.

  static final Pattern INLINE_LOG_FILE_PATTERN =
      Pattern.compile("^\\.([^._]+)_([^.]*)\\.(log|archive)\\.(\\d+)(_((\\d+)-(\\d+)-(\\d+))(\\.cdc)?)?$");
  static final Pattern NATIVE_LOG_FILE_PATTERN =
      Pattern.compile("^([^._]+)_((\\d+)-(\\d+)-(\\d+))_([^_]+)_(\\d+)\\.(log|deletes|cdc)\\.([^.]+)$");
  private static final Pattern PREFIX_BY_FILE_ID_PATTERN = Pattern.compile("^(.+)-(\\d+)");
  private static final char UNDERSCORE = '_';
  private static final char DOT = '.';

  private FileNameParser() {
  }

  /**
   * Parses a Hudi-generated base file name.
   *
   * <p>Expected format: {@code <fileId>_<writeToken>_<commitTime>[.<extension>]}.
   * Decoding mirrors {@code HoodieBaseFile}'s historical substring scan: the first underscore terminates
   * the file id, the second underscore precedes the commit time, and the first dot after the second
   * underscore terminates the commit time. If the dot is absent, the commit time extends to the end of
   * the file name. The decoded {@link BaseFileName#getFileExtension()} value includes the leading dot when
   * an extension is present.</p>
   *
   * @param fileName file name or full path
   * @return decoded base file name when the input matches the Hudi base-file format
   */
  public static Option<BaseFileName> parseBaseFile(String fileName) {
    if (StringUtils.isNullOrEmpty(fileName)) {
      return Option.empty();
    }

    String actualFileName = getActualFileName(fileName);
    int firstUnderscoreIndex = -1;
    int secondUnderscoreIndex = -1;
    int dotIndex = -1;
    short underscoreCount = 0;
    for (int i = 0; i < actualFileName.length(); i++) {
      char c = actualFileName.charAt(i);
      if (c == UNDERSCORE) {
        if (underscoreCount == 0) {
          firstUnderscoreIndex = i;
        } else if (underscoreCount == 1) {
          secondUnderscoreIndex = i;
        }
        underscoreCount++;
      } else if (c == DOT && underscoreCount == 2) {
        dotIndex = i;
        break;
      }
    }
    if (firstUnderscoreIndex < 0 || secondUnderscoreIndex < 0) {
      return Option.empty();
    }
    int commitTimeEndIndex = dotIndex < 0 ? actualFileName.length() : dotIndex;
    return Option.of(new BaseFileName(
        actualFileName.substring(0, firstUnderscoreIndex),
        actualFileName.substring(firstUnderscoreIndex + 1, secondUnderscoreIndex),
        actualFileName.substring(secondUnderscoreIndex + 1, commitTimeEndIndex),
        dotIndex < 0 ? "" : actualFileName.substring(dotIndex)));
  }

  /**
   * Parses either an inline log file name or a native log file name.
   *
   * <p>Inline log format: {@code .<fileId>_<deltaCommitTime>.<extension>.<version>_<writeToken>[.cdc]}.
   * Native log format: {@code <fileId>_<writeToken>_<deltaCommitTime>_<version>.<extension>.<nativeFormat>}.
   * The decoded {@link LogFileName#getFileExtension()} value does not include a leading dot. For inline CDC
   * logs, {@link LogFileName#getSuffix()} is {@code .cdc}; for native logs, it is the native storage format,
   * such as {@code parquet}.</p>
   *
   * @param fileName file name or full path
   * @return decoded log file name when the input matches either supported log-file format
   */
  public static Option<LogFileName> parseLogFile(String fileName) {
    if (StringUtils.isNullOrEmpty(fileName)) {
      return Option.empty();
    }

    String actualFileName = getActualFileName(fileName);
    Option<LogFileName> nativeLogFileName = parseNativeLogFileFromActualFileName(actualFileName);
    if (nativeLogFileName.isPresent()) {
      return nativeLogFileName;
    }

    Matcher matcher = INLINE_LOG_FILE_PATTERN.matcher(actualFileName);
    return matcher.matches() ? Option.of(LogFileName.fromInlineLogFile(matcher)) : Option.empty();
  }

  /**
   * Parses only a native log file name.
   *
   * <p>Use {@link #parseLogFile(String)} when both inline and native log formats are acceptable.</p>
   *
   * @param fileName file name or full path
   * @return decoded native log file name when the input matches the native log-file format
   */
  public static Option<LogFileName> parseNativeLogFile(String fileName) {
    return matchNativeLogFile(fileName).map(LogFileName::fromNativeLogFile);
  }

  /**
   * Returns the file group prefix encoded in a Hudi file id.
   *
   * @param fileId Hudi file id ending with a numeric suffix separated by {@code -}
   * @return file id prefix before the trailing numeric suffix
   * @throws HoodieValidationException when the file id does not contain the expected suffix
   */
  public static String getFileIdPfxFromFileId(String fileId) {
    Matcher matcher = PREFIX_BY_FILE_ID_PATTERN.matcher(fileId);
    if (!matcher.find()) {
      throw new HoodieValidationException("Failed to get prefix from " + fileId);
    }
    return matcher.group(1);
  }

  static Option<Matcher> matchNativeLogFile(String fileName) {
    if (StringUtils.isNullOrEmpty(fileName)) {
      return Option.empty();
    }

    String actualFileName = getActualFileName(fileName);
    return matchNativeLogFileFromActualFileName(actualFileName);
  }

  private static Option<LogFileName> parseNativeLogFileFromActualFileName(String actualFileName) {
    return matchNativeLogFileFromActualFileName(actualFileName).map(LogFileName::fromNativeLogFile);
  }

  private static Option<Matcher> matchNativeLogFileFromActualFileName(String actualFileName) {
    Matcher matcher = NATIVE_LOG_FILE_PATTERN.matcher(actualFileName);
    return matcher.matches() ? Option.of(matcher) : Option.empty();
  }

  private static String getActualFileName(String fileName) {
    return fileName.contains(StoragePath.SEPARATOR)
        ? fileName.substring(fileName.lastIndexOf(StoragePath.SEPARATOR) + 1)
        : fileName;
  }

  /**
   * Decoded parts of a Hudi-generated base file name.
   */
  @Getter
  public static final class BaseFileName {
    private final String fileId;
    private final String writeToken;
    private final String commitTime;
    private final String fileExtension;

    private BaseFileName(String fileId, String writeToken, String commitTime, String fileExtension) {
      this.fileId = fileId;
      this.writeToken = writeToken;
      this.commitTime = commitTime;
      this.fileExtension = fileExtension;
    }

  }

  /**
   * Decoded parts of a Hudi-generated log file name.
   */
  @Getter
  public static final class LogFileName {
    private final String fileId;
    private final String deltaCommitTime;
    private final int logVersion;
    private final String writeToken;
    private final Integer taskPartitionId;
    private final Integer stageId;
    private final Integer taskAttemptId;
    private final String fileExtension;
    private final String suffix;
    private final boolean nativeLogFile;

    private LogFileName(String fileId, String deltaCommitTime, int logVersion, String writeToken,
                        Integer taskPartitionId, Integer stageId, Integer taskAttemptId,
                        String fileExtension, String suffix, boolean nativeLogFile) {
      this.fileId = fileId;
      this.deltaCommitTime = deltaCommitTime;
      this.logVersion = logVersion;
      this.writeToken = writeToken;
      this.taskPartitionId = taskPartitionId;
      this.stageId = stageId;
      this.taskAttemptId = taskAttemptId;
      this.fileExtension = fileExtension;
      this.suffix = suffix;
      this.nativeLogFile = nativeLogFile;
    }

    private static LogFileName fromInlineLogFile(Matcher matcher) {
      String taskPartitionId = matcher.group(7);
      String stageId = matcher.group(8);
      String taskAttemptId = matcher.group(9);
      return new LogFileName(
          matcher.group(1),
          matcher.group(2),
          Integer.parseInt(matcher.group(4)),
          matcher.group(6),
          taskPartitionId == null ? null : Integer.parseInt(taskPartitionId),
          stageId == null ? null : Integer.parseInt(stageId),
          taskAttemptId == null ? null : Integer.parseInt(taskAttemptId),
          matcher.group(3),
          matcher.group(10) == null ? "" : matcher.group(10),
          false);
    }

    private static LogFileName fromNativeLogFile(Matcher matcher) {
      return new LogFileName(
          matcher.group(1),
          matcher.group(6),
          Integer.parseInt(matcher.group(7)),
          matcher.group(2),
          Integer.parseInt(matcher.group(3)),
          Integer.parseInt(matcher.group(4)),
          Integer.parseInt(matcher.group(5)),
          matcher.group(8),
          matcher.group(9),
          true);
    }

    public boolean isCDCLogFile() {
      return nativeLogFile
          ? HoodieCDCUtils.CDC_LOGFILE_SUFFIX.substring(1).equals(fileExtension)
          : HoodieCDCUtils.CDC_LOGFILE_SUFFIX.equals(suffix);
    }
  }
}
