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
import org.apache.hudi.common.util.ExternalFilePathUtil;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.storage.StoragePath;

import lombok.Getter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parser for Hudi base and log file names.
 *
 * <p>The parser accepts either a file name or a full path and always decodes only the last path component.
 * A non-matching input returns {@link Option#empty()} instead of throwing.</p>
 */
public final class FileNameParser {
  // Base files are of this pattern - <fg-id>_<write-token>_<instant>.<suffix>
  // Inline log files are of this pattern - .<fg-id>_<instant>.log.<version>_<write-token>
  // Inline archive log files are of this pattern - .commits_.archive.<version>_<write-token>
  // Native log files are of this pattern - <fg-id>_<write-token>_<instant>_<version>.log.<suffix>
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
   * Parses a base file name.
   *
   * <p>A Hudi-generated base file name is encoded as
   * {@code <fileId>_<writeToken>_<commitTime><fileExtension>}. For example,
   * {@code 136281f3-c24e-423b-a65a-95dbfbddce1d_1-0-1_20240706120100123.parquet}
   * has file id {@code 136281f3-c24e-423b-a65a-95dbfbddce1d}, write token {@code 1-0-1},
   * commit time {@code 20240706120100123}, and file extension {@code .parquet}. The decoded
   * {@link BaseFileName#getFileExtension()} value includes the leading dot when an extension is present.</p>
   *
   * <p>Decoding mirrors {@code HoodieBaseFile}'s historical substring scan for Hudi-generated names: the
   * first underscore terminates the file id, the last underscore seen before a commit-time delimiter
   * precedes the commit time, and the first dot after the second underscore terminates the commit time.
   * If the second underscore is absent, the parser treats the name as a legacy base file without a write
   * token and uses the first dot after the first underscore as the commit-time delimiter. If the delimiter
   * dot is absent, the commit time extends to the end of the file name. This looser fallback is kept to
   * preserve the historical {@code FSUtils} behavior for non-generated/external base file names such as
   * {@code file_1.parquet}; it is not the naming pattern produced by Hudi writers.</p>
   *
   * <p>Externally registered base files marked with {@code _hudiext} are decoded through
   * {@link ExternalFilePathUtil}. For these files, {@link BaseFileName#getWriteToken()} is empty and
   * {@link BaseFileName#getFileExtension()} is derived from the original external file name.</p>
   *
   * @param fileName file name or full path
   * @return decoded base file name when the input matches a supported base-file format
   */
  public static Option<BaseFileName> parseBaseFile(String fileName) {
    if (StringUtils.isNullOrEmpty(fileName)) {
      return Option.empty();
    }

    String actualFileName = getActualFileName(fileName);
    if (ExternalFilePathUtil.isExternallyCreatedFile(actualFileName)) {
      return parseExternalBaseFile(actualFileName);
    }

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
    if (firstUnderscoreIndex < 0) {
      return Option.empty();
    }
    if (secondUnderscoreIndex < 0) {
      // Preserve the old loose base-file parsing used by FSUtils for external/non-generated file names.
      int commitTimeEndIndex = actualFileName.indexOf(DOT, firstUnderscoreIndex + 1);
      return Option.of(new BaseFileName(
          actualFileName.substring(0, firstUnderscoreIndex),
          "",
          actualFileName.substring(firstUnderscoreIndex + 1, commitTimeEndIndex < 0 ? actualFileName.length() : commitTimeEndIndex),
          commitTimeEndIndex < 0 ? "" : actualFileName.substring(commitTimeEndIndex)));
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
    if (StringUtils.isNullOrEmpty(fileName)) {
      return Option.empty();
    }

    String actualFileName = getActualFileName(fileName);
    return parseNativeLogFileFromActualFileName(actualFileName);
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

  private static Option<BaseFileName> parseExternalBaseFile(String actualFileName) {
    String[] fileIdAndCommitTime = ExternalFilePathUtil.parseFileIdAndCommitTimeFromExternalFile(actualFileName);
    return Option.of(new BaseFileName(fileIdAndCommitTime[0], "", fileIdAndCommitTime[1],
        getFileExtension(fileIdAndCommitTime[0])));
  }

  private static String getFileExtension(String fileName) {
    int lastSeparatorIndex = fileName.lastIndexOf(StoragePath.SEPARATOR);
    int dotIndex = fileName.lastIndexOf(DOT);
    return dotIndex > lastSeparatorIndex ? fileName.substring(dotIndex) : "";
  }

  /**
   * Decoded parts of a base file name.
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
    // Inline logs use this as the optional CDC suffix (".cdc"), while native logs use it as
    // the native storage format suffix, such as "parquet".
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
