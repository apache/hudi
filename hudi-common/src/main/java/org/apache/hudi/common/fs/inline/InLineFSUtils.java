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

package org.apache.hudi.common.fs.inline;

import org.apache.hadoop.fs.Path;

import java.io.File;

import static org.apache.hudi.common.util.ValidationUtils.checkArgument;

/**
 * Utils to parse InLineFileSystem paths.
 * Inline FS format:
 * "inlinefs://<path_to_outer_file>/<outer_file_scheme>/?start_offset=start_offset>&length=<length>"
 * Eg: "inlinefs://<path_to_outer_file>/s3a/?start_offset=20&length=40"
 */
public class InLineFSUtils {
  private static final String START_OFFSET_STR = "start_offset";
  private static final String LENGTH_STR = "length";
  private static final String PATH_SEPARATOR = "/";
  private static final String SCHEME_SEPARATOR = ":";
  private static final String EQUALS_STR = "=";
  private static final String LOCAL_FILESYSTEM_SCHEME = "file";

  /**
   * Get the InlineFS Path for a given schema and its Path.
   * <p>
   * Examples:
   * Input Path: s3a://file1, origScheme: file, startOffset = 20, length = 40
   * Output: "inlinefs://file1/s3a/?start_offset=20&length=40"
   *
   * @param outerPath         The outer file Path
   * @param origScheme        The file schema
   * @param inLineStartOffset Start offset for the inline file
   * @param inLineLength      Length for the inline file
   * @return InlineFS Path for the requested outer path and schema
   */
  public static Path getInlineFilePath(Path outerPath, String origScheme, long inLineStartOffset, long inLineLength) {
    final String subPath = new File(outerPath.toString().substring(outerPath.toString().indexOf(":") + 1)).getPath();
    return new Path(
        InLineFileSystem.SCHEME + SCHEME_SEPARATOR + PATH_SEPARATOR + subPath + PATH_SEPARATOR + origScheme
            + PATH_SEPARATOR + "?" + START_OFFSET_STR + EQUALS_STR + inLineStartOffset
            + "&" + LENGTH_STR + EQUALS_STR + inLineLength
    );
  }

  /**
   * InlineFS Path format:
   * "inlinefs://path/to/outer/file/outer_file_scheme/?start_offset=start_offset>&length=<length>"
   * <p>
   * Outer File Path format:
   * "outer_file_scheme://path/to/outer/file"
   * <p>
   * Example
   * Input: "inlinefs://file1/s3a/?start_offset=20&length=40".
   * Output: "s3a://file1"
   *
   * @param inlineFSPath InLineFS Path to get the outer file Path
   * @return Outer file Path from the InLineFS Path
   */
  public static Path getOuterFilePathFromInlinePath(Path inlineFSPath) {
    assertInlineFSPath(inlineFSPath);

    final String outerFileScheme = inlineFSPath.getParent().getName();
    final Path basePath = inlineFSPath.getParent().getParent();
    checkArgument(basePath.toString().contains(SCHEME_SEPARATOR),
        "Invalid InLineFS path: " + inlineFSPath);

    final String pathExceptScheme = basePath.toString().substring(basePath.toString().indexOf(SCHEME_SEPARATOR) + 1);
    final String fullPath = outerFileScheme + SCHEME_SEPARATOR
        + (outerFileScheme.equals(LOCAL_FILESYSTEM_SCHEME) ? PATH_SEPARATOR : "")
        + pathExceptScheme;
    return new Path(fullPath);
  }

  /**
   * Returns start offset w/in the base for the block identified by the given InlineFS path
   *
   * input: "inlinefs://file1/s3a/?start_offset=20&length=40".
   * output: 20
   */
  public static long startOffset(Path inlineFSPath) {
    assertInlineFSPath(inlineFSPath);

    String[] slices = inlineFSPath.toString().split("[?&=]");
    return Long.parseLong(slices[slices.length - 3]);
  }

  /**
   * Returns length of the block (embedded w/in the base file) identified by the given InlineFS path
   *
   * input: "inlinefs:/file1/s3a/?start_offset=20&length=40".
   * output: 40
   */
  public static long length(Path inlinePath) {
    assertInlineFSPath(inlinePath);

    String[] slices = inlinePath.toString().split("[?&=]");
    return Long.parseLong(slices[slices.length - 1]);
  }

  private static void assertInlineFSPath(Path inlinePath) {
    String scheme = inlinePath.toUri().getScheme();
    checkArgument(InLineFileSystem.SCHEME.equals(scheme));
  }
}
