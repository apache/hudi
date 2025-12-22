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

package org.apache.hudi.storage.inline;

import org.apache.hudi.storage.StoragePath;

import java.io.File;

import static org.apache.hudi.common.util.ValidationUtils.checkArgument;

public class InLineFSUtils {

  public static final String SCHEME = "inlinefs";
  protected static final String START_OFFSET_STR = "start_offset";
  protected static final String LENGTH_STR = "length";
  protected static final String SCHEME_SEPARATOR = "" + StoragePath.COLON_CHAR;
  protected static final String EQUALS_STR = "=";
  protected static final String LOCAL_FILESYSTEM_SCHEME = "file";

  /**
   * Get the InlineFS Path for a given schema and its Path.
   * <p>
   * Examples:
   * Input Path: s3a://file1, origScheme: file, startOffset = 20, length = 40
   * Output: "inlinefs://file1/s3a/?start_offset=20&length=40"
   *
   * @param outerPath         The outer file path
   * @param origScheme        The file schema
   * @param inLineStartOffset Start offset for the inline file
   * @param inLineLength      Length for the inline file
   * @return InlineFS {@link StoragePath} for the requested outer path and schema
   */
  public static StoragePath getInlineFilePath(StoragePath outerPath,
                                              String origScheme,
                                              long inLineStartOffset,
                                              long inLineLength) {
    final String subPath = new File(outerPath.toString().substring(outerPath.toString().indexOf(":") + 1)).getPath();
    return new StoragePath(
        SCHEME + SCHEME_SEPARATOR
            + StoragePath.SEPARATOR + subPath + StoragePath.SEPARATOR + origScheme
            + StoragePath.SEPARATOR + "?" + START_OFFSET_STR + EQUALS_STR + inLineStartOffset
            + "&" + LENGTH_STR + EQUALS_STR + inLineLength
    );
  }

  public static StoragePath getOuterFilePathFromInlinePath(StoragePath inlineFSPath) {
    assertInlineFSPath(inlineFSPath);

    final String outerFileScheme = inlineFSPath.getParent().getName();
    final StoragePath basePath = inlineFSPath.getParent().getParent();
    checkArgument(basePath.toString().contains(SCHEME_SEPARATOR),
        "Invalid InLineFS path: " + inlineFSPath);

    final String pathExceptScheme = basePath.toString().substring(basePath.toString().indexOf(SCHEME_SEPARATOR) + 1);
    final String fullPath = outerFileScheme + SCHEME_SEPARATOR
        + (outerFileScheme.equals(LOCAL_FILESYSTEM_SCHEME) ? StoragePath.SEPARATOR : "")
        + pathExceptScheme;
    return new StoragePath(fullPath);
  }

  /**
   * Returns start offset w/in the base for the block identified by the given InlineFS path
   *
   * input: "inlinefs://file1/s3a/?start_offset=20&length=40".
   * output: 20
   */
  public static long startOffset(StoragePath inlineFSPath) {
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
  public static long length(StoragePath inlinePath) {
    assertInlineFSPath(inlinePath);

    String[] slices = inlinePath.toString().split("[?&=]");
    return Long.parseLong(slices[slices.length - 1]);
  }

  private static void assertInlineFSPath(StoragePath inlinePath) {
    String scheme = inlinePath.toUri().getScheme();
    checkArgument(SCHEME.equals(scheme));
  }
}
