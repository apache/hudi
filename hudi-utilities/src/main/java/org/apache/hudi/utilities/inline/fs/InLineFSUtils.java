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

package org.apache.hudi.utilities.inline.fs;

import org.apache.hadoop.fs.Path;

/**
 * Utils to parse InlineFileSystem paths.
 * Inline FS format: "inlinefs:/<path_to_outer_file>/<outer_file_scheme>/<start_offset>/<length>"
 * "inlinefs:/<path_to_outer_file>/<outer_file_scheme>/inline_file/?start_offset=start_offset>&length=<length>"
 */
public class InLineFSUtils {

  private static final String INLINE_FILE_STR = "inline_file";
  private static final String START_OFFSET_STR = "start_offset";
  private static final String LENGTH_STR = "length";
  private static final String EQUALS_STR = "=";

  /**
   * Fetch embedded inline file path from outer path.
   * Eg
   * Input:
   * Path = file:/file1, origScheme: file, startOffset = 20, length = 40
   * Output: "inlinefs:/file1/file/inline_file/?start_offset=20&length=40"
   *
   * @param outerPath
   * @param origScheme
   * @param inLineStartOffset
   * @param inLineLength
   * @return
   */
  public static Path getEmbeddedInLineFilePath(Path outerPath, String origScheme, long inLineStartOffset, int inLineLength) {
    String subPath = outerPath.toString().substring(outerPath.toString().indexOf(":") + 1);
    return new Path(
        InlineFileSystem.SCHEME + "://" + subPath + "/" + origScheme + "/" + INLINE_FILE_STR + "/"
            + "?" + START_OFFSET_STR + EQUALS_STR + inLineStartOffset + "&" + LENGTH_STR + EQUALS_STR + inLineLength
    );
  }

  /**
   * Eg input : "inlinefs:/file1/file/inline_file/?start_offset=20&length=40"
   * Output : "file:/file1"
   *
   * @param inlinePath
   * @param outerScheme
   * @return
   */
  public static Path getOuterfilePathFromInlinePath(Path inlinePath, String outerScheme) {
    String scheme = inlinePath.getParent().getParent().getName();
    Path basePath = inlinePath.getParent().getParent().getParent();
    return new Path(basePath.toString().replaceFirst(outerScheme, scheme));
  }

  /**
   * Eg input : "inlinefs:/file1/file/inline_file/?start_offset=20&length=40"
   * output: 20
   *
   * @param inlinePath
   * @return
   */
  public static int startOffset(Path inlinePath) {
    String pathName = inlinePath.getName();
    return Integer.parseInt(pathName.substring(pathName.indexOf('=') + 1, pathName.indexOf('&')));
  }

  /**
   * Eg input : "inlinefs:/file1/file/inline_file/?start_offset=20&length=40"
   * Output: 40
   *
   * @param inlinePath
   * @return
   */
  public static int length(Path inlinePath) {
    String pathName = inlinePath.getName();
    return Integer.parseInt(pathName.substring(pathName.lastIndexOf('=') + 1));
  }

}
