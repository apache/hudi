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

/**
 * Utils to parse InLineFileSystem paths.
 * Inline FS format:
 * "inlinefs://<path_to_outer_file>/<outer_file_scheme>/?start_offset=start_offset>&length=<length>"
 * Eg: "inlinefs://<path_to_outer_file>/s3a/?start_offset=20&length=40"
 */
public class InLineFSUtils {
  private static final String START_OFFSET_STR = "start_offset";
  private static final String LENGTH_STR = "length";
  private static final String EQUALS_STR = "=";

  /**
   * Fetch inline file path from outer path.
   * Eg
   * Input:
   * Path = s3a://file1, origScheme: file, startOffset = 20, length = 40
   * Output: "inlinefs:/file1/s3a/?start_offset=20&length=40"
   *
   * @param outerPath
   * @param origScheme
   * @param inLineStartOffset
   * @param inLineLength
   * @return
   */
  public static Path getInlineFilePath(Path outerPath, String origScheme, long inLineStartOffset, long inLineLength) {
    String subPath = outerPath.toString().substring(outerPath.toString().indexOf(":") + 1);
    return new Path(
        InLineFileSystem.SCHEME + "://" + subPath + "/" + origScheme
            + "/" + "?" + START_OFFSET_STR + EQUALS_STR + inLineStartOffset
            + "&" + LENGTH_STR + EQUALS_STR + inLineLength
    );
  }

  /**
   * Inline file format
   * "inlinefs://<path_to_outer_file>/<outer_file_scheme>/?start_offset=start_offset>&length=<length>"
   * Outer File format
   * "<outer_file_scheme>://<path_to_outer_file>"
   * <p>
   * Eg input : "inlinefs://file1/sa3/?start_offset=20&length=40".
   * Output : "sa3://file1"
   *
   * @param inlinePath inline file system path
   * @return
   */
  public static Path getOuterfilePathFromInlinePath(Path inlinePath) {
    String scheme = inlinePath.getParent().getName();
    Path basePath = inlinePath.getParent().getParent();
    return new Path(basePath.toString().replaceFirst(InLineFileSystem.SCHEME, scheme));
  }

  /**
   * Eg input : "inlinefs://file1/s3a/?start_offset=20&length=40".
   * output: 20
   *
   * @param inlinePath
   * @return
   */
  public static int startOffset(Path inlinePath) {
    String[] slices = inlinePath.toString().split("[?&=]");
    return Integer.parseInt(slices[slices.length - 3]);
  }

  /**
   * Eg input : "inlinefs:/file1/s3a/?start_offset=20&length=40".
   * Output: 40
   *
   * @param inlinePath
   * @return
   */
  public static int length(Path inlinePath) {
    String[] slices = inlinePath.toString().split("[?&=]");
    return Integer.parseInt(slices[slices.length - 1]);
  }

}
