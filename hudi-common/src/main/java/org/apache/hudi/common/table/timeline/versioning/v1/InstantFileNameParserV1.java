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

package org.apache.hudi.common.table.timeline.versioning.v1;

import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.InstantFileNameParser;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class InstantFileNameParserV1 implements InstantFileNameParser {

  private static final Pattern NAME_FORMAT = Pattern.compile("^(\\d+)(\\.\\w+)(\\.\\D+)?$");

  public String extractTimestamp(String fileName) throws IllegalArgumentException {
    Matcher matcher = NAME_FORMAT.matcher(fileName);
    if (matcher.find()) {
      return matcher.group(1);
    }
    throw new IllegalArgumentException("Failed to retrieve timestamp from name: "
        + String.format(HoodieInstant.FILE_NAME_FORMAT_ERROR, fileName));
  }

  public String getTimelineFileExtension(String fileName) {
    Objects.requireNonNull(fileName);
    Matcher matcher = NAME_FORMAT.matcher(fileName);
    if (matcher.find()) {
      return fileName.substring(matcher.group(1).length());
    }
    return HoodieInstant.EMPTY_FILE_EXTENSION;
  }
}
