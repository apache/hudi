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

package org.apache.hudi.common.table.timeline.versioning;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * This class wraps around the commit time generator and helps to handle version upgrade changes.
 */
public class InstantTimeFormatter {

  public static final SimpleDateFormat COMMIT_FORMATTER_V1 = new SimpleDateFormat("yyyyMMddHHmmss");
  public static final SimpleDateFormat COMMIT_FORMATTER_V2 = new SimpleDateFormat("yyyyMMddHHmmssSSS");
  public static Integer latestVersion = TimelineLayoutVersion.CURR_VERSION;

  public String format(Date date) {
    switch (latestVersion) {
      case 0:
      case 1:
        return COMMIT_FORMATTER_V1.format(date);
      case 2:
        return COMMIT_FORMATTER_V2.format(date);
      default:
        throw new UnsupportedOperationException();
    }
  }

  public Date parse(String commitTime) throws ParseException {
    int length = commitTime.length();
    switch (length) {
      // For second granularity instant times parsing in V1
      /**
       * 13 length is for {@link org.apache.hudi.metadata.HoodieTableMetadata#SOLO_COMMIT_TIMESTAMP}
       * This value needs to be less than {@link org.apache.hudi.common.table.timeline.HoodieTimeline#INIT_INSTANT_TS}
       * so we carry this logic forward.
       */
      case 13:
      case 14:
        return COMMIT_FORMATTER_V1.parse(commitTime);
      // For milliseconds granularity instant times parsing in V2
      case 17:
        return COMMIT_FORMATTER_V2.parse(commitTime);
      default:
        throw new UnsupportedOperationException();
    }
  }
}
