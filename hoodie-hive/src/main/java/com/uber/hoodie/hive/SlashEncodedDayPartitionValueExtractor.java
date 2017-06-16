/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.hive;

import com.beust.jcommander.internal.Lists;
import java.util.List;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
/**
 * HDFS Path contain hive partition values for the keys it is partitioned on.
 * This mapping is not straight forward and requires a pluggable implementation to extract the partition value from HDFS path.
 *
 * This implementation extracts datestr=yyyy-mm-dd from path of type /yyyy/mm/dd
 */
public class SlashEncodedDayPartitionValueExtractor implements PartitionValueExtractor {

  private final DateTimeFormatter dtfOut;

  public SlashEncodedDayPartitionValueExtractor() {
    this.dtfOut = DateTimeFormat.forPattern("yyyy-MM-dd");
  }

  @Override
  public List<String> extractPartitionValuesInPath(String partitionPath) {
    // partition path is expected to be in this format yyyy/mm/dd
    String[] splits = partitionPath.split("/");
    if (splits.length != 3) {
      throw new IllegalArgumentException(
          "Partition path " + partitionPath + " is not in the form yyyy/mm/dd ");
    }
    // Get the partition part and remove the / as well at the end
    int year = Integer.parseInt(splits[0]);
    int mm = Integer.parseInt(splits[1]);
    int dd = Integer.parseInt(splits[2]);
    DateTime dateTime = new DateTime(year, mm, dd, 0, 0);
    return Lists.newArrayList(dtfOut.print(dateTime));
  }
}
