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

package org.apache.hudi.hive;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Collections;
import java.util.List;

/**
 * HDFS Path contain hive partition values for the keys it is partitioned on. This mapping is not straight forward and
 * requires a pluggable implementation to extract the partition value from HDFS path.
 * <p>
 * This implementation extracts datestr=yyyy-mm-dd-HH from path of type /yyyy/mm/dd/HH
 */
public class SlashEncodedHourPartitionValueExtractor implements PartitionValueExtractor {

  private static final long serialVersionUID = 1L;
  private transient DateTimeFormatter dtfOut;

  public SlashEncodedHourPartitionValueExtractor() {
    this.dtfOut = DateTimeFormat.forPattern("yyyy-MM-dd-HH");
  }

  private DateTimeFormatter getDtfOut() {
    if (dtfOut == null) {
      dtfOut = DateTimeFormat.forPattern("yyyy-MM-dd-HH");
    }
    return dtfOut;
  }

  @Override
  public List<String> extractPartitionValuesInPath(String partitionPath) {
    // partition path is expected to be in this format yyyy/mm/dd/HH
    String[] splits = partitionPath.split("/");
    if (splits.length != 4) {
      throw new IllegalArgumentException("Partition path " + partitionPath + " is not in the form  yyyy/mm/dd/HH");
    }
    //Hive style partitions need to contain '='
    int year = Integer.parseInt(splits[0].contains("=") ? splits[0].split("=")[1] : splits[0]);
    int mm = Integer.parseInt(splits[1].contains("=") ? splits[1].split("=")[1] : splits[1]);
    int dd = Integer.parseInt(splits[2].contains("=") ? splits[2].split("=")[1] : splits[2]);
    int hh = Integer.parseInt(splits[3].contains("=") ? splits[3].split("=")[1] : splits[3]);

    DateTime dateTime = new DateTime(year, mm, dd, hh, 0);

    return Collections.singletonList(getDtfOut().print(dateTime));
  }
}
