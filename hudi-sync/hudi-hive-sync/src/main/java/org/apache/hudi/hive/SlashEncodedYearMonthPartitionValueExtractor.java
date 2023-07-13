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

package org.apache.hudi.hive;

import org.apache.hudi.sync.common.model.PartitionValueExtractor;
import java.util.Collections;
import java.util.List;

/**
 * The SlashEncodedYearMonthPartitionValueExtractor class is a custom implementation of the
 * PartitionValueExtractor interface to support extracting partition values from paths
 * in the format 'yyyy/mm'.
 */
public class SlashEncodedYearMonthPartitionValueExtractor implements PartitionValueExtractor {

  private static final long serialVersionUID = 1L;

  /**
   * Overridden method from the PartitionValueExtractor interface to extract partition values
   * from a string partition path, assuming the path is in the format 'yyyy/mm'.
   * 
   * @param partitionPath The string from which the partition values are to be extracted.
   * @return A singleton list containing the partition value, formatted as 'yyyy-MM'.
   * @throws IllegalArgumentException If the partitionPath is not in the format 'yyyy/mm'.
   */
  @Override
  public List<String> extractPartitionValuesInPath(String partitionPath) {
    // partition path is expected to be in this format yyyy/mm/dd
    String[] splits = partitionPath.split("/");
    if (splits.length != 2) {
      throw new IllegalArgumentException("Partition path " + partitionPath + " is not in the form yyyy/mm ");
    }
    // Get the partition part and remove the / as well at the end
    // Format partition as yyyy-MM
    String formattedPartitionPath = splits[0] + "-" + splits[1];

    return Collections.singletonList(formattedPartitionPath);
  }
}
