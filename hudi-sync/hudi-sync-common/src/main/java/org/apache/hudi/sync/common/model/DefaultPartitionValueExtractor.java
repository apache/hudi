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

package org.apache.hudi.sync.common.model;

import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class DefaultPartitionValueExtractor implements PartitionValueExtractor {

  private final List<String> partitionFields;

  public DefaultPartitionValueExtractor(List<String> partitionFields) {
    this.partitionFields = partitionFields;
  }

  @Override
  public List<String> extractPartitionValuesInPath(String partitionPath) {
    if (CollectionUtils.isNullOrEmpty(partitionFields)) {
      return Collections.emptyList();
    }

    ValidationUtils.checkArgument(StringUtils.nonEmpty(partitionPath),
        "Expected non-empty partition path but got " + partitionPath);

    String[] parts = partitionPath.split("/");
    int depth = parts.length;
    ValidationUtils.checkArgument(depth == partitionFields.size(),
        "Expected partition depth of " + partitionFields.size() + " but got " + depth);

    String[] partitionValues = new String[depth];
    Boolean isHiveStyle = null;
    for (int i = 0; i < depth; i++) {
      int equalSignIndex = parts[i].indexOf("=");
      boolean foundHiveStyle = equalSignIndex != -1;
      if (isHiveStyle == null) {
        isHiveStyle = foundHiveStyle;
      } else {
        ValidationUtils.checkArgument(foundHiveStyle == isHiveStyle,
            "Expected hiveStyle=" + isHiveStyle + " at depth=" + i + " but got hiveStyle=" + foundHiveStyle);
      }

      if (isHiveStyle) {
        String foundFieldName = parts[i].substring(0, equalSignIndex);
        String fieldName = partitionFields.get(i);
        ValidationUtils.checkArgument(Objects.equals(fieldName, foundFieldName),
            "Expected field `" + fieldName + "` at depth=" + i + " but got `" + foundFieldName + "`");
        partitionValues[i] = parts[i].substring(equalSignIndex + 1);
      } else {
        partitionValues[i] = parts[i];
      }
    }

    return Arrays.asList(partitionValues);
  }
}
