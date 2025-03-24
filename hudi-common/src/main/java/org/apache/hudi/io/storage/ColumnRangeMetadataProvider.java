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

package org.apache.hudi.io.storage;

import org.apache.hudi.common.model.HoodieColumnRangeMetadata;

import java.util.Map;

/**
 * A provider that used to collect column statistics like column range metadata.
 */
public interface ColumnRangeMetadataProvider {

  /**
   * Get the column statistics, key is column name, value is the statistic for the column.
   */
  Map<String, HoodieColumnRangeMetadata<Comparable>> getColumnRangeMeta(String filePath);
}
