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

package org.apache.hudi.common.table.compaction;

import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;

import java.util.Comparator;

public class SortMergeCompactionHelper {
  public static final Comparator<String> DEFAULT_RECORD_KEY_COMPACTOR = String::compareTo;
  public static final Comparator<HoodieKey> DEFAULT_HOODIE_KEY_COMPACTOR = (o1, o2) -> DEFAULT_RECORD_KEY_COMPACTOR.compare(o1.getRecordKey(), o2.getRecordKey());
  public static final Comparator<HoodieRecord> DEFAULT_RECORD_COMPACTOR = (o1, o2) -> DEFAULT_HOODIE_KEY_COMPACTOR.compare(o1.getKey(), o2.getKey());
}
