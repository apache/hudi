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

package org.apache.hudi.testutils;

import org.apache.hudi.HoodieSparkRecordMerger;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecords;

import java.io.IOException;

/**
 * Instead of merging, it will just delete the record. Use for testing
 *
 */
public class HoodieSparkDeleteRecordMerger extends HoodieSparkRecordMerger {
  public static final String DELETE_MERGER_STRATEGY = "aea2e14e-19a2-4e33-bc37-f8871d55bd5b";

  @Override
  public <T> BufferedRecord<T> merge(BufferedRecord<T> older, BufferedRecord<T> newer, RecordContext<T> recordContext, TypedProperties props) throws IOException {
    return BufferedRecords.createDelete(newer.getRecordKey(), HoodieRecordMerger.maxOrderingValue(older, newer));
  }

  @Override
  public String getMergingStrategy() {
    return DELETE_MERGER_STRATEGY;
  }
}
