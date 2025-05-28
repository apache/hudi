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

package org.apache.hudi.common.model;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;

import java.io.IOException;

import static org.apache.hudi.common.model.HoodieRecord.DEFAULT_ORDERING_VALUE;

/**
 * Implements the event time based merging for native Avro records.
 *
 * This is to replace {@link HoodieAvroRecordMerger} class, which is
 * designed to support payload based Avro records.
 */
public class EventTimeBasedAvroRecordMerger implements HoodieRecordMerger {
  public static final EventTimeBasedAvroRecordMerger INSTANCE = new EventTimeBasedAvroRecordMerger();

  @Override
  public Option<Pair<HoodieRecord, Schema>> merge(HoodieRecord oldRecord,
                                                  Schema oldSchema,
                                                  HoodieRecord newRecord,
                                                  Schema newSchema,
                                                  TypedProperties props) throws IOException {
    if (shouldKeepNewerRecord(oldRecord, newRecord, oldSchema, newSchema, props)) {
      return Option.of(Pair.of(newRecord, newSchema));
    }
    return Option.of(Pair.of(oldRecord, oldSchema));
  }

  @Override
  public HoodieRecord.HoodieRecordType getRecordType() {
    return HoodieRecord.HoodieRecordType.AVRO;
  }

  @Override
  public String getMergingStrategy() {
    return HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID;
  }

  protected boolean isCommitTimeOrderingDelete(HoodieRecord record,
                                               Schema schema,
                                               TypedProperties props) throws IOException {
    Comparable orderingValue = record.getOrderingValue(schema, props);
    return record.isDelete(schema, props) && orderingValue.equals(DEFAULT_ORDERING_VALUE);
  }

  protected boolean shouldKeepNewerRecord(HoodieRecord oldRecord,
                                          HoodieRecord newRecord,
                                          Schema oldSchema,
                                          Schema newSchema,
                                          TypedProperties props) throws IOException {
    if (isCommitTimeOrderingDelete(newRecord, newSchema, props)) {
      // handle records coming from DELETE statements(the orderingVal is constant 0)
      return true;
    }
    return newRecord.getOrderingValue(newSchema, props)
        .compareTo(oldRecord.getOrderingValue(oldSchema, props)) >= 0;
  }
}
