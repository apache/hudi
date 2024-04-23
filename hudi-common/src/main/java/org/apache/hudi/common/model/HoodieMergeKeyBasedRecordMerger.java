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
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Record merger based on {@link HoodieMergeKey}. If the merge keys are of type {@link HoodieCompositeMergeKey},
 * then it returns the older and newer records. Otherwise, it calls the merge method from the parent class.
 */
public class HoodieMergeKeyBasedRecordMerger extends HoodiePreCombineAvroRecordMerger {

  public static final HoodieMergeKeyBasedRecordMerger INSTANCE = new HoodieMergeKeyBasedRecordMerger();

  @Override
  public List<Pair<HoodieRecord, Schema>> fullOuterMerge(HoodieRecord older, Schema oldSchema, HoodieRecord newer, Schema newSchema, TypedProperties props) throws IOException {
    // TODO: Implement this method. Currently, it just checks if the merge keys are of type HoodieSimpleMergeKey,
    //       then it calls the merge method from the parent class. Otherwise, it returns the older and newer records.
    //       We need to implement the full outer merge logic for HoodieCompositeMergeKey.
    if (older.getMergeKey() instanceof HoodieCompositeMergeKey && newer.getMergeKey() instanceof HoodieCompositeMergeKey) {
      return Arrays.asList(Pair.of(older, oldSchema), Pair.of(newer, newSchema));
    }
    return Arrays.asList(super.merge(older, oldSchema, newer, newSchema, props).get());
  }
}
