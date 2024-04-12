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

package org.apache.hudi;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;

import java.io.IOException;

public class PrecombineBasedSparkRecordMerger extends HoodieSparkRecordMerger {

  @Override
  public String getMergingStrategy() {
    return PRECOMBINE_BASED_MERGER_STRATEGY_UUID;
  }

  @Override
  public Option<Pair<HoodieRecord, Schema>> merge(HoodieRecord older, Schema oldSchema, HoodieRecord newer, Schema newSchema, TypedProperties props) throws IOException {
    ValidationUtils.checkArgument(older.getRecordType() == HoodieRecord.HoodieRecordType.SPARK);
    ValidationUtils.checkArgument(newer.getRecordType() == HoodieRecord.HoodieRecordType.SPARK);
    int compare = 0;
    try {
      compare = older.getOrderingValue(oldSchema, props).compareTo(newer.getOrderingValue(newSchema, props));
    } catch (Exception e) {
      System.out.println("hi");
    }
    if (compare > 0) {
      if (older instanceof HoodieSparkRecord) {
        HoodieSparkRecord oldSparkRecord = (HoodieSparkRecord) older;
        if (oldSparkRecord.isDeleted()) {
          // Delete record
          return Option.empty();
        }
      } else {
        if (older.getData() == null) {
          // Delete record
          return Option.empty();
        }
      }
      return Option.of(Pair.of(older, oldSchema));
    } else {
      if (newer instanceof HoodieSparkRecord) {
        HoodieSparkRecord newSparkRecord = (HoodieSparkRecord) newer;
        if (newSparkRecord.isDeleted()) {
          // Delete record
          return Option.empty();
        }
      } else {
        if (newer.getData() == null) {
          // Delete record
          return Option.empty();
        }
      }
      return Option.of(Pair.of(newer, newSchema));
    }
  }
}
