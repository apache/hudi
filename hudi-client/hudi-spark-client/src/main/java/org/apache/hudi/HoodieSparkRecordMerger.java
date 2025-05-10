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
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;

public abstract class HoodieSparkRecordMerger implements HoodieRecordMerger {
  @Override
  public HoodieRecord.HoodieRecordType getRecordType() {
    return HoodieRecord.HoodieRecordType.SPARK;
  }

  /**
   * Basic handling of deletes that is used by many of the spark mergers
   * returns null if merger specific logic should be used
   */
  protected Option<Pair<HoodieRecord, Schema>> handleDeletes(HoodieRecord older, Schema oldSchema, HoodieRecord newer, Schema newSchema, TypedProperties props) {
    ValidationUtils.checkArgument(older.getRecordType() == HoodieRecord.HoodieRecordType.SPARK);
    ValidationUtils.checkArgument(newer.getRecordType() == HoodieRecord.HoodieRecordType.SPARK);

    if (newer instanceof HoodieSparkRecord) {
      HoodieSparkRecord newSparkRecord = (HoodieSparkRecord) newer;
      if (newSparkRecord.isDelete(newSchema, props)) {
        // Delete record
        return Option.empty();
      }
    } else {
      if (newer.getData() == null) {
        // Delete record
        return Option.empty();
      }
    }

    if (older instanceof HoodieSparkRecord) {
      HoodieSparkRecord oldSparkRecord = (HoodieSparkRecord) older;
      if (oldSparkRecord.isDelete(oldSchema, props)) {
        // use natural order for delete record
        return Option.of(Pair.of(newer, newSchema));
      }
    } else {
      if (older.getData() == null) {
        // use natural order for delete record
        return Option.of(Pair.of(newer, newSchema));
      }
    }

    return null;
  }
}
