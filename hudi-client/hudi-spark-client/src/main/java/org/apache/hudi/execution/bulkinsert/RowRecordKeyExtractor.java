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

package org.apache.hudi.execution.bulkinsert;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.keygen.BuiltinKeyGenerator;

import org.apache.spark.sql.Row;

import java.io.Serializable;

public interface RowRecordKeyExtractor extends Serializable {
  String getPartitionPath(Row row);

  String getRecordKey(Row row);

  static RowRecordKeyExtractor getRowRecordKeyExtractor(boolean populateMetaFields, Option<BuiltinKeyGenerator> keyGeneratorOpt) {
    if (populateMetaFields) {
      return new MetaFiledRowKeyExtractor();
    } else if (keyGeneratorOpt.isPresent()) {
      return new BuiltInKeyGenRowKeyExtractor(keyGeneratorOpt.get());
    } else {
      return new EmptyRowKeyExtractor();
    }
  }

  class MetaFiledRowKeyExtractor implements RowRecordKeyExtractor {
    @Override
    public String getPartitionPath(Row row) {
      // In case meta-fields are materialized w/in the table itself, we can just simply extract
      // partition path from there
      return row.getString(HoodieRecord.PARTITION_PATH_META_FIELD_ORD);
    }

    @Override
    public String getRecordKey(Row row) {
      // In case meta-fields are materialized w/in the table itself, we can just simply extract
      // record key from there
      return row.getString(HoodieRecord.RECORD_KEY_META_FIELD_ORD);
    }
  }

  class BuiltInKeyGenRowKeyExtractor implements RowRecordKeyExtractor {
    private final BuiltinKeyGenerator keyGenerator;

    public BuiltInKeyGenRowKeyExtractor(BuiltinKeyGenerator keyGenerator) {
      this.keyGenerator = keyGenerator;
    }

    @Override
    public String getPartitionPath(Row row) {
      return keyGenerator.getPartitionPath(row);
    }

    @Override
    public String getRecordKey(Row row) {
      return keyGenerator.getRecordKey(row);
    }
  }

  class EmptyRowKeyExtractor implements RowRecordKeyExtractor {
    @Override
    public String getPartitionPath(Row row) {
      return StringUtils.EMPTY_STRING;
    }

    @Override
    public String getRecordKey(Row row) {
      return StringUtils.EMPTY_STRING;
    }
  }

}
