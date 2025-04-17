/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.transform;

import org.apache.hudi.client.model.HoodieFlinkAvroRecord;
import org.apache.hudi.client.model.HoodieFlinkRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.sink.bulk.RowDataKeyGen;
import org.apache.hudi.table.action.commit.BucketInfo;
import org.apache.hudi.util.CommonClientUtils;
import org.apache.hudi.util.OrderingValueExtractor;
import org.apache.hudi.util.RowDataToAvroConverters;
import org.apache.hudi.util.StreamerUtil;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;

/**
 * Function that converts the given {@link RowData} into a hoodie record.
 */
public interface RecordConverter extends Serializable {
  HoodieRecord convert(RowData dataRow, BucketInfo bucketInfo);

  static RecordConverter getInstance(
      Configuration flinkConf,
      RowType rowType,
      RowDataKeyGen keyGen,
      HoodieWriteConfig writeConfig,
      HoodieTableConfig tableConfig) {
    // construct flink record according to the log block format type
    HoodieLogBlock.HoodieLogBlockType logBlockType = CommonClientUtils.getLogBlockType(writeConfig, tableConfig);
    OrderingValueExtractor orderingValueExtractor = OrderingValueExtractor.getInstance(flinkConf, rowType);
    if (logBlockType == HoodieLogBlock.HoodieLogBlockType.PARQUET_DATA_BLOCK) {
      return (dataRow, bucketInfo) -> {
        String key = keyGen.getRecordKey(dataRow);
        Comparable<?> orderingValue = orderingValueExtractor.getOrderingValue(dataRow);
        HoodieOperation operation = HoodieOperation.fromValue(dataRow.getRowKind().toByteValue());
        HoodieKey hoodieKey = new HoodieKey(key, bucketInfo.getPartitionPath());
        return new HoodieFlinkRecord(hoodieKey, operation, orderingValue, dataRow);
      };
    } else if (logBlockType == HoodieLogBlock.HoodieLogBlockType.AVRO_DATA_BLOCK) {
      return new RecordConverter() {
        private final Schema avroSchema = StreamerUtil.getSourceSchema(flinkConf);
        private final RowDataToAvroConverters.RowDataToAvroConverter converter = RowDataToAvroConverters.createConverter(rowType, flinkConf.get(FlinkOptions.WRITE_UTC_TIMEZONE));

        @Override
        public HoodieRecord convert(RowData dataRow, BucketInfo bucketInfo) {
          String key = keyGen.getRecordKey(dataRow);
          Comparable<?> orderingValue = orderingValueExtractor.getOrderingValue(dataRow);
          HoodieOperation operation = HoodieOperation.fromValue(dataRow.getRowKind().toByteValue());
          HoodieKey hoodieKey = new HoodieKey(key, bucketInfo.getPartitionPath());

          GenericRecord record = (GenericRecord) converter.convert(avroSchema, dataRow);
          return new HoodieFlinkAvroRecord(hoodieKey, operation, orderingValue, record);
        }
      };
    } else {
      throw new HoodieException("Unsupported log block type: " + logBlockType);
    }
  }
}
