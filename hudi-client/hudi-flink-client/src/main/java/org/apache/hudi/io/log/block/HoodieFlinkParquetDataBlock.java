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

package org.apache.hudi.io.log.block;

import org.apache.hudi.avro.AvroSchemaCache;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.log.block.HoodieParquetDataBlock;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.io.storage.ColumnRangeMetadataProvider;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.storage.HoodieStorage;

import org.apache.avro.Schema;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.config.HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME;
import static org.apache.hudi.common.config.HoodieStorageConfig.PARQUET_COMPRESSION_RATIO_FRACTION;
import static org.apache.hudi.common.config.HoodieStorageConfig.PARQUET_DICTIONARY_ENABLED;
import static org.apache.hudi.common.model.HoodieFileFormat.PARQUET;

/**
 * HoodieFlinkParquetDataBlock employs an HoodieRecord iterator rather than a HoodieRecord list for
 * parquet data block, aiming to better utilize the optimizations of {@code BinaryInMemorySortBuffer},
 * for example, object reusing to decrease GC costs.
 *
 * <p> todo: HoodieFlinkParquetDataBlock does not support record-position for update/delete currently,
 * and it will be supported later, see HUDI-9192.
 */
public class HoodieFlinkParquetDataBlock extends HoodieParquetDataBlock implements ColumnRangeMetadataProvider {

  private final Iterator<HoodieRecord> recordIterator;
  /**
   * Column stats for the written records, collected during serialization for efficiency.
   */
  protected Option<Map<String, HoodieColumnRangeMetadata<Comparable>>> columnStatsMeta = Option.empty();

  public HoodieFlinkParquetDataBlock(
      List<HoodieRecord> records,
      Map<HeaderMetadataType, String> header,
      String keyField,
      String compressionCodecName,
      double expectedCompressionRatio,
      boolean useDictionaryEncoding) {
    super(records, header, keyField, compressionCodecName, expectedCompressionRatio, useDictionaryEncoding);
    this.recordIterator = records.iterator();
  }

  @Override
  public byte[] getContentBytes(HoodieStorage storage) throws IOException {
    Map<String, String> paramsMap = new HashMap<>();
    paramsMap.put(PARQUET_COMPRESSION_CODEC_NAME.key(), compressionCodecName.get());
    paramsMap.put(PARQUET_COMPRESSION_RATIO_FRACTION.key(), String.valueOf(expectedCompressionRatio.get()));
    paramsMap.put(PARQUET_DICTIONARY_ENABLED.key(), String.valueOf(useDictionaryEncoding.get()));
    Schema writerSchema = AvroSchemaCache.intern(new Schema.Parser().parse(
        super.getLogBlockHeader().get(HoodieLogBlock.HeaderMetadataType.SCHEMA)));

    Pair<byte[], Map<String, HoodieColumnRangeMetadata<Comparable>>> result =
        HoodieIOFactory.getIOFactory(storage).getFileFormatUtils(PARQUET)
            .serializeRecordsToLogBlock(
                storage,
                recordIterator,
                HoodieRecord.HoodieRecordType.FLINK,
                writerSchema,
                getSchema(),
                getKeyFieldName(),
                paramsMap);
    Map<String, HoodieColumnRangeMetadata<Comparable>> columnRangeMeta = result.getRight();
    ValidationUtils.checkArgument(!columnRangeMeta.isEmpty(),
        "Column range metadata collected from Parquet metadata should not be empty.");
    this.columnStatsMeta = Option.of(columnRangeMeta);
    return result.getLeft();
  }

  @Override
  public Map<String, HoodieColumnRangeMetadata<Comparable>> getColumnRangeMeta() {
    ValidationUtils.checkArgument(columnStatsMeta.isPresent(), "`columnStatsMeta` should not be empty.");
    return columnStatsMeta.get();
  }
}
