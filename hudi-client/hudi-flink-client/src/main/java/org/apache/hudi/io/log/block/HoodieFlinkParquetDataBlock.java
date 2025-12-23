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

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaCache;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.log.block.HoodieParquetDataBlock;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.io.storage.ColumnRangeMetadataProvider;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.metadata.HoodieIndexVersion;
import org.apache.hudi.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.storage.HoodieStorage;

import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.config.HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME;
import static org.apache.hudi.common.config.HoodieStorageConfig.PARQUET_COMPRESSION_RATIO_FRACTION;
import static org.apache.hudi.common.config.HoodieStorageConfig.PARQUET_DICTIONARY_ENABLED;
import static org.apache.hudi.common.model.HoodieFileFormat.PARQUET;

/**
 * HoodieFlinkParquetDataBlock employs an HoodieRecord iterator rather than a HoodieRecord list for
 * parquet data block, aiming to better utilize the optimizations of {@code BinaryInMemorySortBuffer},
 * for example, object reusing to decrease GC costs.
 */
public class HoodieFlinkParquetDataBlock extends HoodieParquetDataBlock implements ColumnRangeMetadataProvider {

  private final Iterator<HoodieRecord> recordIterator;
  /**
   * Parquet metadata collected during serializing the records, used to build column range metadata.
   */
  private ParquetMetadata parquetMetadata;

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
  public ByteArrayOutputStream getContentBytes(HoodieStorage storage) throws IOException {
    Map<String, String> paramsMap = new HashMap<>();
    paramsMap.put(PARQUET_COMPRESSION_CODEC_NAME.key(), compressionCodecName.get());
    paramsMap.put(PARQUET_COMPRESSION_RATIO_FRACTION.key(), String.valueOf(expectedCompressionRatio.get()));
    paramsMap.put(PARQUET_DICTIONARY_ENABLED.key(), String.valueOf(useDictionaryEncoding.get()));
    HoodieSchema writerSchema = HoodieSchemaCache.intern(HoodieSchema.parse(
        super.getLogBlockHeader().get(HoodieLogBlock.HeaderMetadataType.SCHEMA)));

    Pair<ByteArrayOutputStream, Object> result =
        HoodieIOFactory.getIOFactory(storage).getFileFormatUtils(PARQUET)
            .serializeRecordsToLogBlock(
                storage,
                recordIterator,
                HoodieRecord.HoodieRecordType.FLINK,
                writerSchema,
                getSchema(),
                getKeyFieldName(),
                paramsMap);
    ValidationUtils.checkArgument(result.getRight() instanceof ParquetMetadata,
        "The returned format metadata should be ParquetMetadata.");
    this.parquetMetadata = (ParquetMetadata) result.getRight();
    return result.getLeft();
  }

  @Override
  public Map<String, HoodieColumnRangeMetadata<Comparable>> getColumnRangeMeta(String filePath, HoodieIndexVersion indexVersion) {
    ValidationUtils.checkArgument(parquetMetadata != null, "parquetMetadata should not be null.");
    ParquetUtils parquetUtils = new ParquetUtils();
    List<HoodieColumnRangeMetadata<Comparable>> columnMetaList = parquetUtils.readColumnStatsFromMetadata(parquetMetadata, filePath, Option.empty(), indexVersion);
    return columnMetaList.stream().collect(Collectors.toMap(HoodieColumnRangeMetadata::getColumnName, colMeta -> colMeta));
  }
}
