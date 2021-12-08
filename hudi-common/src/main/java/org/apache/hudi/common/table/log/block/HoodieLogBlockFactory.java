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

package org.apache.hudi.common.table.log.block;

import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metadata.HoodieMetadataHFileDataBlock;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadata;

import java.util.List;
import java.util.Map;

public class HoodieLogBlockFactory {

  /**
   * Util method to get a data block for the requested type.
   *
   * @param logDataBlockFormat - Data block type
   * @param recordList         - List of records that goes in the data block
   * @param header             - data block header
   * @param tableConfig        - Table config
   * @param metadataConfig     - Metadata config
   * @param tableBasePath      - Table base path
   * @param populateMetaFields - Whether to populate meta fields in the record
   * @return Data block of the requested type.
   */
  public static HoodieLogBlock getBlock(HoodieLogBlock.HoodieLogBlockType logDataBlockFormat,
                                        List<IndexedRecord> recordList,
                                        Map<HoodieLogBlock.HeaderMetadataType, String> header,
                                        HoodieTableConfig tableConfig, HoodieMetadataConfig metadataConfig,
                                        String tableBasePath, boolean populateMetaFields) {
    final boolean isMetadataKeyDeDuplicate = metadataConfig.getRecordKeyDeDuplicate()
        && HoodieTableMetadata.isMetadataTable(tableBasePath);
    String keyField;
    if (populateMetaFields) {
      keyField = (isMetadataKeyDeDuplicate
          ? HoodieMetadataPayload.SCHEMA_FIELD_ID_KEY : HoodieRecord.RECORD_KEY_METADATA_FIELD);
    } else {
      keyField = tableConfig.getRecordKeyFieldProp();
    }
    return getBlock(logDataBlockFormat, recordList, header, keyField, isMetadataKeyDeDuplicate);
  }

  /**
   * Util method to get a data block for the requested type.
   *
   * @param logDataBlockFormat       - Data block type
   * @param recordList               - List of records that goes in the data block
   * @param header                   - data block header
   * @param keyField                 - FieldId to get the key from the records
   * @param isMetadataKeyDeDuplicate - Whether metadata key de duplication needed
   * @return Data block of the requested type.
   */
  private static HoodieLogBlock getBlock(HoodieLogBlock.HoodieLogBlockType logDataBlockFormat, List<IndexedRecord> recordList,
                                         Map<HoodieLogBlock.HeaderMetadataType, String> header, String keyField,
                                         boolean isMetadataKeyDeDuplicate) {
    switch (logDataBlockFormat) {
      case AVRO_DATA_BLOCK:
        return new HoodieAvroDataBlock(recordList, header, keyField);
      case HFILE_DATA_BLOCK:
        if (isMetadataKeyDeDuplicate) {
          return new HoodieMetadataHFileDataBlock(recordList, header, keyField);
        }
        return new HoodieHFileDataBlock(recordList, header, keyField);
      default:
        throw new HoodieException("Data block format " + logDataBlockFormat + " not implemented");
    }
  }

}
