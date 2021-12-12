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
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metadata.HoodieMetadataPayload;

import java.util.List;
import java.util.Map;

public class HoodieLogBlockFactory {

  /**
   * Util method to get a data block for the requested type.
   *
   * @param logDataBlockFormat   - Data block type
   * @param recordList           - List of records that goes in the data block
   * @param header               - data block header
   * @param tableConfig          - Table config
   * @param excludeKeyFromRecord - Exclude key from record
   * @param populateMetaFields   - Whether to populate meta fields in the record
   * @return Data block of the requested type.
   */
  public static HoodieLogBlock getBlock(HoodieLogBlock.HoodieLogBlockType logDataBlockFormat,
                                        List<IndexedRecord> recordList,
                                        Map<HoodieLogBlock.HeaderMetadataType, String> header,
                                        HoodieTableConfig tableConfig,
                                        boolean populateMetaFields, boolean excludeKeyFromRecord) {
    String keyField;
    if (populateMetaFields) {
      keyField = (excludeKeyFromRecord
          ? HoodieMetadataPayload.SCHEMA_FIELD_ID_KEY : HoodieRecord.RECORD_KEY_METADATA_FIELD);
    } else {
      keyField = tableConfig.getRecordKeyFieldProp();
    }
    return getBlock(logDataBlockFormat, recordList, header, keyField, excludeKeyFromRecord);
  }

  /**
   * Util method to get a data block for the requested type.
   *
   * @param logDataBlockFormat   - Data block type
   * @param recordList           - List of records that goes in the data block
   * @param header               - data block header
   * @param keyField             - FieldId to get the key from the records
   * @param excludeKeyFromRecord - Whether key need to be excluded from the record payload
   * @return Data block of the requested type.
   */
  private static HoodieLogBlock getBlock(HoodieLogBlock.HoodieLogBlockType logDataBlockFormat, List<IndexedRecord> recordList,
                                         Map<HoodieLogBlock.HeaderMetadataType, String> header, String keyField,
                                         boolean excludeKeyFromRecord) {
    switch (logDataBlockFormat) {
      case AVRO_DATA_BLOCK:
        return new HoodieAvroDataBlock(recordList, header, keyField);
      case HFILE_DATA_BLOCK:
        if (excludeKeyFromRecord) {
          return new HoodieHFileKeyExcludedDataBlock(recordList, header, keyField);
        }
        return new HoodieHFileDataBlock(recordList, header, keyField);
      default:
        throw new HoodieException("Data block format " + logDataBlockFormat + " not implemented");
    }
  }

}
