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

package org.apache.parquet.avro;

import org.apache.hudi.common.util.Option;

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Type;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Support to avro-record read parquet-log which written by spark-record.
 * See the examples in TestMORDataSource#testRecordTypeCompatibilityWithParquetLog.
 * Exception throw when schema end with map or list.
 */
public class HoodieAvroReadSupport<T> extends AvroReadSupport<T> {

  private Option<MessageType> tableSchema;

  public HoodieAvroReadSupport(GenericData model, Option<MessageType> tableSchema) {
    super(model);
    this.tableSchema = tableSchema;
  }

  public HoodieAvroReadSupport() {
    tableSchema = Option.empty();
  }

  @Override
  public ReadContext init(Configuration configuration, Map<String, String> keyValueMetaData, MessageType fileSchema) {
    boolean legacyMode = checkLegacyMode(fileSchema.getFields());
    adjustConfToReadWithFileProduceMode(legacyMode, configuration);
    ReadContext readContext = super.init(configuration, keyValueMetaData, fileSchema);
    MessageType requestedSchema = repairLogicalTypes(readContext.getRequestedSchema(), tableSchema);
    // support non-legacy map. Convert non-legacy map to legacy map
    // Because there is no AvroWriteSupport.WRITE_OLD_MAP_STRUCTURE
    // according to AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE
    if (!legacyMode) {
      requestedSchema = new MessageType(requestedSchema.getName(), convertLegacyMap(requestedSchema.getFields()));
    }
    return new ReadContext(requestedSchema, readContext.getReadSupportMetadata());
  }

  /**
   * Here we want set config with which file has been written.
   * Even though user may have overwritten {@link AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE},
   * it's only applicable to how to produce new files(here is a read path).
   * Later the config value {@link AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE} will still be used
   * to write new file according to the user preferences.
   **/
  private void adjustConfToReadWithFileProduceMode(Boolean isLegacyModeWrittenFile, Configuration configuration) {
    if (isLegacyModeWrittenFile) {
      configuration.set(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE,
          "true", "support reading avro from legacy map/list in parquet file");
    } else {
      configuration.set(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE,
          "false", "support reading avro from non-legacy map/list in parquet file");
    }
  }

  /**
   * Check whether write map/list with legacy mode.
   * legacy:
   *  list:
   *    optional group obj_ids (LIST) {
   *      repeated binary array (UTF8);
   *    }
   *  map:
   *    optional group obj_ids (MAP) {
   *      repeated group map (MAP_KEY_VALUE) {
   *          required binary key (UTF8);
   *          required binary value (UTF8);
   *      }
   *    }
   * non-legacy:
   *    optional group obj_ids (LIST) {
   *      repeated group list {
   *        optional binary element (UTF8);
   *      }
   *    }
   *    optional group obj_maps (MAP) {
   *      repeated group key_value {
   *        required binary key (UTF8);
   *        optional binary value (UTF8);
   *      }
   *    }
   */
  private boolean checkLegacyMode(List<Type> parquetFields) {
    for (Type type : parquetFields) {
      if (!type.isPrimitive()) {
        GroupType groupType = type.asGroupType();
        OriginalType originalType = groupType.getOriginalType();
        if (originalType == OriginalType.MAP
            && groupType.getFields().get(0).getOriginalType() != OriginalType.MAP_KEY_VALUE) {
          return false;
        }
        if (originalType == OriginalType.LIST
            && !groupType.getType(0).getName().equals("array")) {
          return false;
        }
        if (!checkLegacyMode(groupType.getFields())) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Convert non-legacy map to legacy map.
   */
  private List<Type> convertLegacyMap(List<Type> oldTypes) {
    List<Type> newTypes = new ArrayList<>(oldTypes.size());
    for (Type type : oldTypes) {
      if (!type.isPrimitive()) {
        GroupType parent = type.asGroupType();
        List<Type> types = convertLegacyMap(parent.getFields());
        if (type.getOriginalType() == OriginalType.MAP_KEY_VALUE) {
          newTypes.add(new GroupType(parent.getRepetition(), "key_value", types));
        } else {
          newTypes.add(new GroupType(parent.getRepetition(), parent.getName(), parent.getOriginalType(), types));
        }
      } else {
        newTypes.add(type);
      }
    }
    return newTypes;
  }

  private MessageType repairLogicalTypes(MessageType fileSchema, Option<MessageType> tableSchemaOpt) {
    try {
      Class<?> repairClass = Class.forName("org.apache.parquet.schema.SchemaRepair");
      Method repairMethod = repairClass.getDeclaredMethod(
          "repairLogicalTypes", MessageType.class, Option.class);
      MessageType repaired = (MessageType) repairMethod.invoke(null, fileSchema, tableSchemaOpt);
      return repaired != null ? repaired : fileSchema;
    } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      return fileSchema;
    }
  }
}
