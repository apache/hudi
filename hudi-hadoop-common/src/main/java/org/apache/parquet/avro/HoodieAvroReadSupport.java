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

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.SchemaRepair;
import org.apache.parquet.schema.Type;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.util.TypeUtils.unsafeCast;

/**
 * Support to avro-record read parquet-log which written by spark-record.
 * See the examples in TestMORDataSource#testRecordTypeCompatibilityWithParquetLog.
 * Exception throw when schema end with map or list.
 */
public class HoodieAvroReadSupport<T> extends AvroReadSupport<T> {

  private static final String AVRO_READ_SCHEMA_KEY = "avro.read.schema";
  private static final MethodHandle PARENT_PREPARE_FOR_READ = lookupParentPrepareForRead();

  private final GenericData model;
  private Option<MessageType> tableSchema;

  public HoodieAvroReadSupport(GenericData model, Option<MessageType> tableSchema) {
    super(model);
    this.model = model;
    this.tableSchema = tableSchema;
  }

  public HoodieAvroReadSupport() {
    this(GenericData.get(), Option.empty());
  }

  @Override
  public ReadContext init(Configuration configuration, Map<String, String> keyValueMetaData, MessageType fileSchema) {
    boolean legacyMode = checkLegacyMode(fileSchema.getFields());
    adjustConfToReadWithFileProduceMode(legacyMode, configuration);
    ReadContext readContext = super.init(configuration, keyValueMetaData, fileSchema);
    MessageType requestedSchema = SchemaRepair.repairLogicalTypes(readContext.getRequestedSchema(), tableSchema);
    // support non-legacy map. Convert non-legacy map to legacy map
    // Because there is no AvroWriteSupport.WRITE_OLD_MAP_STRUCTURE
    // according to AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE
    if (!legacyMode) {
      requestedSchema = new MessageType(requestedSchema.getName(), convertLegacyMap(requestedSchema.getFields()));
    }
    requestedSchema = SchemaRepair.alignDecimalPhysicalTypes(requestedSchema, fileSchema);
    return new ReadContext(requestedSchema, withDecimalReadSchema(requestedSchema, readContext.getReadSupportMetadata(), keyValueMetaData, configuration));
  }

  /**
   * Initializes the Avro read support for Parquet files using {@link ParquetConfiguration} (Avro 1.12.x+).
   * This method overrides {@code AvroReadSupport#init} to handle legacy list structure compatibility
   * and projection schema configuration.
   *
   * @param configuration    The Parquet configuration containing read settings and projection schema
   * @param keyValueMetaData Key-value metadata from the Parquet file footer
   * @param fileSchema       The schema of the Parquet file being read
   * @return A {@link ReadContext} containing the projection schema and read support metadata
   */
  public ReadContext init(ParquetConfiguration configuration, Map<String, String> keyValueMetaData, MessageType fileSchema) {
    boolean legacyMode = checkLegacyMode(fileSchema.getFields());
    configuration.set(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, String.valueOf(legacyMode));
    MessageType projection = SchemaRepair.repairLogicalTypes(fileSchema, tableSchema);
    Map<String, String> metadata = new LinkedHashMap<String, String>();

    String requestedProjectionString = configuration.get(AVRO_REQUESTED_PROJECTION);
    if (requestedProjectionString != null) {
      HoodieSchema avroRequestedProjection = HoodieSchema.parse(requestedProjectionString);
      Configuration conf = new Configuration(false);
      configuration.forEach(entry -> conf.set(entry.getKey(), entry.getValue()));
      projection = new AvroSchemaConverter(conf).convert(avroRequestedProjection.toAvroSchema());
    }

    String avroReadSchema = configuration.get("parquet.avro.read.schema");
    if (avroReadSchema != null) {
      metadata.put(AVRO_READ_SCHEMA_KEY, avroReadSchema);
    }

    if (configuration.getBoolean(AVRO_COMPATIBILITY, AVRO_DEFAULT_COMPATIBILITY)) {
      metadata.put(AVRO_COMPATIBILITY, "true");
    }

    ReadContext readContext = new ReadContext(projection, metadata);
    MessageType requestedSchema = readContext.getRequestedSchema();
    // support non-legacy map. Convert non-legacy map to legacy map
    // Because there is no AvroWriteSupport.WRITE_OLD_MAP_STRUCTURE
    // according to AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE
    if (!legacyMode) {
      requestedSchema = new MessageType(requestedSchema.getName(), convertLegacyMap(requestedSchema.getFields()));
    }
    requestedSchema = SchemaRepair.alignDecimalPhysicalTypes(requestedSchema, fileSchema);
    Configuration conf = new Configuration(false);
    configuration.forEach(entry -> conf.set(entry.getKey(), entry.getValue()));
    return new ReadContext(requestedSchema, withDecimalReadSchema(requestedSchema, readContext.getReadSupportMetadata(), keyValueMetaData, conf));
  }

  @Override
  public RecordMaterializer<T> prepareForRead(Configuration configuration, Map<String, String> keyValueMetaData,
                                              MessageType fileSchema, ReadContext readContext) {
    RecordMaterializer<T> materializer = super.prepareForRead(configuration, keyValueMetaData, fileSchema, withoutCompatibility(readContext));
    return readIntegerDecimals(materializer, configuration, keyValueMetaData, readContext);
  }

  /**
   * Entry point on parquet 1.14+, whose record reader dispatches through {@link ParquetConfiguration}. The parent's
   * overload of this signature does the real work there and is reached with a super-call method handle, since
   * naming it in source would not compile against the parquet versions that lack it; on those versions this overload
   * is never dispatched to and simply falls back to the {@link Configuration} path.
   */
  public RecordMaterializer<T> prepareForRead(ParquetConfiguration configuration, Map<String, String> keyValueMetaData,
                                              MessageType fileSchema, ReadContext readContext) {
    Configuration conf = new Configuration(false);
    configuration.forEach(entry -> conf.set(entry.getKey(), entry.getValue()));
    if (PARENT_PREPARE_FOR_READ == null) {
      return prepareForRead(conf, keyValueMetaData, fileSchema, readContext);
    }
    RecordMaterializer<T> materializer;
    try {
      materializer = unsafeCast(PARENT_PREPARE_FOR_READ.invoke(this, configuration, keyValueMetaData, fileSchema, withoutCompatibility(readContext)));
    } catch (RuntimeException e) {
      throw e;
    } catch (Throwable t) {
      throw new HoodieException("Failed to prepare the Avro parquet reader", t);
    }
    return readIntegerDecimals(materializer, conf, keyValueMetaData, readContext);
  }

  private static MethodHandle lookupParentPrepareForRead() {
    try {
      return MethodHandles.lookup().findSpecial(AvroReadSupport.class, "prepareForRead",
          MethodType.methodType(RecordMaterializer.class, ParquetConfiguration.class, Map.class, MessageType.class, ReadContext.class),
          HoodieAvroReadSupport.class);
    } catch (NoSuchMethodException | IllegalAccessException e) {
      return null;
    }
  }

  private static boolean hasIntegerBackedDecimal(MessageType schema) {
    return schema.getFields().stream().anyMatch(SchemaRepair::containsIntegerBackedDecimal);
  }

  /**
   * Without a read schema, parquet-avro derives the record schema from the projection with its own converter, which
   * types an INT32/INT64 decimal column as int or long. The wrapped converter would then put a decimal into that
   * slot, so the Hudi-converted schema is supplied as the read schema instead. A writer-stored Avro schema in the
   * footer keeps precedence, as in {@link AvroReadSupport}.
   */
  private static Map<String, String> withDecimalReadSchema(MessageType requestedSchema, Map<String, String> metadata,
                                                           Map<String, String> keyValueMetaData, Configuration configuration) {
    if (metadata.get(AVRO_READ_SCHEMA_KEY) != null
        || keyValueMetaData.get(AVRO_SCHEMA_METADATA_KEY) != null
        || keyValueMetaData.get(OLD_AVRO_SCHEMA_METADATA_KEY) != null
        || !hasIntegerBackedDecimal(requestedSchema)) {
      return metadata;
    }
    Map<String, String> adjusted = new LinkedHashMap<>(metadata);
    adjusted.put(AVRO_READ_SCHEMA_KEY,
        HoodieAvroParquetSchemaConverter.getAvroSchemaConverter(configuration).convert(requestedSchema).toAvroSchema().toString());
    return adjusted;
  }

  /**
   * The compat materializer parquet-avro picks by default builds a converter this class cannot wrap, so a projection
   * with an INT32/INT64 decimal column is read through the regular record converter instead.
   */
  private static ReadContext withoutCompatibility(ReadContext readContext) {
    Map<String, String> metadata = readContext.getReadSupportMetadata();
    if (!hasIntegerBackedDecimal(readContext.getRequestedSchema()) || !Boolean.parseBoolean(metadata.get(AVRO_COMPATIBILITY))) {
      return readContext;
    }
    Map<String, String> adjusted = new LinkedHashMap<>(metadata);
    adjusted.put(AVRO_COMPATIBILITY, "false");
    return new ReadContext(readContext.getRequestedSchema(), adjusted);
  }

  private RecordMaterializer<T> readIntegerDecimals(RecordMaterializer<T> materializer, Configuration configuration,
                                                    Map<String, String> keyValueMetaData, ReadContext readContext) {
    MessageType requestedSchema = readContext.getRequestedSchema();
    if (materializer instanceof IntegerDecimalRecordConverter.Materializer || !hasIntegerBackedDecimal(requestedSchema)) {
      return materializer;
    }
    Schema avroSchema = resolveAvroReadSchema(configuration, keyValueMetaData, readContext.getReadSupportMetadata(), requestedSchema);
    GenericData dataModel = model != null ? model : GenericData.get();
    return new IntegerDecimalRecordConverter.Materializer<>(materializer, requestedSchema, avroSchema, dataModel);
  }

  /**
   * The Avro schema the parent materializer reads with, resolved in the same order as {@link AvroReadSupport}: the
   * requested read schema, the Avro schema the writer stored in the footer, else a conversion of the projection. The
   * conversion goes through the Hudi converter so that integer-backed decimal columns surface as decimals.
   */
  private static Schema resolveAvroReadSchema(Configuration configuration, Map<String, String> keyValueMetaData,
                                              Map<String, String> metadata, MessageType requestedSchema) {
    if (metadata.get(AVRO_READ_SCHEMA_KEY) != null) {
      return new Schema.Parser().parse(metadata.get(AVRO_READ_SCHEMA_KEY));
    }
    if (keyValueMetaData.get(AVRO_SCHEMA_METADATA_KEY) != null) {
      return new Schema.Parser().parse(keyValueMetaData.get(AVRO_SCHEMA_METADATA_KEY));
    }
    if (keyValueMetaData.get(OLD_AVRO_SCHEMA_METADATA_KEY) != null) {
      return new Schema.Parser().parse(keyValueMetaData.get(OLD_AVRO_SCHEMA_METADATA_KEY));
    }
    return HoodieAvroParquetSchemaConverter.getAvroSchemaConverter(configuration).convert(requestedSchema).toAvroSchema();
  }

  /**
   * Here we want set config with which file has been written.
   * Even though user may have overwritten {@link AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE},
   * it's only applicable to how to produce new files(here is a read path).
   * Later the config value {@link AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE} will still be used
   * to write new file according to the user preferences.
   **/
  private void adjustConfToReadWithFileProduceMode(boolean isLegacyModeWrittenFile, Configuration configuration) {
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
  private static boolean checkLegacyMode(List<Type> parquetFields) {
    return parquetFields.stream().anyMatch(type -> {
      if (!type.isPrimitive()) {
        GroupType groupType = type.asGroupType();
        OriginalType originalType = groupType.getOriginalType();
        if (originalType == OriginalType.MAP
            && !groupType.getFields().get(0).getName().equals("key_value")) {
          return true;
        }
        if (originalType == OriginalType.LIST
            && !groupType.getType(0).getName().equals("list")) {
          return true;
        }
        return checkLegacyMode(groupType.getFields());
      }
      return false;
    });
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
}
